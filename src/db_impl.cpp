#include "db_impl.h"
#include "db.h"
#include "filename.h"
#include "write_batch.h"
#include "write_batch_internal.h"
#include "glog/logging.h"
#include "coding.h"
#include "log_reader.h"

namespace cowbpt {
    Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
        *dbptr = nullptr;
        DBImpl* impl = new DBImpl(options, dbname);
        std::lock_guard<std::mutex> lck(impl->_mutex);
        Status s = impl->Recover();
        if (s.ok()) {
            impl->RemoveObsoleteFiles();
        }
        if (s.ok()) {
            *dbptr = impl;
        } else {
            delete impl;
        }
        return s;
    }

    Status DestroyDB(const std::string& dbname, const Options& options) {
        Env* env = options.env;
        std::vector<std::string> filenames;
        Status result = env->GetChildren(dbname, &filenames);
        if (!result.ok()) {
            // Ignore error in case directory does not exist
            return Status::OK();
        }

        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            Status del = env->RemoveFile(dbname + "/" + filenames[i]);
            if (result.ok() && !del.ok()) {
            result = del;
            }
        }
        }
        env->RemoveDir(dbname);  // Ignore error in case dir contains other files

        rocksdb::Options rocks_options;
        rocksdb::Status rocks_status = rocksdb::DestroyDB(InternalDBName(dbname), rocks_options);
        // TODO convert rocks status to cowbpt status
        return result;
    }

    void DBImpl::RemoveObsoleteFiles() {
        std::vector<std::string> filenames;
        _env->GetChildren(_dbname, &filenames);  // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;
        for (std::string& filename : filenames) {
            if (ParseFileName(filename, &number, &type)) {
                bool keep;
                switch (type) {
                    case kLogFile:
                        keep = (number >= _logfile_number);
                        break;
                    default:
                        assert(false);
                }

                if (!keep) {
                    files_to_delete.push_back(std::move(filename));
                }
            }
        }

        for (const std::string& filename : files_to_delete) {
            _env->RemoveFile(_dbname + "/" + filename);
        }
    }

    Status DBImpl::Recover() {
        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        _env->CreateDir(_dbname);

        Status s = recover_meta_from_internalDB();
        if (!s.ok()) {
            return s;
        }

        if (_bpt == nullptr) {
            _bpt = new Bpt(_DB_options.comparator);
        }

        s = recover_log_files();
        if (!s.ok()) {
            return s;
        }

        return Status::OK();
    }

    Status DBImpl::recover_meta_from_internalDB() {
        rocksdb::Status rocks_status = rocksdb::DB::Open(_internalDB_options, InternalDBName(_dbname), &_internalDB);
        if (!rocks_status.ok()) {
            LOG(ERROR) << "Fail to open internal rocksdb: " << rocks_status.ToString();
            return Status::Corruption(rocks_status.ToString());
        }
        LOG(INFO) << "Succeed to open internal rocksdb";

        std::string value;
        rocks_status = _internalDB->Get(rocksdb::ReadOptions(), LogFileNumberKey(), &value);
        if (rocks_status.ok()) {
            _logfile_number = DecodeFixed64(value.c_str());
        } else if (!rocks_status.IsNotFound()) {
            LOG(ERROR) << "Error when reading logfile_number from internal db: " << rocks_status.ToString();
            return Status::Corruption(rocks_status.ToString());
        }

        value.clear();
        rocks_status = _internalDB->Get(rocksdb::ReadOptions(), LastSeqInLastLogFileKey(), &value);
        if (rocks_status.ok()) {
            SetLastSequence(DecodeFixed64(value.c_str()));
        } else if (!rocks_status.IsNotFound()) {
            LOG(ERROR) << "Error when reading LastSeqInLastLogFile from internal db: " << rocks_status.ToString();
            return Status::Corruption(rocks_status.ToString());
        }

        // TODO: recover others

        LOG(INFO) << "Succeed to recover meta from internal db";
        return Status::OK();
    }

    Status DBImpl::recover_log_files() {
        std::vector<std::string> filenames;
        Status s = _env->GetChildren(_dbname, &filenames);
        if (!s.ok()) {
            LOG(ERROR) << "Fail to list dir " << _dbname << " " << s.string();
            return s;
        }
        uint64_t number;
        FileType type;
        std::vector<uint64_t> logs;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                assert(type == kLogFile);
                if (number >= _logfile_number)
                    logs.push_back(number);
            }
        }
        // Recover in the order in which the logs were generated
        std::sort(logs.begin(), logs.end());
        for(size_t i = 0; i < logs.size(); i++) {
            s = recover_log(logs[i]);
            if (!s.ok()) {
                return s;
            }
        }
        return Status::OK();
    }

    Status DBImpl::recover_log(uint64_t log_number) {
        struct LogReporter : public log::Reader::Reporter {
            Env* env;
            const std::string* fname;
            Status* status;  // null if options_.paranoid_checks==false
            void Corruption(size_t bytes, const Status& s) override {
            LOG(INFO) << *fname << " log file dropped " << bytes << " when recovering: " << s.string();
            if (this->status != nullptr && this->status->ok()) *this->status = s;
            }
        };
        // Open the log file
        std::string fname = LogFileName(_dbname, log_number);
        SequentialFilePtr file;
        Status s = _env->NewSequentialFile(fname, file);
        if (!s.ok()) {
            LOG(ERROR) << "Fail to open log file " << fname << " when recovering: " << s.string();
            return s;
        }

        // Create the log reader.
        LogReporter reporter;
        reporter.env = _env;
        reporter.fname = &fname;
        reporter.status = nullptr;
        log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
        LOG(INFO) << "Recovering log file " << log_number;

        // Read all the records and add to a memtable
        std::string scratch;
        Slice record;
        WriteBatch batch;
        while (reader.ReadRecord(&record, &scratch) && s.ok()) {
            if (record.size() < 12) {
                reporter.Corruption(record.size(),
                                    Status::Corruption("log record too small"));
                continue;
            }
            WriteBatchInternal::SetContents(&batch, record);

            s = WriteBatchInternal::InsertInto(&batch, _bpt);
            if (!s.ok()) {
                LOG(ERROR) << "Fail to insert when recovering log file " << fname << " : " << s.string();
                return s;
            }
            const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                            WriteBatchInternal::Count(&batch) - 1;
            if (last_seq > LastSequence()) {
                SetLastSequence(last_seq);
            }
        }

        LOG(INFO) << "Successed in recovering log file " << log_number;

        return Status::OK();
    }

    DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : _env(raw_options.env),
      _bpt(nullptr),
      _last_seq_id(0),
      _logfile_number(0),
      _logfile(nullptr),
      _log(nullptr),
      _internalDB(nullptr),
      _mutex(),
      _dbname(dbname),
      _DB_options(raw_options),
      _internalDB_options(),
      _tmp_batch(new WriteBatch) {
          _internalDB_options.create_if_missing = _DB_options.create_if_missing;
          _internalDB_options.error_if_exists = _DB_options.error_if_exists;
    }
      
    DBImpl::~DBImpl() {
        if (_internalDB) {
            delete _internalDB;
        }
        delete _tmp_batch;
    }

    Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
        WriteBatch batch;
        batch.Put(key, val);
        return Write(o, &batch);
    }

    Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(options, &batch);
    }

    // Information kept for every waiting writer
    struct DBImpl::Writer {
        explicit Writer()
            : batch(nullptr), sync(false), done(false) {}

        Status status;
        WriteBatch* batch;
        bool sync;
        bool done;
        std::condition_variable cv;
    };

    Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
        Writer w;
        w.batch = updates;
        w.sync = options.sync;
        w.done = false;

        std::unique_lock<std::mutex> lck(_mutex);
        _writers.push_back(&w);
        while (!w.done && &w != _writers.front()) {
            w.cv.wait(lck);
        }
        if (w.done) {
            return w.status;
        }

        uint64_t last_sequence = LastSequence();
        Writer* last_writer = &w;
        
        WriteBatch* write_batch = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
        last_sequence += WriteBatchInternal::Count(write_batch);

        // Add to log and apply to memtable.  We can release the lock
        // during this phase since &w is currently responsible for logging
        // and protects against concurrent loggers and concurrent writes
        // into mem.
        
        lck.unlock();
        Status status = _log->AddRecord(WriteBatchInternal::Contents(write_batch));
        bool sync_error = false;
        if (status.ok() && options.sync) {
            status = _logfile->Sync();
            if (!status.ok()) {
            sync_error = true;
            }
        }
        if (status.ok()) {
            status = WriteBatchInternal::InsertInto(write_batch, _bpt);
        }
        lck.lock();

        if (sync_error) {
            LOG(FATAL) << "Fail to sync log file :" << status.string();
        }
        
        if (write_batch == _tmp_batch) _tmp_batch->Clear();

        SetLastSequence(last_sequence);
        

        while (true) {
            Writer* ready = _writers.front();
            _writers.pop_front();
            if (ready != &w) {
                ready->status = status;
                ready->done = true;
                ready->cv.notify_one();
            }
            if (ready == last_writer) break;
        }

        // Notify new head of write queue
        if (!_writers.empty()) {
            _writers.front()->cv.notify_one();
        }

        return status;
    }

    WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
        assert(!_writers.empty());
        Writer* first = _writers.front();
        WriteBatch* result = first->batch;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(first->batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        size_t max_size = 1 << 20;
        if (size <= (128 << 10)) {
            max_size = size + (128 << 10);
        }

        *last_writer = first;
        std::deque<Writer*>::iterator iter = _writers.begin();
        ++iter;  // Advance past "first"
        for (; iter != _writers.end(); ++iter) {
            Writer* w = *iter;
            if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
            }

            if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = _tmp_batch;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
            }
            *last_writer = w;
        }
        return result;
    }
}