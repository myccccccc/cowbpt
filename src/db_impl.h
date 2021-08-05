#ifndef DB_IMPL_H
#define DB_IMPL_H

#include <cassert>
#include <thread>
#include <condition_variable>


#include "db.h"
#include "bpt.h"
#include "env.h"
#include "leveldb/db.h"
#include "leveldb/db.h"
#include "log_writer.h"

namespace cowbpt {
    
    class DBImpl : public DB {
    private:
        struct Writer;
    public:
        DBImpl(const Options& options, const std::string& dbname);

        DBImpl(const DBImpl&) = delete;
        DBImpl& operator=(const DBImpl&) = delete;

        ~DBImpl() override;

        // Implementations of the DB interface
        Status Put(const WriteOptions&, const Slice& key,
                    const Slice& value) override;
        Status Delete(const WriteOptions&, const Slice& key) override;
        Status Write(const WriteOptions& options, WriteBatch* updates) override;
        Status Get(const ReadOptions& options, const Slice& key,
                    std::string* value) override;
        Status ManualCheckPoint() override;
        void DeepTraverse(const Bpt::NodePtr& root);
        Iterator* NewIterator(const ReadOptions&) override;
        // const Snapshot* GetSnapshot() override;
        // void ReleaseSnapshot(const Snapshot* snapshot) override;
    
    private:

        // Return the last sequence number.
        uint64_t LastSequence() const { return _last_seq_id; }

        // Set the last sequence number to s.
        void SetLastSequence(uint64_t s) {
            assert(s >= _last_seq_id);
            _last_seq_id = s;
        }
        void run_period(){
            std::this_thread::sleep_for(std::chrono::seconds(600));
            ManualCheckPoint();
        }

        Status Recover();
        void start_checkpoint_thread();
        Status recover_meta_from_internalDB();
        Status recover_pages_from_internalDB();
        Status recover_log_files();
        Status recover_log(uint64_t log_number);

        WriteBatch* BuildBatchGroup(Writer** last_writer);
        void RemoveObsoleteFiles();
    
    private:
        typedef Bpt::NodePtr NodePtr;
        friend class DB;

        Env* _env;

        Bpt* _bpt;

        NodeManager* _nm;

        uint64_t _last_seq_id;
        uint64_t _logfile_number;
        uint64_t _last_obsolete_logfile_number;

        WritableFilePtr _logfile;
        log::Writer* _log; 

        leveldb::DB* _internalDB;

        std::mutex _mutex;

        const std::string _dbname;

        Options _DB_options;
        leveldb::Options _internalDB_options;

        std::deque<Writer*> _writers;

        WriteBatch* _tmp_batch;
        
        uint64_t _last_checkpoint_snapshot_seq;

        uint64_t _max_node_id_in_internalDB;
    };
    
    class IteratorImpl : public Iterator {
        private:
        typedef Bpt::NodePtr NodePtr;

        public:
        IteratorImpl(NodePtr root, NodeManager* nm);

        IteratorImpl(const IteratorImpl&) = delete;
        IteratorImpl& operator=(const IteratorImpl&) = delete;

        virtual ~IteratorImpl() = default;

        // An iterator is either positioned at a key/value pair, or
        // not valid.  This method returns true iff the iterator is valid.
        virtual bool Valid() const override;

        // Position at the first key in the source.  The iterator is Valid()
        // after this call iff the source is not empty.
        virtual void SeekToFirst() override;

        // Position at the last key in the source.  The iterator is
        // Valid() after this call iff the source is not empty.
        virtual void SeekToLast() override;

        // Position at the first key in the source that is at or past target.
        // The iterator is Valid() after this call iff the source contains
        // an entry that comes at or past target.
        virtual void Seek(const Slice& target) override;

        // Moves to the next entry in the source.  After this call, Valid() is
        // true iff the iterator was not positioned at the last entry in the source.
        // REQUIRES: Valid()
        virtual void Next() override;

        // Moves to the previous entry in the source.  After this call, Valid() is
        // true iff the iterator was not positioned at the first entry in source.
        // REQUIRES: Valid()
        virtual void Prev() override;

        // Return the key for the current entry.  The underlying storage for
        // the returned slice is valid only until the next modification of
        // the iterator.
        // REQUIRES: Valid()
        virtual Slice key() const override;

        // Return the value for the current entry.  The underlying storage for
        // the returned slice is valid only until the next modification of
        // the iterator.
        // REQUIRES: Valid()
        virtual Slice value() const override;

        // If an error has occurred, return it.  Else return an ok status.
        virtual Status status() const override;

        private:

        NodePtr _root;
        NodeManager* _nm;
        Status _s;
        std::vector<NodePtr> _parents;
        std::vector<size_t> _positions;
        Slice _cur_key;
        Slice _cur_value;
        bool _valid;

    };
}
#endif