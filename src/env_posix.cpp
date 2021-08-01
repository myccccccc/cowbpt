#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <atomic>

#include "env.h"

namespace cowbpt {

    Status PosixError(const std::string& context, int error_number) {
        if (error_number == ENOENT) {
            return Status::NotFound(context+" "+std::strerror(error_number));
        } else {
            return Status::IOError(context+" "+std::strerror(error_number));
        }
    }

    constexpr const size_t kWritableFileBufferSize = 65536;

    constexpr const int kOpenBaseFlags = O_CLOEXEC;

    class PosixWritableFile final : public WritableFile {
    public:
    PosixWritableFile(std::string filename, int fd)
        : pos_(0),
            fd_(fd),
            filename_(std::move(filename)),
            dirname_(Dirname(filename_)) {}

    ~PosixWritableFile() override {
        if (fd_ >= 0) {
        // Ignoring any potential errors
        Close();
        }
    }

    Status Append(const Slice& data) override {
        size_t write_size = data.size();
        const char* write_data = data.c_string();

        // Fit as much as possible into buffer.
        size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
        std::memcpy(buf_ + pos_, write_data, copy_size);
        write_data += copy_size;
        write_size -= copy_size;
        pos_ += copy_size;
        if (write_size == 0) {
        return Status::OK();
        }

        // Can't fit in buffer, so need to do at least one write.
        Status status = FlushBuffer();
        if (!status.ok()) {
        return status;
        }

        // Small writes go to buffer, large writes are written directly.
        if (write_size < kWritableFileBufferSize) {
        std::memcpy(buf_, write_data, write_size);
        pos_ = write_size;
        return Status::OK();
        }
        return WriteUnbuffered(write_data, write_size);
    }

    Status Close() override {
        Status status = FlushBuffer();
        const int close_result = ::close(fd_);
        if (close_result < 0 && status.ok()) {
        status = PosixError(filename_, errno);
        }
        fd_ = -1;
        return status;
    }

    Status Flush() override { return FlushBuffer(); }

    Status Sync() override {
        Status status = FlushBuffer();
        if (!status.ok()) {
        return status;
        }

        return SyncFd(fd_, filename_);
    }

    private:
    Status FlushBuffer() {
        Status status = WriteUnbuffered(buf_, pos_);
        pos_ = 0;
        return status;
    }

    Status WriteUnbuffered(const char* data, size_t size) {
        while (size > 0) {
        ssize_t write_result = ::write(fd_, data, size);
        if (write_result < 0) {
            if (errno == EINTR) {
            continue;  // Retry
            }
            return PosixError(filename_, errno);
        }
        data += write_result;
        size -= write_result;
        }
        return Status::OK();
    }

    // Ensures that all the caches associated with the given file descriptor's
    // data are flushed all the way to durable media, and can withstand power
    // failures.
    //
    // The path argument is only used to populate the description string in the
    // returned Status if an error occurs.
    static Status SyncFd(int fd, const std::string& fd_path) {
        bool sync_success = ::fsync(fd) == 0;

        if (sync_success) {
        return Status::OK();
        }
        return PosixError(fd_path, errno);
    }

    // Returns the directory name in a path pointing to a file.
    //
    // Returns "." if the path does not contain any directory separator.
    static std::string Dirname(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
        return std::string(".");
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) == std::string::npos);

        return filename.substr(0, separator_pos);
    }

    // Extracts the file name from a path pointing to a file.
    static Slice Basename(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
        return Slice(filename);
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) == std::string::npos);

        return Slice(filename.data() + separator_pos + 1);
    }

    // buf_[0, pos_ - 1] contains data to be written to fd_.
    char buf_[kWritableFileBufferSize];
    size_t pos_;
    int fd_;

    const std::string filename_;
    const std::string dirname_;  // The directory of filename_.
    };


    // Implements sequential read access in a file using read().
    //
    // Instances of this class are thread-friendly but not thread-safe, as required
    // by the SequentialFile API.
    class PosixSequentialFile final : public SequentialFile {
    public:
    PosixSequentialFile(std::string filename, int fd)
        : fd_(fd), filename_(filename) {}
    ~PosixSequentialFile() override { close(fd_); }

    Status Read(size_t n, Slice& result) override {
        Status status;

        // TODO: avoid new ervery time we call read
        char* scratch = new char[n + 1]();

        while (true) {
        ::ssize_t read_size = ::read(fd_, scratch, n);
        if (read_size < 0) {  // Read error.
            if (errno == EINTR) {
            continue;  // Retry
            }
            status = PosixError(filename_, errno);
            break;
        }
        result = Slice(scratch, read_size);
        break;
        }

        delete [] scratch;
        return status;
    }

    Status Skip(uint64_t n) override {
        if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
        return PosixError(filename_, errno);
        }
        return Status::OK();
    }

    private:
    const int fd_;
    const std::string filename_;
    };

    // Implements random read access in a file using pread().
    //
    // Instances of this class are thread-safe, as required by the RandomAccessFile
    // API. Instances are immutable and Read() only calls thread-safe library
    // functions.
    class PosixRandomAccessFile final : public RandomAccessFile {
    public:
        // The new instance takes ownership of |fd|. 
        PosixRandomAccessFile(std::string filename, int fd)
            : has_permanent_fd_(true),
                fd_(has_permanent_fd_ ? fd : -1),
                filename_(std::move(filename)) {
            if (!has_permanent_fd_) {
            assert(fd_ == -1);
            ::close(fd);  // The file will be opened on every read.
            }
        }

        ~PosixRandomAccessFile() override {
            if (has_permanent_fd_) {
            assert(fd_ != -1);
            ::close(fd_);
            }
        }

        Status Read(uint64_t offset, size_t n, Slice& result) const override {
            int fd = fd_;
            if (!has_permanent_fd_) {
            fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
            if (fd < 0) {
                return PosixError(filename_, errno);
            }
            }

            assert(fd != -1);

            // TODO: avoid new ervery time we call read
            char* scratch = new char[n + 1]();

            Status status;
            ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
            result = Slice(scratch, (read_size < 0) ? 0 : read_size);
            if (read_size < 0) {
            // An error: return a non-ok status.
            status = PosixError(filename_, errno);
            }
            if (!has_permanent_fd_) {
            // Close the temporary file descriptor opened earlier.
            assert(fd != fd_);
            ::close(fd);
            }

            delete [] scratch;

            return status;
        }

    private:
        const bool has_permanent_fd_;  // If false, the file is opened on every read.
        const int fd_;                 // -1 if has_permanent_fd_ is false.
        const std::string filename_;
    };


    class PosixEnv : public Env {
    public:
    PosixEnv() = default;
    ~PosixEnv() override {
        static const char msg[] =
            "PosixEnv singleton destroyed. Unsupported behavior!\n";
        std::fwrite(msg, 1, sizeof(msg), stderr);
        std::abort();
    }

    Status NewSequentialFile(const std::string& filename,
                            SequentialFilePtr& result) override {
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
        result = nullptr;
        return PosixError(filename, errno);
        }

        result.reset(new PosixSequentialFile(filename, fd));
        return Status::OK();
    }

    Status NewRandomAccessFile(const std::string& filename,
                                RandomAccessFilePtr result) override {
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
        return PosixError(filename, errno);
        }

        result.reset(new PosixRandomAccessFile(filename, fd));
        return Status::OK();
    }

    Status NewWritableFile(const std::string& filename,
                            WritableFilePtr& result) override {
        int fd = ::open(filename.c_str(),
                        O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
        result = nullptr;
        return PosixError(filename, errno);
        }

        result.reset(new PosixWritableFile(filename, fd));
        return Status::OK();
    }

    Status NewAppendableFile(const std::string& filename,
                            WritableFilePtr& result) override {
        int fd = ::open(filename.c_str(),
                        O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
        result = nullptr;
        return PosixError(filename, errno);
        }

        result.reset(new PosixWritableFile(filename, fd));
        return Status::OK();
    }

    bool FileExists(const std::string& filename) override {
        return ::access(filename.c_str(), F_OK) == 0;
    }

    Status GetChildren(const std::string& directory_path,
                        std::vector<std::string>* result) override {
        result->clear();
        ::DIR* dir = ::opendir(directory_path.c_str());
        if (dir == nullptr) {
        return PosixError(directory_path, errno);
        }
        struct ::dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
        result->emplace_back(entry->d_name);
        }
        ::closedir(dir);
        return Status::OK();
    }

    Status RemoveFile(const std::string& filename) override {
        if (::unlink(filename.c_str()) != 0) {
        return PosixError(filename, errno);
        }
        return Status::OK();
    }

    Status CreateDir(const std::string& dirname) override {
        if (::mkdir(dirname.c_str(), 0755) != 0) {
        return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    Status RemoveDir(const std::string& dirname) override {
        if (::rmdir(dirname.c_str()) != 0) {
        return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    Status GetFileSize(const std::string& filename, uint64_t* size) override {
        struct ::stat file_stat;
        if (::stat(filename.c_str(), &file_stat) != 0) {
        *size = 0;
        return PosixError(filename, errno);
        }
        *size = file_stat.st_size;
        return Status::OK();
    }

    Status RenameFile(const std::string& from, const std::string& to) override {
        if (std::rename(from.c_str(), to.c_str()) != 0) {
        return PosixError(from, errno);
        }
        return Status::OK();
    }

    // Status LockFile(const std::string& filename, FileLock** lock) override {
    //     *lock = nullptr;

    //     int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
    //     if (fd < 0) {
    //     return PosixError(filename, errno);
    //     }

    //     if (!locks_.Insert(filename)) {
    //     ::close(fd);
    //     return Status::IOError("lock " + filename, "already held by process");
    //     }

    //     if (LockOrUnlock(fd, true) == -1) {
    //     int lock_errno = errno;
    //     ::close(fd);
    //     locks_.Remove(filename);
    //     return PosixError("lock " + filename, lock_errno);
    //     }

    //     *lock = new PosixFileLock(fd, filename);
    //     return Status::OK();
    // }

    // Status UnlockFile(FileLock* lock) override {
    //     PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    //     if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
    //     return PosixError("unlock " + posix_file_lock->filename(), errno);
    //     }
    //     locks_.Remove(posix_file_lock->filename());
    //     ::close(posix_file_lock->fd());
    //     delete posix_file_lock;
    //     return Status::OK();
    // }

    // void Schedule(void (*background_work_function)(void* background_work_arg),
    //                 void* background_work_arg) override;

    // void StartThread(void (*thread_main)(void* thread_main_arg),
    //                 void* thread_main_arg) override {
    //     std::thread new_thread(thread_main, thread_main_arg);
    //     new_thread.detach();
    // }

    Status GetTestDirectory(std::string* result) override {
        const char* env = std::getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
        *result = env;
        } else {
        char buf[100];
        std::snprintf(buf, sizeof(buf), "/tmp/cowbpttest-%d",
                        static_cast<int>(::geteuid()));
        *result = buf;
        }

        // The CreateDir status is ignored because the directory may already exist.
        CreateDir(*result);

        return Status::OK();
    }

    // Status NewLogger(const std::string& filename, Logger** result) override {
    //     int fd = ::open(filename.c_str(),
    //                     O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    //     if (fd < 0) {
    //     *result = nullptr;
    //     return PosixError(filename, errno);
    //     }

    //     std::FILE* fp = ::fdopen(fd, "w");
    //     if (fp == nullptr) {
    //     ::close(fd);
    //     *result = nullptr;
    //     return PosixError(filename, errno);
    //     } else {
    //     *result = new PosixLogger(fp);
    //     return Status::OK();
    //     }
    // }

    // uint64_t NowMicros() override {
    //     static constexpr uint64_t kUsecondsPerSecond = 1000000;
    //     struct ::timeval tv;
    //     ::gettimeofday(&tv, nullptr);
    //     return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
    // }

    // void SleepForMicroseconds(int micros) override {
    //     std::this_thread::sleep_for(std::chrono::microseconds(micros));
    // }

    // private:
    // void BackgroundThreadMain();

    // static void BackgroundThreadEntryPoint(PosixEnv* env) {
    //     env->BackgroundThreadMain();
    // }

    // // Stores the work item data in a Schedule() call.
    // //
    // // Instances are constructed on the thread calling Schedule() and used on the
    // // background thread.
    // //
    // // This structure is thread-safe beacuse it is immutable.
    // struct BackgroundWorkItem {
    //     explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
    //         : function(function), arg(arg) {}

    //     void (*const function)(void*);
    //     void* const arg;
    // };

    // port::Mutex background_work_mutex_;
    // port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
    // bool started_background_thread_ GUARDED_BY(background_work_mutex_);

    // std::queue<BackgroundWorkItem> background_work_queue_
    //     GUARDED_BY(background_work_mutex_);

    // PosixLockTable locks_;  // Thread-safe.
    // Limiter mmap_limiter_;  // Thread-safe.
    // Limiter fd_limiter_;    // Thread-safe.
    };

    namespace {
        // Wraps an Env instance whose destructor is never created.
        //
        // Intended usage:
        //   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
        //   void ConfigurePosixEnv(int param) {
        //     PlatformSingletonEnv::AssertEnvNotInitialized();
        //     // set global configuration flags.
        //   }
        //   Env* Env::Default() {
        //     static PlatformSingletonEnv default_env;
        //     return default_env.env();
        //   }
        template <typename EnvType>
        class SingletonEnv {
        public:
        SingletonEnv() {
        #if !defined(NDEBUG)
            env_initialized_.store(true, std::memory_order::memory_order_relaxed);
        #endif  // !defined(NDEBUG)
            static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                        "env_storage_ will not fit the Env");
            static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                        "env_storage_ does not meet the Env's alignment needs");
            new (&env_storage_) EnvType();
        }
        ~SingletonEnv() = default;

        SingletonEnv(const SingletonEnv&) = delete;
        SingletonEnv& operator=(const SingletonEnv&) = delete;

        Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

        static void AssertEnvNotInitialized() {
        #if !defined(NDEBUG)
            assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
        #endif  // !defined(NDEBUG)
        }

        private:
        typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
            env_storage_;
        #if !defined(NDEBUG)
        static std::atomic<bool> env_initialized_;
        #endif  // !defined(NDEBUG)
        };

        #if !defined(NDEBUG)
        template <typename EnvType>
        std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
        #endif  // !defined(NDEBUG)

        using PosixDefaultEnv = SingletonEnv<PosixEnv>;

    }  // namespace

    Env* Env::Default() {
    static PosixDefaultEnv env_container;
    return env_container.env();
    }

    Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
        data->clear();
        SequentialFilePtr file;
        Status s = env->NewSequentialFile(fname, file);
        if (!s.ok()) {
            return s;
        }
        static const int kBufferSize = 8192;
        while (true) {
            Slice fragment;
            s = file->Read(kBufferSize, fragment);
            if (!s.ok()) {
            break;
            }
            data->append(fragment.c_string(), fragment.size());
            if (fragment.empty()) {
            break;
            }
        }
        return s;
    }
}