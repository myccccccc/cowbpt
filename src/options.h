
#ifndef OPTIONS_H
#define OPTIONS_H

#include <cstddef>

namespace cowbpt {


class Comparator;
class Env;
// class Snapshot;

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
  // Create an Options object with default values for all fields.
  Options();

  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  Comparator* comparator;

  // If true, the database will be created if it is missing.
  bool create_if_missing = false;

  // If true, an error is raised if the database already exists.
  bool error_if_exists = false;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;
};

// Options that control read operations
struct ReadOptions {
  ReadOptions() = default;

  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  bool verify_checksums = false;

  // // If "snapshot" is non-null, read as of the supplied snapshot
  // // (which must belong to the DB that is being read and which must
  // // not have been released).  If "snapshot" is null, use an implicit
  // // snapshot of the state at the beginning of this read operation.
  // const Snapshot* snapshot = nullptr;
};

// Options that control write operations
struct WriteOptions {
  WriteOptions() = default;

  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fsync()".
  bool sync = false;
};

}  

#endif
