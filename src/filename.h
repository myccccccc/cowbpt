
#ifndef FILENAME_H
#define FILENAME_H

#include <cstdint>
#include <string>

#include "slice.h"
#include "status.h"

namespace cowbpt {

class Env;

enum FileType {
  kLogFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string LogFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
std::string DescriptorFileName(const std::string& dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
std::string CurrentFileName(const std::string& dbname);

// Return the name of the internal level DB of the current db.  
// the internal level DB constains all the necessary info to recovery
std::string InternalDBName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
std::string TempFileName(const std::string& dbname, uint64_t number);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type);

// log_number key that stores in leveldb
std::string LogFileNumberKey();

// LastSeqInLastLogFile key that stores in leveldb
std::string LastSeqInLastLogFileKey();
}  

#endif
