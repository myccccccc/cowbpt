cmake_minimum_required(VERSION 3.5)

project(cowbpt)
    
set(CMAKE_CXX_FLAGS   "-std=c++11 -g -Wall")             
set(CMAKE_CXX_FLAGS_DEBUG   "-O0" )             
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG " ) 

option(COWBPT_BUILD_TESTS "Build cowbpt's unit tests" ON)

## set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/" "${CMAKE_MODULE_PATH}")

file(GLOB COWBPT_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)
add_library(cowbpt_library STATIC 
        ${COWBPT_SRC_FILES}
)
target_include_directories(cowbpt_library
    PUBLIC 
        ${PROJECT_SOURCE_DIR}/src
)

find_path(GFLAGS_INCLUDE_PATH NAMES gflags/gflags.h)
find_library(GFLAGS_LIB NAMES gflags)
if (NOT GFLAGS_LIB OR NOT GFLAGS_INCLUDE_PATH)
        message(FATAL_ERROR "Fail to find gflags ")
endif()
target_include_directories(cowbpt_library
     PRIVATE
        ${GFLAGS_INCLUDE_PATH}
)
target_link_libraries(cowbpt_library
        ${GFLAGS_LIB})

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/glog)
target_link_libraries(cowbpt_library
        glog::glog)

SET(LEVELDB_BUILD_TESTS OFF)
SET(LEVELDB_BUILD_BENCHMARKS OFF)
SET(LEVELDB_INSTALL OFF)
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/leveldb)
target_link_libraries(cowbpt_library
        leveldb)

find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
find_library(ROCKSDB_LIB NAMES rocksdb)
if (NOT ROCKSDB_LIB OR NOT ROCKSDB_INCLUDE_PATH)
        message(FATAL_ERROR "Fail to find rocksdb ")
endif()
target_include_directories(cowbpt_library
     PRIVATE
        ${ROCKSDB_INCLUDE_PATH}
)
target_link_libraries(cowbpt_library
        ${ROCKSDB_LIB})

if(COWBPT_BUILD_TESTS)
        enable_testing()

        find_path(gtest_SOURCE_DIR NAMES gtest/gtest.h REQUIRED)
        find_path(gmock_SOURCE_DIR NAMES gmock/gmock.h REQUIRED)
        find_library(GTEST_LIB NAMES gtest REQUIRED)
        find_library(GTEST_MAIN_LIB names gtest_main REQUIRED)
        find_library(GMOCK_LIB names gmock REQUIRED)

        include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
        include_directories(${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})
        message(STATUS ${gtest_SOURCE_DIR})
        file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/test/*.cpp)
        add_executable(runUnitTests ${TEST_SRC_FILES})
        target_link_libraries(runUnitTests ${GTEST_LIB} ${GTEST_MAIN_LIB} ${GMOCK_LIB})
        target_link_libraries(runUnitTests cowbpt_library)
        add_test(UnitTests runUnitTests)

endif(COWBPT_BUILD_TESTS)