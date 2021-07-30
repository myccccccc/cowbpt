cmake_minimum_required(VERSION 3.5)

project(cowbpt)
    
set(CMAKE_CXX_FLAGS   "-std=c++11 -g -Wall")             
set(CMAKE_CXX_FLAGS_DEBUG   "-O0" )             
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG " )   

option(COWBPT_BUILD_TESTS "Build cowbpt's unit tests" ON)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/" "${CMAKE_MODULE_PATH}")

file(GLOB COWBPT_SRC_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)
add_library(cowbpt_library STATIC 
        ${COWBPT_SRC_FILES}
)
target_include_directories(cowbpt_library
    PUBLIC 
        ${PROJECT_SOURCE_DIR}/src
)

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/gflags)
target_link_libraries(cowbpt_library
         gflags::gflags)

add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/glog)
target_link_libraries(cowbpt_library
         glog::glog)

set(CMAKE_INCLUDE_PATH "${PROJECT_SOURCE_DIR}/third_party/rocksdb/include" "${CMAKE_INCLUDE_PATH}")
set(CMAKE_LIBRARY_PATH "${PROJECT_SOURCE_DIR}/third_party/rocksdb" "${CMAKE_LIBRARY_PATH}")
find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
find_library(ROCKSDB_LIB NAMES rocksdb)
if (NOT ROCKSDB_LIB)
        message(FATAL_ERROR "Fail to find rocksdb lib, \"make static_lib\" to build rocksdb")
endif()
target_include_directories(cowbpt_library
     PRIVATE
        ${ROCKSDB_INCLUDE_PATH}
)
target_link_libraries(cowbpt_library
        ${ROCKSDB_LIB})

if(COWBPT_BUILD_TESTS)
        enable_testing()

        set(install_gtest OFF)
        set(install_gmock OFF)
        set(build_gmock ON)
        add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/googletest)

        include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
        file(GLOB TEST_SRC_FILES ${PROJECT_SOURCE_DIR}/test/*.cpp)
        add_executable(runUnitTests ${TEST_SRC_FILES})
        target_link_libraries(runUnitTests gtest gtest_main gmock)
        target_link_libraries(runUnitTests cowbpt_library)
        add_test(UnitTests runUnitTests)

endif(COWBPT_BUILD_TESTS)