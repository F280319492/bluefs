cmake_minimum_required(VERSION 3.0)
project(bluefs)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

find_package(Threads REQUIRED)
find_package(aio REQUIRED)
set(HAVE_LIBAIO ${AIO_FOUND})
if(HAVE_LIBAIO)
    ADD_DEFINITIONS(-DHAVE_LIBAIO)
endif(HAVE_LIBAIO)

set(libbluefs_srcs
    common/aio.cc
	common/bufferlist.cc
	common/debug.cc
    dev/KernelDevice.cc
    dev/BlockDevice.cc
	Allocator/Allocator.cc
	Allocator/StableAllocator.cc
	bluefs_type.cc
    BlueFS.cc)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(bluefs STATIC ${libbluefs_srcs})