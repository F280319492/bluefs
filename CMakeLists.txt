cmake_minimum_required(VERSION 3.0)
project(bluefs)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wno-unused-value -Wno-unused-but-set-variable -fPIC")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

find_package(Threads REQUIRED)
find_package(aio REQUIRED)
set(HAVE_LIBAIO ${AIO_FOUND})
if(HAVE_LIBAIO)
    ADD_DEFINITIONS(-DHAVE_LIBAIO)
endif(HAVE_LIBAIO)
find_package(Boost COMPONENTS program_options thread REQUIRED)

option(WITH_SPDK "Enable SPDK" ON)
if(WITH_SPDK)
    ADD_DEFINITIONS(-DHAVE_SPDK)
endif(WITH_SPDK)

set(libbluefs_srcs
        common/aio.cc
        common/bufferlist.cc
        common/debug.cc
        common/utime.cc
        common/queue_pair.cc
        dev/KernelDevice.cc
        dev/BlockDevice.cc
        dev/SPDKDevice.cc
        Allocator/Allocator.cc
        Allocator/StableAllocator.cc
        env/BlueRocksEnv.cc
        bluefs_type.cc
        BlueFS.cc)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(bluefs STATIC ${libbluefs_srcs})
target_link_libraries (bluefs aio ${Boost_LIBRARIES})

if(WITH_SPDK)
    target_link_libraries (bluefs isal dpdk spdk spdk_env_dpdk)
endif(WITH_SPDK)
