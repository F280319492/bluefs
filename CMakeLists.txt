set(libbluefs_srcs
    common/aio.cc
	common/bufferlist.cc
	common/debug.cc
    dev/KernelDevice.cc
    dev/BlockDevice.cc
	Allocator/Allocator.cc
	Allocator/StableAllocator.cc)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(bluefs STATIC ${libbluefs_srcs})