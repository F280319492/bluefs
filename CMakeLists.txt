set(libbluefs_srcs
  aio.cc
  KernelDevice.cc)


if(HAVE_LIBAIO)
  target_link_libraries(os ${AIO_LIBRARIES})
endif(HAVE_LIBAIO)

add_library(bluefs STATIC ${libbluefs_srcs})