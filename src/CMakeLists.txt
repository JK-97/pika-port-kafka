cmake_minimum_required(VERSION 3.18)
project(pika_port_kafka LANGUAGES CXX)

set(CXXFLAGS "-O2 -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
-Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
-D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++17 -gdwarf-2 -Wno-redundant-decls")

set(PIKIWIDB_ROOT "" CACHE PATH "Path to pikiwidb source root")
set(PIKIWIDB_BUILD "" CACHE PATH "Path to pikiwidb build dir")
set(PIKIWIDB_DEPS "" CACHE PATH "Path to pikiwidb deps dir (contains include/lib)")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(pika_port ${BASE_OBJS})

target_include_directories(pika_port PRIVATE ${PROJECT_SOURCE_DIR})
if(INSTALL_INCLUDEDIR)
  target_include_directories(pika_port PRIVATE ${INSTALL_INCLUDEDIR})
endif()
if(PIKIWIDB_DEPS)
  target_include_directories(pika_port PRIVATE ${PIKIWIDB_DEPS}/include)
endif()
if(PIKIWIDB_ROOT)
  target_include_directories(pika_port PRIVATE
    ${PIKIWIDB_ROOT}
    ${PIKIWIDB_ROOT}/src
    ${PIKIWIDB_ROOT}/src/storage
    ${PIKIWIDB_ROOT}/src/storage/include
    ${PIKIWIDB_ROOT}/src/storage/include/storage
    ${PIKIWIDB_ROOT}/src/cache/include
  )
endif()
if(PIKIWIDB_BUILD)
  target_link_directories(pika_port PRIVATE
    ${PIKIWIDB_BUILD}/src/net
    ${PIKIWIDB_BUILD}/src/pstd
    ${PIKIWIDB_BUILD}/src/storage
  )
endif()

target_link_libraries(pika_port
                                storage pstd net
                                ${ROCKSDB_LIBRARY} ${SNAPPY_LIBRARY} ${ZSTD_LIBRARY} ${LZ4_LIBRARY}
                                ${ZLIB_LIBRARY} ${BZ2_LIBRARY} ${JEMALLOC_LIBRARY}
                                ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${FMT_LIBRARY}
                                pthread rdkafka)
set_target_properties(pika_port PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})
foreach(dep rocksdb glog gflags snappy zlib bz2 lz4 zstd)
  if(TARGET ${dep})
    add_dependencies(pika_port ${dep})
  endif()
endforeach()
