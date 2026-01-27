cmake_minimum_required(VERSION 3.18)
project(pika_port_kafka LANGUAGES CXX)

set(CXXFLAGS "-O2 -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
-Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
-D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++17 -gdwarf-2 -Wno-redundant-decls")

set(PIKIWIDB_ROOT "" CACHE PATH "Path to pikiwidb source root")
set(PIKIWIDB_BUILD "" CACHE PATH "Path to pikiwidb build dir")
set(PIKIWIDB_DEPS "" CACHE PATH "Path to pikiwidb deps dir (contains include/lib)")
set(UNWIND_LIBRARY "" CACHE FILEPATH "Path to libunwind")
set(UNWIND_X86_64_LIBRARY "" CACHE FILEPATH "Path to libunwind-x86_64")
set(PROTOBUF_LIBRARY "" CACHE FILEPATH "Path to libprotobuf")
set(PROTOBUF_PROTOC "" CACHE FILEPATH "Path to protoc")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

set(PROTO_DIR ${PROJECT_SOURCE_DIR}/../proto)
set(PROTO_FILES
    ${PROTO_DIR}/pika_inner_message.proto
    ${PROTO_DIR}/rsync_service.proto)

include(${PROJECT_SOURCE_DIR}/../cmake/protogen.cmake)

if(NOT PROTOBUF_PROTOC)
  find_program(PROTOBUF_PROTOC protoc)
endif()
if(NOT PROTOBUF_LIBRARY)
  find_library(PROTOBUF_LIBRARY protobuf)
endif()
if(NOT PROTOBUF_LIBRARY)
  message(FATAL_ERROR "libprotobuf not found; set PROTOBUF_LIBRARY")
endif()
if(NOT PROTOBUF_PROTOC)
  message(FATAL_ERROR "protoc not found; set PROTOBUF_PROTOC")
endif()

custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_executable(pika_port ${BASE_OBJS})
target_sources(pika_port PRIVATE ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(pika_port PRIVATE ${PROJECT_SOURCE_DIR})
target_include_directories(pika_port PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
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

if(NOT UNWIND_LIBRARY)
  find_library(UNWIND_LIBRARY unwind)
endif()
if(NOT UNWIND_X86_64_LIBRARY)
  find_library(UNWIND_X86_64_LIBRARY unwind-x86_64)
endif()

set(EXTRA_UNWIND_LIBS "")
if(UNWIND_LIBRARY AND NOT UNWIND_LIBRARY MATCHES "NOTFOUND")
  list(APPEND EXTRA_UNWIND_LIBS ${UNWIND_LIBRARY})
endif()
if(UNWIND_X86_64_LIBRARY AND NOT UNWIND_X86_64_LIBRARY MATCHES "NOTFOUND")
  list(APPEND EXTRA_UNWIND_LIBS ${UNWIND_X86_64_LIBRARY})
endif()

target_link_libraries(pika_port
                                storage pstd net
                                ${ROCKSDB_LIBRARY} ${SNAPPY_LIBRARY} ${ZSTD_LIBRARY} ${LZ4_LIBRARY}
                                ${ZLIB_LIBRARY} ${BZ2_LIBRARY} ${JEMALLOC_LIBRARY}
                                ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${FMT_LIBRARY}
                                ${PROTOBUF_LIBRARY}
                                ${EXTRA_UNWIND_LIBS}
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
