cmake_minimum_required(VERSION 3.13)
project(stream-data-processor)

set(CMAKE_CXX_STANDARD 17)

option(ENABLE_TESTS "enables tests building" OFF)
option(BUILD_SHARED_LIBS "enables building shared libraries" OFF)
option(CLANG_TIDY_LINT "enables clang-tidy linter" OFF)

find_package(PkgConfig)
pkg_check_modules(PC_ZeroMQ QUIET zmq)
find_path(ZeroMQ_INCLUDE_DIR
  NAMES zmq.hpp
  PATHS ${PC_ZeroMQ_INCLUDE_DIRS}
  )
find_library(ZeroMQ_LIBRARY
  NAMES zmq
  PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
  )
add_library(dependency_zmq UNKNOWN IMPORTED GLOBAL)
set_property(TARGET dependency_zmq PROPERTY IMPORTED_LOCATION "${ZeroMQ_LIBRARY}")
#add_library(dependencies::zmq ALIAS dependency_zmq)      # Introduced in cmake 3.15
message(STATUS "ZeroMQ include dir: ${ZeroMQ_INCLUDE_DIR}")
message(STATUS "ZeroMQ lib dir: ${ZeroMQ_LIBRARY}")

find_package(Arrow REQUIRED)
message(STATUS "Arrow lib dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${Arrow_DIR}")
find_package(Gandiva REQUIRED)
message(STATUS "Gandiva lib dir: ${Gandiva_DIR}")

find_package(spdlog REQUIRED)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

message(STATUS "Arrow libs: ${DEPENDENCY_ARROW_TARGET} ${DEPENDENCY_GANDIVA_TARGET}")

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party")

protobuf_generate_cpp(UDFProtoSources UDFProtoHeaders "${CMAKE_CURRENT_SOURCE_DIR}/src/kapacitor_udf/udf.proto")
add_library(protofiles_object_library OBJECT ${UDFProtoSources})
set_target_properties(protofiles_object_library PROPERTIES CXX_CLANG_TIDY "")

set(STREAM_DATA_PROCESSOR_LIBRARY_NAME "stream_data_processor_lib")
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PRIVATE_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/bprinter/include"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  arrow_shared
  gandiva_shared
  spdlog::spdlog
  bprinter
  dependency_zmq
  protobuf::libprotobuf
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  src/consumers/print_consumer.cpp
  src/consumers/publisher_consumer.cpp
  src/consumers/tcp_consumer.cpp
  src/nodes/node.cpp
  src/nodes/eval_node.cpp
  src/nodes/period_node.cpp
  src/data_handlers/data_parser.cpp
  src/record_batch_handlers/aggregate_handler.cpp
  src/data_handlers/serialized_record_batch_handler.cpp
  src/record_batch_handlers/default_handler.cpp
  src/record_batch_handlers/filter_handler.cpp
  src/record_batch_handlers/group_handler.cpp
  src/record_batch_handlers/map_handler.cpp
  src/record_batch_handlers/pipeline_handler.cpp
  src/record_batch_handlers/sort_handler.cpp
  src/record_batch_handlers/aggregate_functions/min_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/max_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/first_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/last_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/mean_aggregate_function.cpp
  src/record_batch_handlers/window_handler.cpp
  src/record_batch_handlers/join_handler.cpp
  src/server/unix_socket_server.cpp
  src/period_handlers/serialized_period_handler.cpp
  src/producers/tcp_producer.cpp
  src/producers/subscriber_producer.cpp
  src/utils/arrow_utils.cpp
  src/utils/parsers/csv_parser.cpp
  src/utils/parsers/graphite_parser.cpp
  src/utils/compute_utils.cpp
  src/utils/data_converter.cpp
  src/utils/transport_utils.cpp
  src/utils/serializer.cpp
  src/utils/string_utils.cpp
  src/utils/uvarint.cpp
  src/node_pipeline/node_pipeline.cpp
  )
list(APPEND LIBRARIES_NAMES "${STREAM_DATA_PROCESSOR_LIBRARY_NAME}")

set(KAPACITOR_UDF_LIBRARY_NAME "kapacitor_udf_lib")
set("${KAPACITOR_UDF_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  )
set("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  arrow_shared
  gandiva_shared
  spdlog::spdlog
  protobuf::libprotobuf
  )
set("${KAPACITOR_UDF_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  src/kapacitor_udf/udf_agent.cpp
  src/kapacitor_udf/request_handlers/aggregate_options_parser.cpp
  src/kapacitor_udf/request_handlers/group_tags_parser.cpp
  src/kapacitor_udf/request_handlers/batch_aggregate_request_handler.cpp
  src/kapacitor_udf/request_handlers/record_batch_request_handler.cpp
  src/kapacitor_udf/request_handlers/batch_to_stream_request_handler.cpp
  src/kapacitor_udf/request_handlers/stream_to_stream_request_handler.cpp
  src/record_batch_handlers/aggregate_handler.cpp
  src/record_batch_handlers/default_handler.cpp
  src/record_batch_handlers/filter_handler.cpp
  src/record_batch_handlers/group_handler.cpp
  src/record_batch_handlers/map_handler.cpp
  src/record_batch_handlers/pipeline_handler.cpp
  src/record_batch_handlers/sort_handler.cpp
  src/record_batch_handlers/aggregate_functions/min_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/max_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/first_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/last_aggregate_function.cpp
  src/record_batch_handlers/aggregate_functions/mean_aggregate_function.cpp
  src/record_batch_handlers/window_handler.cpp
  src/record_batch_handlers/join_handler.cpp
  src/server/unix_socket_server.cpp
  src/utils/arrow_utils.cpp
  src/utils/parsers/csv_parser.cpp
  src/utils/parsers/graphite_parser.cpp
  src/utils/compute_utils.cpp
  src/utils/data_converter.cpp
  src/utils/transport_utils.cpp
  src/utils/serializer.cpp
  src/utils/string_utils.cpp
  src/utils/uvarint.cpp
  )
list(APPEND LIBRARIES_NAMES "${KAPACITOR_UDF_LIBRARY_NAME}")

if (CLANG_TIDY_LINT)
  set(CMAKE_CXX_CLANG_TIDY "clang-tidy")
endif()

foreach(library_name ${LIBRARIES_NAMES})
  add_library("${library_name}" ${${library_name}_SOURCES})
  target_include_directories("${library_name}" PUBLIC ${${library_name}_PUBLIC_INCLUDE_DIRS})
  target_include_directories("${library_name}" PRIVATE ${${library_name}_PRIVATE_INCLUDE_DIRS})
  target_link_libraries("${library_name}" ${${library_name}_DEPENDENCIES})
endforeach()

function(add_exec_target file_path)
  message(STATUS "Adding target for source file: ${file_path}")
  get_filename_component(file_name "${file_path}" NAME_WE)
  add_executable("${file_name}" "${file_path}")
  foreach(library IN LISTS ARGN)
    target_link_libraries("${file_name}" "${library}")
  endforeach()
endfunction()

add_subdirectory(examples)

if(ENABLE_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()
