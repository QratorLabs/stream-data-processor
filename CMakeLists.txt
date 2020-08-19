cmake_minimum_required(VERSION 3.13)
project(stream-data-processor)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

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
message(STATUS "ZeroMQ include dir: ${ZeroMQ_INCLUDE_DIR}")
message(STATUS "ZeroMQ lib dir: ${ZeroMQ_LIBRARY}")

find_package(Arrow REQUIRED)
message(STATUS "Arrow lib dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${Arrow_DIR})
find_package(Gandiva REQUIRED)
message(STATUS "Gandiva lib dir: ${Gandiva_DIR}")

find_package(spdlog REQUIRED)

set(USE_BOOST_KARMA OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/bprinter)

set(STREAM_DATA_PROCESSOR_SOURCES
  src/consumers/print_consumer.cpp
  src/consumers/publisher_consumer.cpp
  src/consumers/tcp_consumer.cpp
  src/nodes/node.cpp
  src/nodes/eval_node.cpp
  src/nodes/period_node.cpp
  src/data_handlers/aggregate_handler.cpp
  src/data_handlers/data_parser.cpp
  src/data_handlers/default_handler.cpp
  src/data_handlers/filter_handler.cpp
  src/data_handlers/group_handler.cpp
  src/data_handlers/map_handler.cpp
  src/data_handlers/sort_handler.cpp
  src/data_handlers/aggregate_functions/min_aggregate_function.cpp
  src/data_handlers/aggregate_functions/max_aggregate_function.cpp
  src/data_handlers/aggregate_functions/first_aggregate_function.cpp
  src/data_handlers/aggregate_functions/last_aggregate_function.cpp
  src/data_handlers/aggregate_functions/mean_aggregate_function.cpp
  src/period_handlers/window_handler.cpp
  src/period_handlers/join_handler.cpp
  src/producers/producer.cpp
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
  src/node_pipeline/node_pipeline.cpp)

set(STREAM_DATA_PROCESSOR_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/src")

set(PROJECT_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/bprinter/include"
  ${STREAM_DATA_PROCESSOR_INCLUDE_DIRS}
  ${ZeroMQ_INCLUDE_DIR})

set(PROJECT_DEPENDENCIES
  uvw::uvw-static
  arrow_shared
  gandiva_shared
  spdlog::spdlog
  bprinter
  ${ZeroMQ_LIBRARY})

set(LIBRARY_NAME "stream_data_processor")

add_library(${LIBRARY_NAME} STATIC "${STREAM_DATA_PROCESSOR_SOURCES}")
target_include_directories(${LIBRARY_NAME} PUBLIC "${PROJECT_INCLUDE_DIRS}")
target_link_libraries(${LIBRARY_NAME} "${PROJECT_DEPENDENCIES}")

function(add_exec_target file_path)
  message(STATUS "Adding target for source file: ${file_path}")
  get_filename_component(file_name "${file_path}" NAME_WE)
  add_executable("${file_name}" "${file_path}")
  target_link_libraries("${file_name}" ${LIBRARY_NAME})
endfunction()

file(GLOB exec_files "executables/*.cpp")
foreach(file ${exec_files})
  add_exec_target(${file})
endforeach()

enable_testing()
add_subdirectory(test)
