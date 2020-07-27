cmake_minimum_required(VERSION 3.16)
project(stream-data-processor)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)
message(STATUS "Arrow lib dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${Arrow_DIR})
find_package(Gandiva REQUIRED)
message(STATUS "Gandiva lib dir: ${Gandiva_DIR}")

find_package(spdlog REQUIRED)

set(STREAM_DATA_PROCESSOR_SOURCES
  src/nodes/node_base.cpp
  src/nodes/pass_node_base.cpp
  src/nodes/eval_node.cpp
  src/nodes/finalize_node.cpp
  src/nodes/window_node.cpp
  src/data_handlers/aggregate_handler.cpp
  src/data_handlers/data_parser.cpp
  src/data_handlers/map_handler.cpp
  src/data_handlers/sort_handler.cpp
  src/data_handlers/aggregate_functions/min_aggregate_function.cpp
  src/data_handlers/aggregate_functions/max_aggregate_function.cpp
  src/data_handlers/aggregate_functions/first_aggregate_function.cpp
  src/data_handlers/aggregate_functions/last_aggregate_function.cpp
  src/utils/parsers/csv_parser.cpp
  src/utils/parsers/graphite_parser.cpp
  src/utils/compute_utils.cpp
  src/utils/data_converter.cpp
  src/utils/network_utils.cpp
  src/utils/serializer.cpp
  src/utils/string_utils.cpp)

set(STREAM_DATA_PROCESSOR_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/src")

set(PROJECT_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  ${STREAM_DATA_PROCESSOR_INCLUDE_DIRS})

set(PROJECT_DEPENDENCIES
  uvw::uvw-static
  arrow_shared
  gandiva_shared
  spdlog::spdlog)

set(LIBRARY_NAME "stream_data_processor")

add_library(${LIBRARY_NAME} STATIC "${STREAM_DATA_PROCESSOR_SOURCES}")
target_include_directories(${LIBRARY_NAME} PUBLIC "${PROJECT_INCLUDE_DIRS}")
target_link_libraries(${LIBRARY_NAME} "${PROJECT_DEPENDENCIES}")

function(add_exec_target file_path)
  get_filename_component(file_name "${file_path}" NAME_WE)
  add_executable("${file_name}" "${file_path}")
  target_link_libraries("${file_name}" ${LIBRARY_NAME})
endfunction()

file(GLOB exec_files "executables/*.cpp")
foreach(file ${exec_files})
  add_exec_target(${file})
endforeach()

add_subdirectory(test)