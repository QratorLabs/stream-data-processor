cmake_minimum_required(VERSION 3.16)
project(tcp_echo_server)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)
message(STATUS "Arrow lib dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${Arrow_DIR})
find_package(Gandiva REQUIRED)
message(STATUS "Gandiva lib dir: ${Gandiva_DIR}")

find_package(spdlog REQUIRED)

set(TCP_ECHO_SERVER_SOURCES
  nodes/node_base.cpp
  nodes/pass_node_base.cpp
  nodes/eval_node.cpp
  nodes/finalize_node.cpp
  nodes/window_node.cpp
  data_handlers/aggregate_handler.cpp
  data_handlers/data_parser.cpp
  data_handlers/map_handler.cpp
  data_handlers/sort_handler.cpp
  data_handlers/aggregate_functions/min_aggregate_function.cpp
  data_handlers/aggregate_functions/max_aggregate_function.cpp
  data_handlers/aggregate_functions/first_aggregate_function.cpp
  data_handlers/aggregate_functions/last_aggregate_function.cpp
  utils/parsers/csv_parser.cpp
  utils/compute_utils.cpp
  utils/data_converter.cpp
  utils/network_utils.cpp
  utils/serializer.cpp)

set(TCP_ECHO_SERVER_INCLUDE_DIRS
  ${CMAKE_CURRENT_SOURCE_DIR})

set(PROJECT_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  ${TCP_ECHO_SERVER_INCLUDE_DIRS})

set(PROJECT_DEPENDENCIES
  uvw::uvw-static
  arrow_shared
  gandiva_shared
  spdlog::spdlog)

file(GLOB files "executables/*.cpp")
foreach(file ${files})
  get_filename_component(file_name "${file}" NAME_WE)
  add_executable("${file_name}" "${file}" ${TCP_ECHO_SERVER_SOURCES})
  target_include_directories("${file_name}" PRIVATE "${PROJECT_INCLUDE_DIRS}")
  target_link_libraries("${file_name}" "${PROJECT_DEPENDENCIES}")
endforeach()
