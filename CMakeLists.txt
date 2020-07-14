cmake_minimum_required(VERSION 3.16)
project(tcp_echo_server)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)
message(STATUS "Arrow lib dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${Arrow_DIR})
find_package(Gandiva REQUIRED)

set(TCP_ECHO_SERVER_SOURCES
  nodes/eval_node.cpp
  nodes/finalize_node.cpp
  data_handlers/csv_to_record_batches_converter.cpp)
set(TCP_ECHO_SERVER_DIRS
  ${CMAKE_CURRENT_SOURCE_DIR}
  "${CMAKE_CURRENT_SOURCE_DIR}/nodes"
  "${CMAKE_CURRENT_SOURCE_DIR}/data_handlers")

add_executable(eval_graph main.cpp ${TCP_ECHO_SERVER_SOURCES})

set(PROJECT_INCLUDE_DIRS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src"
  ${TCP_ECHO_SERVER_DIRS}
  ${ARROW_INCLUDE_DIR}
  ${GANDIVA_INCLUDE_DIR})
target_include_directories(eval_graph PRIVATE "${PROJECT_INCLUDE_DIRS}")

set(PROJECT_DEPENDENCIES
  uvw::uvw-static
  arrow_shared
  ${GANDIVA_SHARED_LIBRARY})
target_link_libraries(eval_graph "${PROJECT_DEPENDENCIES}")
