cmake_minimum_required(VERSION 3.16)
project(tcp_echo_server)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)

set(TCP_ECHO_SERVER_SOURCES nodes/input_node.cpp nodes/finalize_node.cpp)
set(TCP_ECHO_SERVER_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/nodes")

add_executable(finalize main_finalize.cpp "${TCP_ECHO_SERVER_SOURCES}")

target_include_directories(finalize PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src")
target_include_directories(finalize PRIVATE "${TCP_ECHO_SERVER_DIRS}")

target_link_libraries(finalize uvw::uvw-static)
target_link_libraries(finalize arrow_shared)

add_executable(input main_input.cpp "${TCP_ECHO_SERVER_SOURCES}")

target_include_directories(input PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src")
target_include_directories(input PRIVATE "${TCP_ECHO_SERVER_DIRS}")

target_link_libraries(input uvw::uvw-static)
target_link_libraries(input arrow_shared)