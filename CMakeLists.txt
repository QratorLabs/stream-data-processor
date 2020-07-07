cmake_minimum_required(VERSION 3.16)
project(tcp_echo_server)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)

set(TCP_ECHO_SERVER_SOURCES nodes/pass_node.cpp)
set(TCP_ECHO_SERVER_DIRS "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/nodes")

add_executable(tcp_echo_server main.cpp "${TCP_ECHO_SERVER_SOURCES}")

target_include_directories(tcp_echo_server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src")
target_include_directories(tcp_echo_server PRIVATE "${TCP_ECHO_SERVER_DIRS}")

target_link_libraries(tcp_echo_server uvw::uvw-static)
target_link_libraries(tcp_echo_server arrow_shared)