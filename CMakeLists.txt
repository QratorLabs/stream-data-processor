cmake_minimum_required(VERSION 3.16)
project(tcp_echo_server)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(third_party/uvw)

find_package(Arrow REQUIRED)

add_executable(tcp_echo_server main.cpp server.cpp server.h)

target_include_directories(tcp_echo_server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/uvw/src")

target_link_libraries(tcp_echo_server uvw::uvw-static)
target_link_libraries(tcp_echo_server arrow_shared)