cmake_minimum_required(VERSION 3.16)
project(libuv_tcp_echo)

set(CMAKE_CXX_STANDARD 17)

set(BUILD_UVW_LIBS ON)
add_subdirectory(uvw)

add_executable(libuv_tcp_echo main.cpp server.cpp server.h)

target_include_directories(libuv_tcp_echo PRIVATE uvw/src)

target_link_libraries(libuv_tcp_echo uvw::uvw-static)