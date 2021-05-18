cmake_minimum_required(VERSION 3.13)
project(stream-data-processor)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

option(ENABLE_TESTS "enables tests building" OFF)
option(CLANG_TIDY_LINT "enables clang-tidy linter" OFF)

if(ENABLE_TESTS AND CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND NOT "$ENV{OSTYPE}" STREQUAL "linux-musl")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
  set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} -fsanitize=address -fno-omit-frame-pointer -g")
  message(STATUS "Enabled AddressSanitizer")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer -g")
  set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer -g")
  message(STATUS "Enabled UndefinedBehaviourSanitizer")

  if(NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak -fno-omit-frame-pointer -g")
    set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} -fsanitize=leak -fno-omit-frame-pointer -g")
    message(STATUS "Enabled LeakSanitizer")
  endif()
endif()

if(NOT BUILD_SHARED_LIBS)
  message(STATUS "Building static version")
endif ()

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

if(BUILD_SHARED_LIBS)
  find_library(ZeroMQ_LIBRARY
    NAMES "${CMAKE_SHARED_LIBRARY_PREFIX}zmq${CMAKE_SHARED_LIBRARY_SUFFIX}"
    PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
    )
else()
  find_library(ZeroMQ_LIBRARY
    NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}zmq${CMAKE_STATIC_LIBRARY_SUFFIX}"
    PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
    )
endif ()

add_library(dependency_zmq UNKNOWN IMPORTED GLOBAL)
set_property(TARGET dependency_zmq PROPERTY IMPORTED_LOCATION "${ZeroMQ_LIBRARY}")
#add_library(dependencies::zmq ALIAS dependency_zmq)      # Introduced in cmake 3.15
message(STATUS "ZeroMQ include dir: ${ZeroMQ_INCLUDE_DIR}")
message(STATUS "ZeroMQ lib: ${ZeroMQ_LIBRARY}")

find_package(Arrow REQUIRED)
message(STATUS "Arrow cmake dir: ${Arrow_DIR}")

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${Arrow_DIR}")
find_package(Gandiva REQUIRED)
message(STATUS "Gandiva cmake dir: ${Gandiva_DIR}")

find_package(spdlog REQUIRED)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/third_party")

protobuf_generate_cpp(ProtoSources ProtoHeaders
  "${CMAKE_CURRENT_SOURCE_DIR}/src/kapacitor_udf/udf.proto"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/metadata/metadata.proto"
  )
add_library(protofiles_object_library OBJECT ${ProtoSources})
set_target_properties(protofiles_object_library PROPERTIES CXX_CLANG_TIDY "")

function(add_exec_target file_path)
  message(STATUS "Adding executable target for the source file: ${file_path}")
  get_filename_component(file_name "${file_path}" NAME_WE)
  add_executable("${file_name}" "${file_path}")
  foreach(library IN LISTS ARGN)
    target_link_libraries("${file_name}" "${library}")
  endforeach()
endfunction()

function(add_library_target library_name)
  message(STATUS "Adding library target ${library_name}")
  add_library("${library_name}" ${${library_name}_SOURCES})
  target_include_directories("${library_name}" PUBLIC ${${library_name}_PUBLIC_INCLUDE_DIRS})
  target_include_directories("${library_name}" PRIVATE ${${library_name}_PRIVATE_INCLUDE_DIRS})
  target_link_libraries("${library_name}" ${${library_name}_DEPENDENCIES})
endfunction()

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/src")
add_subdirectory(examples)

if(ENABLE_TESTS)
  enable_testing()
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
