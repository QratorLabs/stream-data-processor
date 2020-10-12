set(CMAKE_CXX_CLANG_TIDY "")

set(USE_BOOST_KARMA OFF CACHE BOOL "" FORCE)
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/bprinter")

set(BUILD_UVW_LIBS ON)
set(BUILD_TESTING OFF CACHE BOOL "disable uvw build gtest" FORCE)
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/uvw")

if(ENABLE_TESTS)
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/googletest" EXCLUDE_FROM_ALL)
endif()
