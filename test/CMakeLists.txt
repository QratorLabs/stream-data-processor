find_package(Catch2 REQUIRED)
include(ParseAndAddCatchTests)

configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(
  COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/googletest-download"
  )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/googletest-download"
  )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
  ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
  EXCLUDE_FROM_ALL)

add_exec_target(test_main.cpp)
target_sources(test_main PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/gmock_catch_interceptor.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/aggregate_functions_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/kapacitor_udf_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/parsers_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/record_batch_handlers_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/uvarint_test.cpp"
  )

target_link_libraries(test_main Catch2::Catch2 gmock gmock_main)
ParseAndAddCatchTests(test_main)
