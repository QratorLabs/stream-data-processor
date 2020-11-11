set(CMAKE_CXX_CLANG_TIDY "")

find_package(Catch2 REQUIRED)
include(ParseAndAddCatchTests)

add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/test_main.cpp" stream_data_processor_lib kapacitor_udf_lib)
target_sources(test_main PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/gmock_catch_interceptor.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/aggregate_functions_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/kapacitor_udf_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/metadata_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/parsers_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/record_batch_builder.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/record_batch_handlers_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/utils_test.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/uvarint_test.cpp"
  )

target_link_libraries(test_main Catch2::Catch2 gmock gmock_main)
ParseAndAddCatchTests(test_main)
