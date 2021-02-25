set(BENCHMARKS_LIBRARY_NAME "benchmarks_lib")
set("${BENCHMARKS_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/third_party/cxxopts/include"
  )
set("${BENCHMARKS_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  spdlog::spdlog
  protobuf::libprotobuf
  kapacitor_udf_lib
  stream_data_processor_lib
  )
set("${BENCHMARKS_LIBRARY_NAME}_SOURCES"
  data_generators/data_generators.h
  data_generators/data_generators.cpp
  )
add_library_target("${BENCHMARKS_LIBRARY_NAME}")
