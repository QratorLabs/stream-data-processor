add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/tick_script_adapted.cpp" stream_data_processor_lib)

if (BUILD_EXPERIMENTS)
  add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/experiments.cpp" stream_data_processor_lib kapacitor_udf_lib)
endif()
