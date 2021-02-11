add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/tick_script_adapted.cpp" stream_data_processor_lib)

add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/aggregate_udf/aggregate_udf.cpp" kapacitor_udf_lib)
add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/dynamic_window_udf/dynamic_window_udf.cpp" kapacitor_udf_lib)
add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/threshold_udf/threshold_udf.cpp" kapacitor_udf_lib)
