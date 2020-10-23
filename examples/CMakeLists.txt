file(GLOB exec_files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/experiments.cpp" stream_data_processor_lib kapacitor_udf_lib)
add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/tick_script_adapted.cpp" stream_data_processor_lib)
add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/window_example.cpp" stream_data_processor_lib)

add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/cpu_kapacitor_udf/cpu_kapacitor_udf.cpp" kapacitor_udf_lib)
