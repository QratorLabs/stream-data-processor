file(GLOB exec_files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
foreach(file ${exec_files})
  add_exec_target("${file}")
endforeach()

add_exec_target("${CMAKE_CURRENT_SOURCE_DIR}/kapacitor_udf/cpu_kapacitor_udf.cpp")
