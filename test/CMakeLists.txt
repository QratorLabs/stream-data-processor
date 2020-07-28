enable_testing()
find_package(GTest REQUIRED)

function(add_test_target file_path)
  add_exec_target(${file_path})
  get_filename_component(file_name "${file_path}" NAME_WE)
  message(STATUS "Adding test: ${file_name}")
  target_link_libraries(${file_name} GTest::GTest GTest::Main)
  add_test(
    NAME ${file_name}
    COMMAND ${file_name}
  )
endfunction()

file(GLOB test_files "./*.cpp")
foreach(file ${test_files})
  add_test_target(${file})
endforeach()
