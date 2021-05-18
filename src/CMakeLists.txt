macro(add_arrow_dependency dependencies_list_name)
  if(BUILD_SHARED_LIBS)
    list(APPEND "${dependencies_list_name}" arrow_shared)
  else()
    set(THREADS_PREFER_PTHREAD_FLAG ON)

    if(NOT Threads_FOUND)
      find_package(Threads REQUIRED)
    endif()

    list(APPEND "${dependencies_list_name}" arrow_static Threads::Threads)
  endif()
endmacro()

macro(add_gandiva_dependency dependencies_list_name)
  if(BUILD_SHARED_LIBS OR APPLE)
    list(APPEND "${dependencies_list_name}" gandiva_shared)
  else()
    if(NOT LLVM_FOUND)
      find_package(LLVM REQUIRED CONFIG)

      message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
      message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
    endif()

    include_directories(${LLVM_INCLUDE_DIRS})
    add_definitions(${LLVM_DEFINITIONS})

    llvm_map_components_to_libnames(llvm_libs
      core
      mcjit
      native
      ipo
      bitreader
      target
      linker
      analysis
      debuginfodwarf)

    if(NOT TARGET dependency_re2)
      pkg_check_modules(RE2_PC re2)
      if(RE2_PC_FOUND)
        set(RE2_INCLUDE_DIR "${RE2_PC_INCLUDEDIR}")

        list(APPEND RE2_PC_LIBRARY_DIRS "${RE2_PC_LIBDIR}")
        find_library(RE2_LIB re2 PATHS ${RE2_PC_LIBRARY_DIRS})
      elseif(RE2_ROOT)
        find_library(
          RE2_LIB
          NAMES
          re2_static re2
          "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}"
          PATHS ${RE2_ROOT})
        find_path(RE2_INCLUDE_DIR
          NAMES re2/re2.h
          PATHS ${RE2_ROOT})
      else()
        find_library(
          RE2_LIB
          NAMES
          re2_static re2
          "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}")
        find_path(RE2_INCLUDE_DIR NAMES re2/re2.h)
      endif()

      if (NOT "${RE2_LIB}" STREQUAL "")
        if(NOT TARGET dependency_re2)
          add_library(dependency_re2 UNKNOWN IMPORTED GLOBAL)
          set_target_properties(dependency_re2
            PROPERTIES IMPORTED_LOCATION "${RE2_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIR}")
        endif()

        message(STATUS "re2 lib: ${RE2_LIB}")
        message(STATUS "re2 include dir: ${RE2_INCLUDE_DIR}")
      else()
        message(FATAL_ERROR "Couldn't find re2 library required for gandiva_static")
      endif()
    endif()

    list(APPEND "${dependencies_list_name}" gandiva_static ${llvm_libs} dependency_re2)
  endif()
endmacro()

set(STREAM_DATA_PROCESSOR_LIBRARY_NAME "stream_data_processor_lib")
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/third_party/uvw/src"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PRIVATE_INCLUDE_DIRS"
  "${CMAKE_SOURCE_DIR}/third_party/bprinter/include"
  "${CMAKE_SOURCE_DIR}/third_party/eigen"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  spdlog::spdlog
  bprinter
  dependency_zmq
  protobuf::libprotobuf
  )
add_gandiva_dependency("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES")
add_arrow_dependency("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES")
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  consumers/print_consumer.cpp
  consumers/publisher_consumer.cpp
  consumers/tcp_consumer.cpp
  metadata/column_typing.cpp
  metadata/time_metadata.cpp
  metadata/grouping.cpp
  metadata/help.cpp
  nodes/node.cpp
  nodes/eval_node.cpp
  nodes/data_handlers/data_handler.cpp
  nodes/data_handlers/data_parser.cpp
  nodes/data_handlers/serialized_record_batch_handler.cpp
  nodes/data_handlers/parsers/parser.cpp
  nodes/data_handlers/parsers/csv_parser.cpp
  nodes/data_handlers/parsers/graphite_parser.cpp
  record_batch_handlers/record_batch_handler.cpp
  record_batch_handlers/aggregate_functions/aggregate_function.cpp
  record_batch_handlers/aggregate_functions/aggregate_functions.cpp
  record_batch_handlers/stateful_handlers/handler_factory.cpp
  record_batch_handlers/stateful_handlers/threshold_state_machine.cpp
  record_batch_handlers/stateful_handlers/derivative_handler.cpp
  record_batch_handlers/aggregate_handler.cpp
  record_batch_handlers/default_handler.cpp
  record_batch_handlers/filter_handler.cpp
  record_batch_handlers/group_dispatcher.cpp
  record_batch_handlers/group_handler.cpp
  record_batch_handlers/log_handler.cpp
  record_batch_handlers/map_handler.cpp
  record_batch_handlers/pipeline_handler.cpp
  record_batch_handlers/sort_handler.cpp
  record_batch_handlers/stateful_handlers/window_handler.cpp
  record_batch_handlers/join_handler.cpp
  server/unix_socket_client.cpp
  server/unix_socket_server.cpp
  producers/tcp_producer.cpp
  producers/subscriber_producer.cpp
  utils/arrow_utils.cpp
  utils/compute_utils.cpp
  utils/convert_utils.cpp
  utils/time_utils.cpp
  utils/transport_utils.cpp
  utils/serialize_utils.cpp
  utils/string_utils.cpp
  utils/uvarint_utils.cpp
  node_pipeline/node_pipeline.cpp
  )
list(APPEND LIBRARIES_NAMES "${STREAM_DATA_PROCESSOR_LIBRARY_NAME}")

set(KAPACITOR_UDF_LIBRARY_NAME "kapacitor_udf_lib")
set("${KAPACITOR_UDF_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/third_party/uvw/src"
  "${CMAKE_SOURCE_DIR}/third_party/cxxopts/include"
  "${CMAKE_SOURCE_DIR}/third_party/eigen"
  )
set("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  spdlog::spdlog
  protobuf::libprotobuf
  )
add_gandiva_dependency("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES")
add_arrow_dependency("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES")
set("${KAPACITOR_UDF_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  kapacitor_udf/utils/grouping_utils.cpp
  kapacitor_udf/udf_agent.cpp
  kapacitor_udf/utils/rw_utils.cpp
  kapacitor_udf/utils/points_converter.cpp
  kapacitor_udf/utils/points_storage.cpp
  kapacitor_udf/request_handlers/aggregate_request_handlers/aggregate_options_parser.cpp
  kapacitor_udf/request_handlers/aggregate_request_handlers/batch_aggregate_request_handler.cpp
  kapacitor_udf/request_handlers/aggregate_request_handlers/stream_aggregate_request_handler.cpp
  kapacitor_udf/request_handlers/derivative_request_handler.cpp
  kapacitor_udf/request_handlers/dynamic_window_request_handler.cpp
  kapacitor_udf/request_handlers/logging_request_handler.cpp
  kapacitor_udf/request_handlers/record_batch_request_handler.cpp
  kapacitor_udf/request_handlers/request_handler.cpp
  kapacitor_udf/request_handlers/stateful_threshold_request_handler.cpp
  kapacitor_udf/request_handlers/batch_request_handler.cpp
  kapacitor_udf/request_handlers/stream_request_handler.cpp
  metadata/column_typing.cpp
  metadata/time_metadata.cpp
  metadata/grouping.cpp
  metadata/help.cpp
  record_batch_handlers/record_batch_handler.cpp
  record_batch_handlers/aggregate_functions/aggregate_function.cpp
  record_batch_handlers/aggregate_functions/aggregate_functions.cpp
  record_batch_handlers/stateful_handlers/handler_factory.cpp
  record_batch_handlers/stateful_handlers/threshold_state_machine.cpp
  record_batch_handlers/stateful_handlers/derivative_handler.cpp
  record_batch_handlers/aggregate_handler.cpp
  record_batch_handlers/default_handler.cpp
  record_batch_handlers/filter_handler.cpp
  record_batch_handlers/group_dispatcher.cpp
  record_batch_handlers/group_handler.cpp
  record_batch_handlers/log_handler.cpp
  record_batch_handlers/map_handler.cpp
  record_batch_handlers/pipeline_handler.cpp
  record_batch_handlers/sort_handler.cpp
  record_batch_handlers/stateful_handlers/window_handler.cpp
  record_batch_handlers/join_handler.cpp
  server/unix_socket_client.cpp
  server/unix_socket_server.cpp
  utils/arrow_utils.cpp
  utils/compute_utils.cpp
  utils/convert_utils.cpp
  utils/time_utils.cpp
  utils/transport_utils.cpp
  utils/serialize_utils.cpp
  utils/string_utils.cpp
  utils/uvarint_utils.cpp
  )
list(APPEND LIBRARIES_NAMES "${KAPACITOR_UDF_LIBRARY_NAME}")

set(SDP_UDF_EXECUTABLE_NAME "sdp-udf")
add_executable("${SDP_UDF_EXECUTABLE_NAME}" "${CMAKE_SOURCE_DIR}/src/kapacitor_udf/main.cpp")
target_link_libraries("${SDP_UDF_EXECUTABLE_NAME}" "${KAPACITOR_UDF_LIBRARY_NAME}")

if (CLANG_TIDY_LINT)
  set(CMAKE_CXX_CLANG_TIDY "clang-tidy")
endif()

foreach(library_name ${LIBRARIES_NAMES})
  add_library_target("${library_name}")
endforeach()
