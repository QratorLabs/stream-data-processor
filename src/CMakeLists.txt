macro(add_arrow_dependency dependencies_list_name)
  list(APPEND "${dependencies_list_name}" arrow_shared)
endmacro()

macro(add_gandiva_dependency dependencies_list_name)
  list(APPEND "${dependencies_list_name}" gandiva_shared)
endmacro()

set(STREAM_DATA_PROCESSOR_LIBRARY_NAME "stream_data_processor_lib")
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/third_party/uvw/src"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_PRIVATE_INCLUDE_DIRS"
  "${CMAKE_SOURCE_DIR}/third_party/bprinter/include"
  )
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  spdlog::spdlog
  bprinter
  dependency_zmq
  protobuf::libprotobuf
  )
add_arrow_dependency("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES")
add_gandiva_dependency("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_DEPENDENCIES")
set("${STREAM_DATA_PROCESSOR_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  consumers/print_consumer.cpp
  consumers/publisher_consumer.cpp
  consumers/tcp_consumer.cpp
  metadata/column_typing.cpp
  metadata/grouping.cpp
  nodes/node.cpp
  nodes/eval_node.cpp
  nodes/period_node.cpp
  nodes/data_handlers/data_parser.cpp
  nodes/data_handlers/serialized_record_batch_handler.cpp
  nodes/period_handlers/serialized_period_handler.cpp
  record_batch_handlers/aggregate_handler.cpp
  record_batch_handlers/default_handler.cpp
  record_batch_handlers/filter_handler.cpp
  record_batch_handlers/group_dispatcher.cpp
  record_batch_handlers/group_handler.cpp
  record_batch_handlers/map_handler.cpp
  record_batch_handlers/pipeline_handler.cpp
  record_batch_handlers/sort_handler.cpp
  record_batch_handlers/aggregate_functions/min_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/max_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/first_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/last_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/mean_aggregate_function.cpp
  record_batch_handlers/window_handler.cpp
  record_batch_handlers/join_handler.cpp
  server/unix_socket_server.cpp
  producers/tcp_producer.cpp
  producers/subscriber_producer.cpp
  utils/arrow_utils.cpp
  utils/parsers/csv_parser.cpp
  utils/parsers/graphite_parser.cpp
  utils/compute_utils.cpp
  utils/data_converter.cpp
  utils/transport_utils.cpp
  utils/serializer.cpp
  utils/string_utils.cpp
  utils/uvarint.cpp
  node_pipeline/node_pipeline.cpp
  )
list(APPEND LIBRARIES_NAMES "${STREAM_DATA_PROCESSOR_LIBRARY_NAME}")

set(KAPACITOR_UDF_LIBRARY_NAME "kapacitor_udf_lib")
set("${KAPACITOR_UDF_LIBRARY_NAME}_PUBLIC_INCLUDE_DIRS"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_SOURCE_DIR}/third_party/uvw/src"
  )
set("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES"
  uvw::uvw-static
  spdlog::spdlog
  protobuf::libprotobuf
  )
add_arrow_dependency("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES")
add_gandiva_dependency("${KAPACITOR_UDF_LIBRARY_NAME}_DEPENDENCIES")
set("${KAPACITOR_UDF_LIBRARY_NAME}_SOURCES"
  $<TARGET_OBJECTS:protofiles_object_library>
  kapacitor_udf/udf_agent.cpp
  kapacitor_udf/points_converter.cpp
  kapacitor_udf/request_handlers/aggregate_request_handlers/aggregate_options_parser.cpp
  kapacitor_udf/group_parser.cpp
  kapacitor_udf/request_handlers/aggregate_request_handlers/batch_aggregate_request_handler.cpp
  kapacitor_udf/request_handlers/record_batch_request_handler.cpp
  kapacitor_udf/request_handlers/batch_to_stream_request_handler.cpp
  kapacitor_udf/request_handlers/stream_to_stream_request_handler.cpp
  metadata/column_typing.cpp
  metadata/grouping.cpp
  record_batch_handlers/aggregate_handler.cpp
  record_batch_handlers/default_handler.cpp
  record_batch_handlers/filter_handler.cpp
  record_batch_handlers/group_dispatcher.cpp
  record_batch_handlers/group_handler.cpp
  record_batch_handlers/map_handler.cpp
  record_batch_handlers/pipeline_handler.cpp
  record_batch_handlers/sort_handler.cpp
  record_batch_handlers/aggregate_functions/min_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/max_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/first_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/last_aggregate_function.cpp
  record_batch_handlers/aggregate_functions/mean_aggregate_function.cpp
  record_batch_handlers/window_handler.cpp
  record_batch_handlers/join_handler.cpp
  server/unix_socket_server.cpp
  utils/arrow_utils.cpp
  utils/parsers/csv_parser.cpp
  utils/parsers/graphite_parser.cpp
  utils/compute_utils.cpp
  utils/data_converter.cpp
  utils/transport_utils.cpp
  utils/serializer.cpp
  utils/string_utils.cpp
  utils/uvarint.cpp
  )
list(APPEND LIBRARIES_NAMES "${KAPACITOR_UDF_LIBRARY_NAME}")

if (CLANG_TIDY_LINT)
  set(CMAKE_CXX_CLANG_TIDY "clang-tidy")
endif()

foreach(library_name ${LIBRARIES_NAMES})
  add_library("${library_name}" ${${library_name}_SOURCES})
  target_include_directories("${library_name}" PUBLIC ${${library_name}_PUBLIC_INCLUDE_DIRS})
  target_include_directories("${library_name}" PRIVATE ${${library_name}_PRIVATE_INCLUDE_DIRS})
  target_link_libraries("${library_name}" ${${library_name}_DEPENDENCIES})
endforeach()
