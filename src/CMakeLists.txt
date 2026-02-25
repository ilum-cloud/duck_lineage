include_directories(include)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_event_builder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logical_lineage_sentinel.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/openlineage_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/openlineage_optimizer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/physical_lineage_sentinel.cpp
    PARENT_SCOPE)
