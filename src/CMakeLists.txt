include_directories(include)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_event_builder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lineage_utils.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logical_lineage_sentinel.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duck_lineage_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duck_lineage_optimizer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/physical_lineage_sentinel.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/column_lineage_extractor.cpp
    PARENT_SCOPE)
