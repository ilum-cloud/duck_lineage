cmake_minimum_required(VERSION 3.11)

# Set extension name here
set(TARGET_NAME openlineage)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Include directories
include_directories(src/include)

# Find dependencies
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(nlohmann_json REQUIRED)

# Add source files
add_subdirectory(src)

# Build the extension
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link dependencies to both the static and loadable extensions
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl nlohmann_json::nlohmann_json)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl nlohmann_json::nlohmann_json)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
