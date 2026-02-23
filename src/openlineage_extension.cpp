#define DUCKDB_EXTENSION_MAIN

#include "openlineage_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void OpenlineageScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Openlineage " + name.GetString() + " 🐥");
	});
}

inline void OpenlineageOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Openlineage " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto openlineage_scalar_function = ScalarFunction("openlineage", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OpenlineageScalarFun);
	loader.RegisterFunction(openlineage_scalar_function);

	// Register another scalar function
	auto openlineage_openssl_version_scalar_function = ScalarFunction("openlineage_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, OpenlineageOpenSSLVersionScalarFun);
	loader.RegisterFunction(openlineage_openssl_version_scalar_function);
}

void OpenlineageExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OpenlineageExtension::Name() {
	return "openlineage";
}

std::string OpenlineageExtension::Version() const {
#ifdef EXT_VERSION_OPENLINEAGE
	return EXT_VERSION_OPENLINEAGE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openlineage, loader) {
	duckdb::LoadInternal(loader);
}
}
