//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_client.hpp
// Description: HTTP client for sending OpenLineage events to a backend server.
//              This class manages a background worker thread that asynchronously
//              sends lineage events via HTTP POST requests.
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

namespace duckdb {

/// @class LineageClient
/// @brief Singleton HTTP client for sending OpenLineage events asynchronously.
///
/// The LineageClient manages a background worker thread that processes a queue of
/// OpenLineage events and sends them to a configured backend URL via HTTP POST.
/// It supports configuration of the target URL, API key authentication, namespace,
/// and debug logging.
///
/// Thread Safety: All public methods are thread-safe and can be called from multiple
/// threads simultaneously. The class uses mutexes to protect shared state.
///
/// Lifecycle: The singleton instance is created on first use and destroyed at program
/// exit. The background worker thread is automatically started and stopped.
class LineageClient {
public:
	/// @brief Get the singleton instance of the LineageClient.
	/// @return Reference to the singleton LineageClient instance.
	static LineageClient &Get();

	/// @brief Queue an OpenLineage event to be sent asynchronously.
	/// @param event_json JSON string containing the OpenLineage event payload.
	/// @note If debug mode is enabled, the event will be printed to stdout.
	/// @note The event is added to a queue and processed by the background worker thread.
	void SendEvent(std::string event_json);

	// ===== Configuration Methods =====

	/// @brief Set the URL of the OpenLineage backend endpoint.
	/// @param url The full URL (e.g., "http://localhost:5000/api/v1/lineage").
	/// @note Thread-safe. Can be called at any time to update the URL.
	void SetUrl(std::string url);

	/// @brief Set the API key for authentication with the OpenLineage backend.
	/// @param key The API key to use in the Authorization: Bearer header.
	/// @note Thread-safe. The key is optional and can be empty.
	void SetApiKey(std::string key);

	/// @brief Set the namespace for OpenLineage events.
	/// @param ns The namespace string (default: "duckdb").
	/// @note Thread-safe. This namespace is used in job definitions.
	void SetNamespace(std::string ns);

	/// @brief Enable or disable debug logging.
	/// @param debug If true, events and HTTP responses are logged to stdout/stderr.
	/// @note Thread-safe. Useful for troubleshooting event transmission.
	void SetDebug(bool debug);

	/// @brief Set the maximum number of retry attempts for failed requests.
	/// @param retries Maximum number of retries (default: 3).
	/// @note Thread-safe. Retries use exponential backoff.
	void SetMaxRetries(size_t retries);

	/// @brief Set the maximum queue size to prevent memory issues.
	/// @param size Maximum number of events to queue (default: 10000).
	/// @note Thread-safe. Events are dropped when queue is full.
	void SetMaxQueueSize(size_t size);

	/// @brief Set the HTTP request timeout in seconds.
	/// @param timeout Timeout in seconds (default: 10).
	/// @note Thread-safe.
	void SetTimeout(int64_t timeout);

	/// @brief Set comma-separated prefixes of dataset names to exclude from lineage events.
	/// @param prefixes_csv Comma-separated list of prefixes (e.g., "__ducklake_metadata_,staging_temp_").
	/// @note Thread-safe. Datasets whose fully qualified name starts with any prefix are skipped.
	void SetExcludeDatasetPrefixes(const std::string &prefixes_csv);

	/// @brief Set the path to a CA certificate bundle file used to verify the backend's TLS certificate.
	/// @param path Filesystem path to a PEM CA bundle (maps to CURLOPT_CAINFO). Empty clears it.
	/// @note Thread-safe. Takes precedence over the inherited DuckDB ca_cert_file and CA env vars.
	void SetCaCertFile(std::string path);

	/// @brief Set the directory of CA certificates used to verify the backend's TLS certificate.
	/// @param path Filesystem path to a CA directory (maps to CURLOPT_CAPATH). Empty clears it.
	/// @note Thread-safe.
	void SetCaCertDir(std::string path);

	/// @brief Set the CA certificate file inherited from DuckDB's global "ca_cert_file" setting.
	/// @param path Filesystem path to a PEM CA bundle, as configured for httpfs. Empty clears it.
	/// @note Thread-safe. Used as a fallback when SetCaCertFile was not called explicitly.
	void SetInheritedCaCertFile(std::string path);

	/// @brief Enable or disable TLS peer/host verification for backend requests.
	/// @param verify If false, disables CURLOPT_SSL_VERIFYPEER and CURLOPT_SSL_VERIFYHOST (insecure).
	/// @note Thread-safe. Disabling verification is intended for development/testing only.
	void SetSslVerify(bool verify);

	/// @brief Set an HTTP/HTTPS proxy to route OpenLineage requests through.
	/// @param proxy Proxy URL (e.g., "http://proxy.company.com:8080"; maps to CURLOPT_PROXY). Empty clears it.
	/// @note Thread-safe. When empty, libcurl still honors the standard proxy environment variables.
	void SetProxy(std::string proxy);

	// ===== Accessor Methods =====

	/// @brief Get the current OpenLineage backend URL.
	/// @return The configured URL.
	/// @note Thread-safe.
	std::string GetUrl() const;

	/// @brief Get the current API key.
	/// @return The configured API key (may be empty).
	/// @note Thread-safe.
	std::string GetApiKey() const;

	/// @brief Get the current namespace.
	/// @return The configured namespace.
	/// @note Thread-safe.
	std::string GetNamespace() const;

	/// @brief Check if debug mode is enabled.
	/// @return True if debug logging is enabled, false otherwise.
	/// @note Thread-safe.
	bool IsDebug() const;

	/// @brief Get the current maximum retry count.
	/// @return Maximum number of retry attempts.
	/// @note Thread-safe.
	size_t GetMaxRetries() const;

	/// @brief Get the current maximum queue size.
	/// @return Maximum queue size.
	/// @note Thread-safe.
	size_t GetMaxQueueSize() const;

	/// @brief Get the current HTTP timeout setting.
	/// @return Timeout in seconds.
	/// @note Thread-safe.
	int64_t GetTimeout() const;

	/// @brief Get the current exclude dataset prefixes.
	/// @return Vector of prefix strings.
	/// @note Thread-safe.
	std::vector<std::string> GetExcludeDatasetPrefixes() const;

	/// @brief Get the explicitly configured CA certificate bundle path.
	/// @return The configured CA bundle path (may be empty).
	/// @note Thread-safe.
	std::string GetCaCertFile() const;

	/// @brief Get the explicitly configured CA certificate directory.
	/// @return The configured CA directory (may be empty).
	/// @note Thread-safe.
	std::string GetCaCertDir() const;

	/// @brief Check whether TLS peer/host verification is enabled.
	/// @return True if verification is enabled (default), false otherwise.
	/// @note Thread-safe.
	bool GetSslVerify() const;

	/// @brief Get the configured proxy URL.
	/// @return The configured proxy URL (may be empty).
	/// @note Thread-safe.
	std::string GetProxy() const;

	/// @brief Get the number of events dropped due to queue overflow.
	/// @return Number of dropped events.
	/// @note Thread-safe.
	size_t GetDroppedEvents() const;

	/// @brief Initialize libcurl and OpenSSL. Idempotent and thread-safe.
	/// @note Must run before anything else in the extension touches OpenSSL, so it is called at extension load.
	///       Disables OpenSSL's atexit cleanup, which would otherwise run while the worker is still sending events.
	static void InitializeHttpLibraries();

	/// @brief Stop the background worker and wait for it to finish.
	/// @note The worker delivers the events that are still queued before stopping, but does not retry and gives
	///       up on the first failed delivery, so an unreachable backend cannot hold up process exit.
	/// @note Called automatically at process exit. Events sent afterwards are dropped.
	void Shutdown();

private:
	/// @brief Private constructor for singleton pattern.
	/// @note Initializes CURL, starts the background worker thread.
	LineageClient();

	/// @brief Destructor. Shuts down the worker thread.
	/// @note Never runs for the singleton, which is intentionally leaked (see Get()).
	~LineageClient();

	/// @brief Background worker thread function.
	/// @note Continuously processes events from the queue until shutdown is requested.
	void BackgroundWorker();

	/// @brief Send an HTTP POST request to the OpenLineage backend.
	/// @param payload JSON string to send in the request body.
	/// @return true if the backend accepted the event.
	/// @note Retries with exponential backoff, except while shutting down.
	bool PostToBackend(const std::string &payload);

	// ===== Queue Management =====
	std::mutex queue_mutex;              ///< Protects access to event_queue
	std::condition_variable queue_cv;    ///< Notifies worker when events are available
	std::queue<std::string> event_queue; ///< Queue of pending events to send

	// ===== Worker Thread Management =====
	std::thread worker_thread;            ///< Background worker thread
	std::atomic<bool> shutdown_requested; ///< Flag to signal worker shutdown

	// ===== Configuration State =====
	mutable std::mutex config_mutex; ///< Protects configuration fields
	std::string duck_lineage_url;    ///< Target URL for OpenLineage events
	std::string api_key;             ///< Optional API key for authentication
	std::string lineage_namespace;   ///< Namespace for lineage events
	bool debug_mode = false;         ///< Enable debug logging
	size_t max_retries = 3;          ///< Maximum number of retry attempts
	size_t max_queue_size = 10000;   ///< Maximum queue size to prevent memory issues
	int64_t timeout_seconds = 10;    ///< HTTP request timeout in seconds
	size_t dropped_events = 0;       ///< Counter for dropped events (queue full)
	std::vector<std::string> exclude_dataset_prefixes = {"__ducklake_metadata_"}; ///< Dataset name prefixes to exclude
	std::string ca_cert_file;           ///< Explicit CA bundle path (duck_lineage_ca_cert_file)
	std::string ca_cert_dir;            ///< Explicit CA directory path (duck_lineage_ca_cert_dir)
	std::string inherited_ca_cert_file; ///< CA bundle inherited from DuckDB's global ca_cert_file setting
	std::string proxy_url;              ///< Proxy URL for backend requests (duck_lineage_proxy)
	bool ssl_verify = true;             ///< Verify TLS peer + host (default true)
};

} // namespace duckdb
