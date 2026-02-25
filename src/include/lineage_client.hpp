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

	/// @brief Get the number of events dropped due to queue overflow.
	/// @return Number of dropped events.
	/// @note Thread-safe.
	size_t GetDroppedEvents() const;

	/// @brief Signal the background worker to shut down.
	/// @note The worker thread will finish processing remaining events before stopping.
	/// @note This is called automatically in the destructor.
	void Shutdown();

private:
	/// @brief Private constructor for singleton pattern.
	/// @note Initializes CURL, starts the background worker thread.
	LineageClient();

	/// @brief Destructor. Shuts down the worker thread and cleans up CURL.
	~LineageClient();

	/// @brief Background worker thread function.
	/// @note Continuously processes events from the queue until shutdown is requested.
	void BackgroundWorker();

	/// @brief Send an HTTP POST request to the OpenLineage backend.
	/// @param payload JSON string to send in the request body.
	/// @note Uses CURL to perform the HTTP request with a 5-second timeout.
	void PostToBackend(const std::string &payload);

	// ===== Queue Management =====
	std::mutex queue_mutex;              ///< Protects access to event_queue
	std::condition_variable queue_cv;    ///< Notifies worker when events are available
	std::queue<std::string> event_queue; ///< Queue of pending events to send

	// ===== Worker Thread Management =====
	std::thread worker_thread;            ///< Background worker thread
	std::atomic<bool> shutdown_requested; ///< Flag to signal worker shutdown

	// ===== Configuration State =====
	mutable std::mutex config_mutex; ///< Protects configuration fields
	std::string openlineage_url;     ///< Target URL for OpenLineage events
	std::string api_key;             ///< Optional API key for authentication
	std::string lineage_namespace;   ///< Namespace for lineage events
	bool debug_mode = false;         ///< Enable debug logging
	size_t max_retries = 3;          ///< Maximum number of retry attempts
	size_t max_queue_size = 10000;   ///< Maximum queue size to prevent memory issues
	int64_t timeout_seconds = 10;    ///< HTTP request timeout in seconds
	size_t dropped_events = 0;       ///< Counter for dropped events (queue full)
};

} // namespace duckdb
