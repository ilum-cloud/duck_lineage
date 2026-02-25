//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_client.cpp
// Description: Implementation of the LineageClient HTTP client.
//              Manages asynchronous sending of OpenLineage events via CURL.
//===----------------------------------------------------------------------===//

#include "lineage_client.hpp"
#include <curl/curl.h>
#include <iostream>
#include <chrono>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Singleton Instance Management
//===--------------------------------------------------------------------===//

LineageClient &LineageClient::Get() {
	static LineageClient instance;
	return instance;
}

//===--------------------------------------------------------------------===//
// Constructor / Destructor
//===--------------------------------------------------------------------===//

LineageClient::LineageClient() : shutdown_requested(false), openlineage_url(""), lineage_namespace("duckdb") {
	// Start the background worker thread to process events asynchronously
	worker_thread = std::thread(&LineageClient::BackgroundWorker, this);
}

LineageClient::~LineageClient() {
	// Request shutdown and wait for worker thread to finish
	Shutdown();
	if (worker_thread.joinable()) {
		worker_thread.join();
	}
}

void LineageClient::Shutdown() {
	// Signal the worker thread to stop processing
	shutdown_requested = true;
	// Wake up the worker if it's waiting on the condition variable
	queue_cv.notify_all();
}

//===--------------------------------------------------------------------===//
// Event Sending
//===--------------------------------------------------------------------===//

void LineageClient::SendEvent(std::string event_json) {
	// If debug mode is enabled, print the event to stdout
	if (IsDebug()) {
		std::cout << "OpenLineage Debug: " << event_json << '\n';
	}

	// Add the event to the queue for asynchronous processing
	{
		std::lock_guard<std::mutex> lock(queue_mutex);

		// Check if queue has reached maximum size
		size_t max_size;
		{
			std::lock_guard<std::mutex> cfg_lock(config_mutex);
			max_size = max_queue_size;
		}

		if (event_queue.size() >= max_size) {
			// Queue is full - drop the event and increment counter
			std::lock_guard<std::mutex> cfg_lock(config_mutex);
			dropped_events++;
			if (IsDebug()) {
				std::cerr << "OpenLineage Debug: Queue full (" << max_size
				          << "). Event dropped. Total dropped: " << dropped_events << '\n';
			}
			return;
		}

		event_queue.push(std::move(event_json));
	}

	// Notify the worker thread that a new event is available
	queue_cv.notify_one();
}

//===--------------------------------------------------------------------===//
// Configuration Setters (Thread-Safe)
//===--------------------------------------------------------------------===//

void LineageClient::SetUrl(std::string url) {
	std::lock_guard<std::mutex> lock(config_mutex);
	openlineage_url = std::move(url);
}

void LineageClient::SetApiKey(std::string key) {
	std::lock_guard<std::mutex> lock(config_mutex);
	api_key = std::move(key);
}

void LineageClient::SetNamespace(std::string ns) {
	std::lock_guard<std::mutex> lock(config_mutex);
	lineage_namespace = std::move(ns);
}

void LineageClient::SetDebug(bool debug) {
	std::lock_guard<std::mutex> lock(config_mutex);
	debug_mode = debug;
}

void LineageClient::SetMaxRetries(size_t retries) {
	std::lock_guard<std::mutex> lock(config_mutex);
	max_retries = retries;
}

void LineageClient::SetMaxQueueSize(size_t size) {
	std::lock_guard<std::mutex> lock(config_mutex);
	max_queue_size = size;
}

void LineageClient::SetTimeout(int64_t timeout) {
	std::lock_guard<std::mutex> lock(config_mutex);
	timeout_seconds = timeout;
}

//===--------------------------------------------------------------------===//
// Configuration Getters (Thread-Safe)
//===--------------------------------------------------------------------===//

std::string LineageClient::GetUrl() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return openlineage_url;
}

std::string LineageClient::GetApiKey() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return api_key;
}

std::string LineageClient::GetNamespace() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	// Return default namespace if the configured one is empty
	if (lineage_namespace.empty()) {
		return "duckdb";
	}
	return lineage_namespace;
}

bool LineageClient::IsDebug() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return debug_mode;
}

size_t LineageClient::GetMaxRetries() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return max_retries;
}

size_t LineageClient::GetMaxQueueSize() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return max_queue_size;
}

int64_t LineageClient::GetTimeout() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return timeout_seconds;
}

size_t LineageClient::GetDroppedEvents() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return dropped_events;
}

//===--------------------------------------------------------------------===//
// HTTP Request Handling
//===--------------------------------------------------------------------===//

/// @brief CURL callback for handling HTTP response data.
/// @note We don't need to store the response, so we just discard it.
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	// Return the number of bytes "processed" (discarded)
	return size * nmemb;
}

void LineageClient::PostToBackend(const std::string &payload) {
	// Retrieve configuration (thread-safely)
	std::string url;
	std::string key;
	size_t retries;
	int64_t timeout;
	{
		std::lock_guard<std::mutex> lock(config_mutex);
		url = openlineage_url;
		key = api_key;
		retries = max_retries;
		timeout = timeout_seconds;
	}

	if (url.empty()) {
		if (IsDebug()) {
			std::cerr << "OpenLineage Debug: OpenLineage URL is not configured. Event not sent." << '\n';
		}
		return;
	}

	CURL *curl = curl_easy_init();
	if (!curl) {
		if (IsDebug()) {
			std::cerr << "OpenLineage Debug: Failed to initialize CURL." << '\n';
		}
		return;
	}

	// Build HTTP headers
	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, "Content-Type: application/json");

	// Add Bearer token authentication if API key is configured
	if (!key.empty()) {
		std::string auth = "Authorization: Bearer " + key;
		headers = curl_slist_append(headers, auth.c_str());
	}

	// Configure CURL request options (reusable across retries)
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);
	// Enable TCP keepalive for better connection reliability
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
	// Follow redirects
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);

	// Retry loop with exponential backoff
	bool success = false;
	for (size_t attempt = 0; attempt <= retries && !success; ++attempt) {
		if (IsDebug() && attempt > 0) {
			std::cout << "OpenLineage Debug: Retry attempt " << attempt << "/" << retries << '\n';
		}

		if (IsDebug() && attempt == 0) {
			std::cout << "OpenLineage Debug: Sending to URL: " << url << '\n';
		}

		// Execute the HTTP request
		CURLcode res = curl_easy_perform(curl);

		if (res == CURLE_OK) {
			// Check HTTP response code
			int64_t response_code = 0;
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

			if (IsDebug()) {
				std::cout << "OpenLineage Debug: Request successful. Response Code: " << response_code << '\n';
			}

			// Success: 2xx response codes
			if (response_code >= 200 && response_code < 300) {
				success = true;
			}
			// Client errors (4xx) - don't retry (except 429 Too Many Requests)
			else if (response_code >= 400 && response_code < 500) {
				if (response_code == 429) {
					// Rate limited - retry with backoff
					if (IsDebug()) {
						std::cerr << "OpenLineage Debug: Rate limited (429). Will retry." << '\n';
					}
				} else {
					// Other client errors - don't retry
					if (IsDebug()) {
						std::cerr << "OpenLineage Debug: Client error " << response_code << ". Not retrying." << '\n';
					}
					break;
				}
			}
			// Server errors (5xx) - retry
			else if (response_code >= 500) {
				if (IsDebug()) {
					std::cerr << "OpenLineage Debug: Server error " << response_code << ". Will retry." << '\n';
				}
			}
		} else {
			// Network/CURL error - retry
			if (IsDebug()) {
				std::cerr << "OpenLineage Debug: CURL error: " << curl_easy_strerror(res) << ". Will retry." << '\n';
			}
		}

		// Apply exponential backoff before retry (skip on last attempt or success)
		if (!success && attempt < retries) {
			// Exponential backoff: 100ms, 200ms, 400ms, 800ms, etc.
			size_t backoff_ms = static_cast<size_t>(100) * (static_cast<size_t>(1) << attempt);
			// Cap at 5 seconds
			if (backoff_ms > 5000) {
				backoff_ms = 5000;
			}
			if (IsDebug()) {
				std::cout << "OpenLineage Debug: Backing off for " << backoff_ms << "ms" << '\n';
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
		}
	}

	if (!success && IsDebug()) {
		std::cerr << "OpenLineage Debug: Failed to send event after " << (retries + 1) << " attempts." << '\n';
	}

	// Cleanup CURL resources
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
}

//===--------------------------------------------------------------------===//
// Background Worker Thread
//===--------------------------------------------------------------------===//

void LineageClient::BackgroundWorker() {
	// Continuously process events until shutdown is requested
	while (!shutdown_requested) {
		std::string payload;

		// Wait for an event to become available or shutdown signal
		{
			std::unique_lock<std::mutex> lock(queue_mutex);
			queue_cv.wait(lock, [this] { return !event_queue.empty() || shutdown_requested; });

			// If shutdown requested and queue is empty, exit the worker
			if (shutdown_requested && event_queue.empty()) {
				return;
			}

			// Retrieve the next event from the queue
			if (!event_queue.empty()) {
				payload = std::move(event_queue.front());
				event_queue.pop();
			}
		}

		// Send the event to the backend (outside the lock)
		if (!payload.empty()) {
			PostToBackend(payload);
		}
	}
}

} // namespace duckdb
