//===----------------------------------------------------------------------===//
// DuckDB OpenLineage Extension
//
// File: lineage_client.cpp
// Description: Implementation of the LineageClient HTTP client.
//              Manages asynchronous sending of OpenLineage events via CURL.
//===----------------------------------------------------------------------===//

#include "lineage_client.hpp"
#include "duckdb/common/string_util.hpp"
#include <curl/curl.h>
#include <openssl/crypto.h>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Singleton Instance Management
//===--------------------------------------------------------------------===//

void LineageClient::InitializeHttpLibraries() {
	static std::once_flag init_flag;
	std::call_once(init_flag, [] {
		// OpenSSL registers an atexit handler (OPENSSL_cleanup) the first time it is initialized. libcurl would do
		// that lazily on the worker thread, i.e. after our own exit hook was registered, so at process exit OpenSSL
		// was torn down *before* the worker finished draining the queue and the next HTTPS request crashed inside
		// OpenSSL. Initialize it here, first, and opt out of its atexit cleanup: the OS reclaims the memory anyway.
		OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr);
		// Must run once before any other thread uses libcurl.
		curl_global_init(CURL_GLOBAL_DEFAULT);
	});
}

LineageClient &LineageClient::Get() {
	// The instance is intentionally never destroyed. DuckDB objects that own a PhysicalLineageSentinel (prepared
	// statements, pending results, connections held by statics of the host application) can be destroyed during
	// static destruction, and the sentinel destructor calls back into this client. A function-local static object
	// would already be gone at that point. Queued events are flushed by the atexit hook registered below.
	static LineageClient *instance = [] {
		auto *client = new LineageClient();
		std::atexit([] { LineageClient::Get().Shutdown(); });
		return client;
	}();
	return *instance;
}

//===--------------------------------------------------------------------===//
// Constructor / Destructor
//===--------------------------------------------------------------------===//

LineageClient::LineageClient() : shutdown_requested(false), duck_lineage_url(""), lineage_namespace("duckdb") {
	InitializeHttpLibraries();
	// Start the background worker thread to process events asynchronously
	worker_thread = std::thread(&LineageClient::BackgroundWorker, this);
}

LineageClient::~LineageClient() {
	Shutdown();
}

void LineageClient::Shutdown() {
	// Signal the worker thread to stop processing
	{
		std::lock_guard<std::mutex> lock(queue_mutex);
		shutdown_requested = true;
	}
	// Wake up the worker if it's waiting on the condition variable or backing off between retries
	queue_cv.notify_all();
	// Wait for the worker to deliver what is still queued
	if (worker_thread.joinable()) {
		worker_thread.join();
	}
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

		// The worker is stopped at process exit; events emitted after that (e.g. by sentinels destroyed
		// during static destruction) can no longer be delivered
		if (shutdown_requested) {
			return;
		}

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
	duck_lineage_url = std::move(url);
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

void LineageClient::SetExcludeDatasetPrefixes(const std::string &prefixes_csv) {
	std::lock_guard<std::mutex> lock(config_mutex);
	exclude_dataset_prefixes.clear();
	if (prefixes_csv.empty()) {
		return;
	}
	auto tokens = duckdb::StringUtil::Split(prefixes_csv, ',');
	for (auto &token : tokens) {
		duckdb::StringUtil::Trim(token);
		if (!token.empty()) {
			exclude_dataset_prefixes.push_back(std::move(token));
		}
	}
}

void LineageClient::SetCaCertFile(std::string path) {
	std::lock_guard<std::mutex> lock(config_mutex);
	ca_cert_file = std::move(path);
}

void LineageClient::SetCaCertDir(std::string path) {
	std::lock_guard<std::mutex> lock(config_mutex);
	ca_cert_dir = std::move(path);
}

void LineageClient::SetInheritedCaCertFile(std::string path) {
	std::lock_guard<std::mutex> lock(config_mutex);
	inherited_ca_cert_file = std::move(path);
}

void LineageClient::SetSslVerify(bool verify) {
	std::lock_guard<std::mutex> lock(config_mutex);
	ssl_verify = verify;
}

void LineageClient::SetProxy(std::string proxy) {
	std::lock_guard<std::mutex> lock(config_mutex);
	proxy_url = std::move(proxy);
}

//===--------------------------------------------------------------------===//
// Configuration Getters (Thread-Safe)
//===--------------------------------------------------------------------===//

std::string LineageClient::GetUrl() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return duck_lineage_url;
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

std::vector<std::string> LineageClient::GetExcludeDatasetPrefixes() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return exclude_dataset_prefixes;
}

size_t LineageClient::GetDroppedEvents() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return dropped_events;
}

std::string LineageClient::GetCaCertFile() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return ca_cert_file;
}

std::string LineageClient::GetCaCertDir() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return ca_cert_dir;
}

bool LineageClient::GetSslVerify() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return ssl_verify;
}

std::string LineageClient::GetProxy() const {
	std::lock_guard<std::mutex> lock(config_mutex);
	return proxy_url;
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

bool LineageClient::PostToBackend(const std::string &payload) {
	// Retrieve configuration (thread-safely)
	std::string url;
	std::string key;
	size_t retries;
	int64_t timeout;
	std::string ca_file;
	std::string ca_dir;
	std::string inherited_ca_file;
	std::string proxy;
	bool verify;
	{
		std::lock_guard<std::mutex> lock(config_mutex);
		url = duck_lineage_url;
		key = api_key;
		retries = max_retries;
		timeout = timeout_seconds;
		ca_file = ca_cert_file;
		ca_dir = ca_cert_dir;
		inherited_ca_file = inherited_ca_cert_file;
		proxy = proxy_url;
		verify = ssl_verify;
	}

	if (url.empty()) {
		if (IsDebug()) {
			std::cerr << "OpenLineage Debug: OpenLineage URL is not configured. Event not sent." << '\n';
		}
		return false;
	}

	CURL *curl = curl_easy_init();
	if (!curl) {
		if (IsDebug()) {
			std::cerr << "OpenLineage Debug: Failed to initialize CURL." << '\n';
		}
		return false;
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

	// TLS / proxy configuration.
	// The bundled libcurl is statically linked against OpenSSL and may not have a usable
	// default CA store on all platforms (notably macOS), so HTTPS verification fails unless we
	// provide a CA bundle. Resolve the CA file with the following precedence:
	//   1. duck_lineage_ca_cert_file (explicit)
	//   2. DuckDB's global "ca_cert_file" setting (inherited from httpfs config)
	//   3. CURL_CA_BUNDLE environment variable
	//   4. SSL_CERT_FILE environment variable
	//   5. libcurl's compiled-in default
	std::string resolved_ca_file = ca_file;
	const char *ca_file_source = "duck_lineage_ca_cert_file";
	if (resolved_ca_file.empty() && !inherited_ca_file.empty()) {
		resolved_ca_file = inherited_ca_file;
		ca_file_source = "ca_cert_file setting";
	}
	if (resolved_ca_file.empty()) {
		if (const char *env = std::getenv("CURL_CA_BUNDLE")) {
			resolved_ca_file = env;
			ca_file_source = "CURL_CA_BUNDLE";
		} else if (const char *ssl_env = std::getenv("SSL_CERT_FILE")) {
			resolved_ca_file = ssl_env;
			ca_file_source = "SSL_CERT_FILE";
		}
	}
	if (!resolved_ca_file.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, resolved_ca_file.c_str());
		if (IsDebug()) {
			std::cout << "OpenLineage Debug: Using CA bundle from " << ca_file_source << ": " << resolved_ca_file
			          << '\n';
		}
	}

	// Resolve the CA directory: explicit duck_lineage_ca_cert_dir, then SSL_CERT_DIR env var.
	std::string resolved_ca_dir = ca_dir;
	if (resolved_ca_dir.empty()) {
		if (const char *env = std::getenv("SSL_CERT_DIR")) {
			resolved_ca_dir = env;
		}
	}
	if (!resolved_ca_dir.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAPATH, resolved_ca_dir.c_str());
	}

	// Optionally disable TLS verification (insecure — intended for development/testing only).
	if (!verify) {
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
		if (IsDebug()) {
			std::cerr << "OpenLineage Debug: TLS certificate verification is DISABLED "
			             "(duck_lineage_ssl_verify=false). This is insecure."
			          << '\n';
		}
	}

	// Route through an explicit proxy when configured. When empty, libcurl still honors the
	// standard http_proxy/https_proxy/ALL_PROXY/NO_PROXY environment variables.
	if (!proxy.empty()) {
		curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
	}

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

		// Don't hold up process exit with retries: if the backend is failing now, it is unlikely to recover
		// within the next few hundred milliseconds
		if (!success && shutdown_requested) {
			break;
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
			// Sleep, but wake up immediately when shutdown is requested
			std::unique_lock<std::mutex> lock(queue_mutex);
			queue_cv.wait_for(lock, std::chrono::milliseconds(backoff_ms),
			                  [this] { return shutdown_requested.load(); });
		}
	}

	if (!success && IsDebug()) {
		std::cerr << "OpenLineage Debug: Failed to send event after " << (retries + 1) << " attempts." << '\n';
	}

	// Cleanup CURL resources
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	return success;
}

//===--------------------------------------------------------------------===//
// Background Worker Thread
//===--------------------------------------------------------------------===//

void LineageClient::BackgroundWorker() {
	while (true) {
		std::vector<std::string> batch;

		{
			std::unique_lock<std::mutex> lock(queue_mutex);
			queue_cv.wait(lock, [this] { return !event_queue.empty() || shutdown_requested; });

			if (shutdown_requested && event_queue.empty()) {
				return;
			}

			// Drain all available events into a local batch
			while (!event_queue.empty()) {
				batch.push_back(std::move(event_queue.front()));
				event_queue.pop();
			}
		}

		// Send all events outside the lock
		for (auto &payload : batch) {
			bool delivered = PostToBackend(payload);
			if (!delivered && shutdown_requested) {
				// The backend is unreachable while the process is exiting: the remaining events would fail
				// the same way, each one delaying exit by up to the request timeout. Give up.
				if (IsDebug()) {
					std::cerr << "OpenLineage Debug: Delivery failed during shutdown. Dropping remaining events."
					          << '\n';
				}
				return;
			}
		}
	}
}

} // namespace duckdb
