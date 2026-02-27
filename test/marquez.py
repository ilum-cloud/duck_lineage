from marquez_client import MarquezClient
from time import sleep, time

import requests


class TestMarquezClient:
    def __init__(self, url: str, retries: int = 6) -> None:
        self.url = url
        self.retries = retries
        self.client = MarquezClient(url)

    def list_dataset_names(self, namespace: str) -> list[str]:
        datasets: dict | None = None
        for i in range(self.retries):
            try:
                datasets = self.client.list_datasets(namespace)  # type: ignore
            except Exception:
                sleep(min(2**i, 5))
            else:
                break

        if datasets is None:
            raise RuntimeError("Failed to retrieve datasets from Marquez after multiple attempts.")

        return [ds['name'] for ds in datasets.get('datasets', [])]

    def get_dataset(self, namespace: str, name: str) -> dict | None:
        """Get a dataset from Marquez, returning None if not found after retries."""
        dataset: dict | None = None
        last_error = None

        for i in range(self.retries):
            try:
                dataset = self.client.get_dataset(namespace, name)  # type: ignore
            except Exception as e:
                # Dataset might not exist yet, retry
                last_error = e
                sleep(min(2**i, 5))
            else:
                break

        if dataset is None:
            # Return None instead of raising - let tests decide how to handle missing datasets
            return None

        return dataset

    def wait_for_dataset(self, namespace: str, name: str, timeout_seconds: int = 30) -> dict | None:
        """Wait for a dataset to appear in Marquez, with exponential backoff."""
        start_time = time()
        attempt = 0

        while time() - start_time < timeout_seconds:
            attempt += 1
            try:
                dataset = self.client.get_dataset(namespace, name)  # type: ignore
                if dataset is not None:
                    return dataset
            except Exception:
                # Dataset not ready yet
                pass

            # Exponential backoff with jitter
            sleep_time = min(2 ** min(attempt, 6), 5) + (attempt % 10) * 0.1
            sleep(sleep_time)

        return None

    def wait_for_dataset_with_facets(
        self, namespace: str, name: str, required_facets: list[str], timeout_seconds: int = 30
    ) -> dict | None:
        """Wait for a dataset to appear with specific facets populated."""
        start_time = time()
        attempt = 0

        while time() - start_time < timeout_seconds:
            attempt += 1
            try:
                dataset = self.client.get_dataset(namespace, name)  # type: ignore
                if dataset is not None:
                    facets = dataset.get("facets", {})
                    if all(facet in facets for facet in required_facets):
                        return dataset
            except Exception:
                # Dataset not ready yet
                pass

            # Exponential backoff with jitter
            sleep_time = min(2 ** min(attempt, 6), 5) + (attempt % 10) * 0.1
            sleep(sleep_time)

        return None

    def wait_for_dataset_with_fields(self, namespace: str, name: str, timeout_seconds: int = 30) -> dict | None:
        """Wait for a dataset to appear with fields populated."""
        start_time = time()
        attempt = 0

        while time() - start_time < timeout_seconds:
            attempt += 1
            try:
                dataset = self.client.get_dataset(namespace, name)  # type: ignore
                if dataset is not None:
                    fields = dataset.get("fields", [])
                    if fields:  # Fields list is non-empty
                        return dataset
            except Exception:
                # Dataset not ready yet
                pass

            # Exponential backoff with jitter
            sleep_time = min(2 ** min(attempt, 6), 5) + (attempt % 10) * 0.1
            sleep(sleep_time)

        return None

    def list_jobs(self, namespace: str) -> list[dict]:
        for i in range(self.retries):
            try:
                result = self.client.list_jobs(namespace)  # type: ignore
                return result.get('jobs', [])
            except Exception:
                sleep(min(2**i, 5))
        return []

    def get_job(self, namespace: str, job_name: str) -> dict | None:
        for i in range(self.retries):
            try:
                return self.client.get_job(namespace, job_name)  # type: ignore
            except Exception:
                sleep(min(2**i, 5))
        return None

    def _fetch_events(self, namespace: str, limit: int = 100) -> list[dict]:
        """Fetch events directly from the Marquez lineage events endpoint, filtered by namespace."""
        url = f"{self.url}/api/v1/events/lineage"
        resp = requests.get(url, params={"limit": limit}, timeout=10)
        resp.raise_for_status()
        all_events = resp.json().get("events", [])
        return [e for e in all_events if (e.get("job") or {}).get("namespace") == namespace]

    def wait_for_events(self, namespace: str, min_events: int, timeout_seconds: int = 30) -> list[dict]:
        """Poll events endpoint until at least min_events appear for the namespace."""
        start_time = time()
        attempt = 0

        while time() - start_time < timeout_seconds:
            attempt += 1
            try:
                events = self._fetch_events(namespace)
                if len(events) >= min_events:
                    return events
            except Exception:
                pass

            sleep_time = min(2 ** min(attempt, 6), 5) + (attempt % 10) * 0.1
            sleep(sleep_time)

        # Return whatever we have, even if below min_events
        try:
            return self._fetch_events(namespace)
        except Exception:
            return []
