import requests

GBIF_OCCURRENCE_API = "https://api.gbif.org/v1/occurrence/search"

DEFAULT_PAGE_SIZE = 300


def search_occurrences(
    scientific_name: str,
    limit: int = 10,
) -> list[dict]:
    """GBIF occurrence APIから観察記録を取得する。"""

    occurrences: list[dict] = []
    offset = 0

    while len(occurrences) < limit:
        page_size = min(
            DEFAULT_PAGE_SIZE,
            limit - len(occurrences),
        )

        params = {
            "scientificName": scientific_name,
            "limit": page_size,
            "offset": offset,
        }

        response = requests.get(
            GBIF_OCCURRENCE_API,
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()

        results = data["results"]
        total_count = data["count"]

        if not results:
            break

        occurrences.extend(results)
        offset += len(results)

        if offset >= total_count:
            break

    return occurrences[:limit]