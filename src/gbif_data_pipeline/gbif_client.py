import requests

from gbif_data_pipeline.logging_config import get_logger

GBIF_OCCURRENCE_API = "https://api.gbif.org/v1/occurrence/search"

logger = get_logger(__name__)


def search_occurrences(
    scientific_name: str,
    limit: int = 10,
) -> list[dict]:
    """GBIF occurrence APIから観察記録を取得する。"""

    logger.info(
        "Searching GBIF occurrences: scientific_name=%s, limit=%s",
        scientific_name,
        limit,
    )

    results = []

    offset = 0

    while len(results) < limit:
        batch_limit = min(300, limit - len(results))

        params = {
            "scientificName": scientific_name,
            "limit": batch_limit,
            "offset": offset,
        }

        try:
            response = requests.get(
                GBIF_OCCURRENCE_API,
                params=params,
                timeout=30,
            )
            response.raise_for_status()

        except requests.RequestException:
            logger.exception(
                "GBIF API request failed: scientific_name=%s, offset=%s",
                scientific_name,
                offset,
            )
            raise

        data = response.json()
        batch = data.get("results", [])

        if not batch:
            break

        results.extend(batch)
        offset += len(batch)

    results = results[:limit]

    logger.info(
        "GBIF search completed: scientific_name=%s, records=%s",
        scientific_name,
        len(results),
    )

    return results