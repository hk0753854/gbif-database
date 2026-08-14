from gbif_data_pipeline.gbif_client import search_occurrences


def test_search_occurrences_pagination(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params.copy())

        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                offset = params["offset"]

                if offset == 0:
                    results = [
                        {"gbifID": "1"},
                        {"gbifID": "2"},
                    ]
                elif offset == 2:
                    results = [
                        {"gbifID": "3"},
                        {"gbifID": "4"},
                    ]
                else:
                    results = []

                return {
                    "results": results,
                    "count": 4,
                }

        return FakeResponse()

    monkeypatch.setattr(
        "gbif_data_pipeline.gbif_client.requests.get",
        fake_get,
    )

    results = search_occurrences(
        scientific_name="Hynobius nebulosus",
        limit=4,
    )

    assert len(results) == 4

    assert [r["gbifID"] for r in results] == [
        "1",
        "2",
        "3",
        "4",
    ]

    assert calls[0]["offset"] == 0
    assert calls[1]["offset"] == 2