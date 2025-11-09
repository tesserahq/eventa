import json


def test_list_events_by_tags_returns_matching_events(
    client, setup_event_factory, faker
):
    matching = setup_event_factory(tags=["alpha", "beta"])
    setup_event_factory(tags=["beta", "gamma"])

    response = client.get("/events", params=[("tags", "alpha"), ("tags", "beta")])

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == str(matching.id)


def test_list_events_by_tags_with_labels(client, setup_event_factory):
    matching = setup_event_factory(tags=["release"], labels={"category": "news"})
    setup_event_factory(tags=["release"], labels={"category": "ops"})

    response = client.get(
        "/events",
        params=[
            ("tags", "release"),
            ("labels", json.dumps({"category": "news"})),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == str(matching.id)


def test_list_events_by_tags_invalid_labels_payload_returns_400(client):
    response = client.get(
        "/events",
        params=[("tags", "alpha"), ("labels", "not-json")],
    )

    assert response.status_code == 400
    assert "labels" in response.json()["detail"]
