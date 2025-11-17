import json


def test_list_events_by_user_id_returns_matching_events(
    client, setup_event_factory, setup_user, setup_another_user
):
    """Test that filtering by user_id returns only events for that user."""
    # Create events for different users
    matching_event = setup_event_factory(user_id=setup_user.id)
    setup_event_factory(user_id=setup_another_user.id)  # Different user
    setup_event_factory(user_id=None)  # Event without user

    response = client.get("/events", params=[("user_id", str(setup_user.id))])

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == str(matching_event.id)
    assert payload["items"][0]["user_id"] == str(setup_user.id)


def test_list_events_by_user_id_with_multiple_events(
    client, setup_event_factory, setup_user
):
    """Test that filtering by user_id returns all events for that user."""
    event1 = setup_event_factory(user_id=setup_user.id, tags=["tag1"])
    event2 = setup_event_factory(user_id=setup_user.id, tags=["tag2"])

    response = client.get("/events", params=[("user_id", str(setup_user.id))])

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    event_ids = [item["id"] for item in payload["items"]]
    assert str(event1.id) in event_ids
    assert str(event2.id) in event_ids


def test_list_events_by_user_id_and_tags_returns_400(client, setup_user):
    """Test that providing both user_id and tags returns 400 error."""
    response = client.get(
        "/events",
        params=[("user_id", str(setup_user.id)), ("tags", "alpha")],
    )

    assert response.status_code == 400
    assert "both user_id and tags" in response.json()["detail"].lower()


def test_list_events_without_user_id_or_tags_returns_422(client):
    """Test that providing neither user_id nor tags returns 422 error."""
    response = client.get("/events")

    assert response.status_code == 422
    assert "user_id or tags" in response.json()["detail"].lower()


def test_list_events_by_tags_returns_matching_events(
    client, setup_event_factory, faker
):
    matching = setup_event_factory(tags=["alpha", "beta"])
    setup_event_factory(
        tags=["beta", "gamma"]
    )  # This won't match since it doesn't have "alpha"

    response = client.get("/events", params=[("tags", "alpha"), ("tags", "beta")])

    assert response.status_code == 200
    payload = response.json()
    # Only matching event should be returned since it has both "alpha" and "beta"
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
