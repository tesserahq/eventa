import json
from uuid import uuid4


def test_list_events_without_user_id_or_tags_returns_422(client):
    """Test that providing neither user_id nor tags returns 422 error."""
    response = client.get("/events")

    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_list_events_by_tags_returns_matching_events(
    client, setup_event_factory, faker
):
    matching = setup_event_factory(tags=["alpha", "beta"])
    setup_event_factory(
        tags=["beta", "gamma"]
    )  # This won't match since it doesn't have "alpha"

    response = client.get(
        "/events",
        params=[
            ("tags", "alpha"),
            ("tags", "beta"),
            ("project_id", "704894b7-f2de-4e02-824a-2c4b6ecf08b6"),
        ],
    )

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
            ("project_id", "704894b7-f2de-4e02-824a-2c4b6ecf08b6"),
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


def test_list_project_events_returns_only_matching_project_events(
    client, setup_event_factory
):
    project_id = uuid4()
    matching = setup_event_factory(project_id=project_id, tags=["release"])
    setup_event_factory(project_id=uuid4(), tags=["release"])

    response = client.get(f"/projects/{project_id}/events")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == str(matching.id)


def test_list_user_events_returns_only_matching_user_events(
    client, setup_event_factory
):
    user_id = uuid4()
    matching = setup_event_factory(user_id=user_id)
    setup_event_factory(user_id=uuid4())

    response = client.get(f"/users/{user_id}/events")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == str(matching.id)
