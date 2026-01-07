def test_list_my_events_returns_current_user_events(
    client, setup_event_factory, setup_user, setup_another_user
):
    """Test that /me endpoint returns only events for the authenticated user."""
    # Create events for different users
    my_event1 = setup_event_factory(user_id=setup_user.id, tags=["tag1"])
    my_event2 = setup_event_factory(user_id=setup_user.id, tags=["tag2"])
    # Event for another user - should not be returned
    setup_event_factory(user_id=setup_another_user.id, tags=["tag3"])
    # Event without user - should not be returned
    setup_event_factory(user_id=None, tags=["tag4"])

    response = client.get("/me")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    event_ids = [item["id"] for item in payload["items"]]
    assert str(my_event1.id) in event_ids
    assert str(my_event2.id) in event_ids


def test_list_my_events_with_pagination(client, setup_event_factory, setup_user):
    """Test that /me endpoint supports pagination."""
    # Create multiple events for the user
    [setup_event_factory(user_id=setup_user.id, tags=[f"tag{i}"]) for i in range(5)]

    # Request first page with size 2
    response = client.get("/me", params={"size": 2, "page": 1})

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 5
    assert len(payload["items"]) == 2
    assert payload["page"] == 1
    assert payload["size"] == 2


def test_list_my_events_returns_empty_when_no_events(client, setup_user):
    """Test that /me endpoint returns empty list when user has no events."""
    response = client.get("/me")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 0
    assert payload["items"] == []


def test_list_my_events_only_returns_own_events(
    client, client_another_user, setup_event_factory, setup_user, setup_another_user
):
    """Test that each user only sees their own events."""
    # Create events for both users
    user1_event = setup_event_factory(user_id=setup_user.id, tags=["user1"])
    user2_event = setup_event_factory(user_id=setup_another_user.id, tags=["user2"])

    # Request as user1
    response1 = client.get("/me")
    assert response1.status_code == 200
    payload1 = response1.json()
    assert payload1["total"] == 1
    assert payload1["items"][0]["id"] == str(user1_event.id)

    # Request as user2
    response2 = client_another_user.get("/me")
    assert response2.status_code == 200
    payload2 = response2.json()
    assert payload2["total"] == 1
    assert payload2["items"][0]["id"] == str(user2_event.id)
