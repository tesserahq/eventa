"""process_nats_event_task stores the event in one transaction and onboards
an unknown user in another, calling Identies with no transaction open."""

from uuid import uuid4

import pytest
from tessera_sdk.infra import current_session

from app.models.event import Event
from app.models.user import User
from app.schemas.user import UserOnboard
from app.tasks import process_nats_event
from app.tasks.process_nats_event import process_nats_event_task


@pytest.fixture
def task_sessions(db, execution_boundary, monkeypatch):
    """Each session_scope() in the task runs against the test session."""
    monkeypatch.setattr(process_nats_event, "session_scope", execution_boundary)


def _message(user_id=None):
    return {
        "source": "tests",
        "event_type": "thing.happened",
        "subject": "thing",
        "event_data": {"a": 1},
        "user_id": str(user_id) if user_id else None,
    }


def test_unknown_user_is_onboarded_without_an_open_transaction(
    db, task_sessions, faker, monkeypatch
):
    user_id = uuid4()

    def fetch(requested_id):
        # Identies is called between the two transactions.
        assert current_session() is None
        return UserOnboard(
            id=user_id,
            email=faker.email(),
            first_name=faker.first_name(),
            last_name=faker.last_name(),
            provider="google",
            external_id=str(uuid4()),
        )

    monkeypatch.setattr(process_nats_event, "_fetch_identies_user", fetch)

    process_nats_event_task(_message(user_id))

    assert db.query(Event).filter(Event.user_id == user_id).count() == 1
    assert db.query(User).filter(User.id == user_id).count() == 1


def test_identies_failure_keeps_the_event(db, task_sessions, monkeypatch):
    user_id = uuid4()

    def fail(requested_id):
        raise RuntimeError("identies unavailable")

    monkeypatch.setattr(process_nats_event, "_fetch_identies_user", fail)

    process_nats_event_task(_message(user_id))

    assert db.query(Event).filter(Event.user_id == user_id).count() == 1
    assert db.query(User).filter(User.id == user_id).count() == 0


def test_known_user_is_not_fetched(db, task_sessions, test_user, monkeypatch):
    def unexpected(requested_id):
        raise AssertionError("Identies must not be called for a known user")

    monkeypatch.setattr(process_nats_event, "_fetch_identies_user", unexpected)

    process_nats_event_task(_message(test_user.id))

    assert db.query(Event).filter(Event.user_id == test_user.id).count() == 1


def test_event_without_time_defaults_to_now(db, task_sessions):
    """The time field is optional in the envelope but required on the model."""
    user_id = uuid4()
    message = _message()
    message["labels"] = {"marker": str(user_id)}

    process_nats_event_task(message)

    event = (
        db.query(Event).filter(Event.labels.contains({"marker": str(user_id)})).one()
    )
    assert event.time is not None
