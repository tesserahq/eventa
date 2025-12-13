import pytest
from unittest.mock import Mock, patch
import jwt
from fastapi import HTTPException
from tessera_sdk.utils.auth import (
    VerifyToken,
    UnauthorizedException,
    UnauthenticatedException,
)
from app.models.user import User

# Mock JWT token for testing
MOCK_TOKEN = "mock.jwt.token"
MOCK_USER_ID = "test-user-id"
MOCK_EMAIL = "test@example.com"
MOCK_NAME = "Test User"
MOCK_AVATAR = "https://example.com/avatar.jpg"


@pytest.fixture
def mock_jwks_client():
    with patch("jwt.PyJWKClient") as mock:
        mock_instance = Mock()
        mock.return_value = mock_instance
        mock_instance.get_signing_key_from_jwt.return_value = Mock(
            key="mock-signing-key"
        )
        yield mock_instance


@pytest.fixture
def mock_user_service_factory():
    """Create a mock user service factory."""
    mock_user_service = Mock()

    def factory():
        return mock_user_service

    return factory, mock_user_service


@pytest.fixture
def verifier(db, mock_jwks_client, mock_user_service_factory):
    user_service_factory, _ = mock_user_service_factory
    return VerifyToken(user_service_factory=user_service_factory)


def test_verify_token_success(verifier, mock_jwks_client, mock_user_service_factory):
    _, mock_user_service = mock_user_service_factory

    # Mock JWT decode
    mock_payload = {"sub": MOCK_USER_ID}
    with patch("jwt.decode", return_value=mock_payload):
        # Mock userinfo response
        mock_userinfo = {
            "email": MOCK_EMAIL,
            "first_name": "Test",
            "last_name": "User",
            "avatar_url": MOCK_AVATAR,
        }
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_userinfo
            mock_get.return_value = mock_response

            # Mock existing user
            mock_user = User(
                id=1,
                external_id=MOCK_USER_ID,
                email=MOCK_EMAIL,
                first_name="Test",
                last_name="User",
                avatar_url=MOCK_AVATAR,
            )
            mock_user_service.get_user_by_external_id.return_value = mock_user

            # Test verification
            result = verifier.verify(MOCK_TOKEN)

            assert isinstance(result, User)
            assert result.external_id == MOCK_USER_ID
            assert result.email == MOCK_EMAIL
            mock_user_service.get_user_by_external_id.assert_called_once_with(
                MOCK_USER_ID
            )
            mock_user_service.onboard_user.assert_not_called()


def test_verify_token_new_user(verifier, mock_jwks_client, mock_user_service_factory):
    _, mock_user_service = mock_user_service_factory

    # Mock JWT decode
    mock_payload = {"sub": MOCK_USER_ID}
    with patch("jwt.decode", return_value=mock_payload):
        # Mock new user (user doesn't exist in database)
        mock_user_service.get_user_by_external_id.return_value = None

        # Test verification
        result = verifier.verify(MOCK_TOKEN)

        # Should return UserNeedsOnboarding for new users
        from tessera_sdk.schemas.user import UserNeedsOnboarding

        assert isinstance(result, UserNeedsOnboarding)
        assert result.external_id == MOCK_USER_ID
        assert result.needs_onboarding == True
        mock_user_service.get_user_by_external_id.assert_called_once_with(MOCK_USER_ID)
        # Should not call onboard_user - that's handled by UserOnboardingMiddleware
        mock_user_service.onboard_user.assert_not_called()


def test_verify_token_invalid_token(verifier, mock_jwks_client):
    mock_jwks_client.get_signing_key_from_jwt.side_effect = (
        jwt.exceptions.PyJWKClientError("Invalid token")
    )

    with pytest.raises(HTTPException) as exc_info:
        verifier.verify(MOCK_TOKEN)

    assert exc_info.value.status_code == 401


def test_verify_token_missing_token(verifier):
    with pytest.raises(UnauthenticatedException):
        verifier.verify(None)


def test_verify_token_userinfo_failure(
    verifier, mock_jwks_client, mock_user_service_factory
):
    _, mock_user_service = mock_user_service_factory

    # Mock JWT decode
    mock_payload = {"sub": MOCK_USER_ID}
    with patch("jwt.decode", return_value=mock_payload):
        # Mock user service to return None (user doesn't exist)
        mock_user_service.get_user_by_external_id.return_value = None

        # Test verification - should return UserNeedsOnboarding for new users
        result = verifier.verify(MOCK_TOKEN)

        # Should return UserNeedsOnboarding for new users
        from tessera_sdk.schemas.user import UserNeedsOnboarding

        assert isinstance(result, UserNeedsOnboarding)
        assert result.external_id == MOCK_USER_ID
        assert result.needs_onboarding == True
        mock_user_service.get_user_by_external_id.assert_called_once_with(MOCK_USER_ID)


def test_verify_token_invalid_payload(verifier, mock_jwks_client):
    with patch("jwt.decode", side_effect=jwt.InvalidTokenError("Invalid token")):
        with pytest.raises(UnauthorizedException) as exc_info:
            verifier.verify(MOCK_TOKEN)

        assert "Invalid token" in str(exc_info.value)
