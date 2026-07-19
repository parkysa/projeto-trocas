import jwt

from app.config import settings


class AuthenticationError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def decode_user_id(token: str) -> str:
    """Decodes a JWT and returns the user_id (sub claim); raises AuthenticationError on failure."""
    try:
        payload = jwt.decode(
            token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm]
        )
    except jwt.PyJWTError:
        raise AuthenticationError("invalid_token")

    return payload["sub"]
