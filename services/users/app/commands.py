import asyncio

from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import User
from app.repository import UserRepository
from app.schemas import (
    AuthenticatedEvent,
    AuthenticationFailedEvent,
    LoginCommand,
    RegisterCommand,
    RegisteredEvent,
    RegistrationFailedEvent,
)
from app.security import create_access_token, hash_password, verify_password

TOPIC_REGISTERED = "users.registered"
TOPIC_REGISTRATION_FAILED = "users.registration_failed"
TOPIC_AUTHENTICATED = "users.authenticated"
TOPIC_AUTHENTICATION_FAILED = "users.authentication_failed"


def _register_user(command: RegisterCommand) -> User | None:
    """Returns the created User, or None if the email is already registered."""
    session = SessionLocal()
    try:
        repository = UserRepository(session)
        if repository.get_by_email(command.email) is not None:
            return None
        return repository.create(
            name=command.name,
            email=command.email,
            password_hash=hash_password(command.password),
        )
    finally:
        session.close()


def _find_user_by_email(email: str) -> User | None:
    session = SessionLocal()
    try:
        return UserRepository(session).get_by_email(email)
    finally:
        session.close()


async def handle_register(payload: dict, correlation_id: str | None) -> None:
    try:
        command = RegisterCommand.model_validate(payload)
    except ValidationError:
        return

    user = await asyncio.to_thread(_register_user, command)

    if user is None:
        event = RegistrationFailedEvent(
            email=command.email, reason="email_already_registered"
        )
        await producer.publish(
            TOPIC_REGISTRATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = RegisteredEvent(user_id=str(user.id), email=user.email)
    await producer.publish(TOPIC_REGISTERED, event.model_dump(), correlation_id)


async def handle_login(payload: dict, correlation_id: str | None) -> None:
    try:
        command = LoginCommand.model_validate(payload)
    except ValidationError:
        return

    user = await asyncio.to_thread(_find_user_by_email, command.email)

    if user is None or not verify_password(command.password, user.password_hash):
        event = AuthenticationFailedEvent(
            email=command.email, reason="invalid_credentials"
        )
        await producer.publish(
            TOPIC_AUTHENTICATION_FAILED, event.model_dump(), correlation_id
        )
        return

    token = create_access_token(user_id=str(user.id), email=user.email)
    event = AuthenticatedEvent(user_id=str(user.id), token=token)
    await producer.publish(TOPIC_AUTHENTICATED, event.model_dump(), correlation_id)
