import asyncio

from pydantic import ValidationError

from app.database import SessionLocal
from app.kafka_producer import producer
from app.models import User
from app.repository import UserRepository
from app.schemas import (
    AuthenticatedEvent,
    AuthenticationFailedEvent,
    GetProfileCommand,
    LoginCommand,
    ProfileFoundEvent,
    ProfileUpdatedEvent,
    ProfileUpdateFailedEvent,
    RegisterCommand,
    RegisteredEvent,
    RegistrationFailedEvent,
    UpdateProfileCommand,
)
from app.security import create_access_token, hash_password, verify_password

TOPIC_REGISTERED = "users.registered"
TOPIC_REGISTRATION_FAILED = "users.registration_failed"
TOPIC_AUTHENTICATED = "users.authenticated"
TOPIC_AUTHENTICATION_FAILED = "users.authentication_failed"
TOPIC_PROFILE_FOUND = "users.profile_found"
TOPIC_PROFILE_UPDATED = "users.profile_updated"
TOPIC_PROFILE_UPDATE_FAILED = "users.profile_update_failed"


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


def _get_profile(user_id: str) -> User | None:
    session = SessionLocal()
    try:
        return UserRepository(session).get_by_id(user_id)
    finally:
        session.close()


def _update_profile(command: UpdateProfileCommand) -> User | None:
    """Returns the updated User, or None if the email is already used by another user."""
    session = SessionLocal()
    try:
        repository = UserRepository(session)
        user = repository.get_by_id(command.user_id)
        existing = repository.get_by_email(command.email)
        if existing is not None and existing.id != user.id:
            return None
        return repository.update(user, name=command.name, email=command.email)
    finally:
        session.close()


async def handle_get_profile(payload: dict, correlation_id: str | None) -> None:
    try:
        command = GetProfileCommand.model_validate(payload)
    except ValidationError:
        return

    user = await asyncio.to_thread(_get_profile, command.user_id)
    if user is None:
        return

    event = ProfileFoundEvent(id=str(user.id), name=user.name, email=user.email)
    await producer.publish(TOPIC_PROFILE_FOUND, event.model_dump(), correlation_id)


async def handle_update_profile(payload: dict, correlation_id: str | None) -> None:
    try:
        command = UpdateProfileCommand.model_validate(payload)
    except ValidationError:
        return

    user = await asyncio.to_thread(_update_profile, command)

    if user is None:
        event = ProfileUpdateFailedEvent(reason="email_already_registered")
        await producer.publish(
            TOPIC_PROFILE_UPDATE_FAILED, event.model_dump(), correlation_id
        )
        return

    event = ProfileUpdatedEvent(id=str(user.id), name=user.name, email=user.email)
    await producer.publish(TOPIC_PROFILE_UPDATED, event.model_dump(), correlation_id)
