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

TOPIC_REGISTERED = "users.usuario.cadastrado"
TOPIC_REGISTRATION_FAILED = "users.usuario.cadastro_falhou"
TOPIC_AUTHENTICATED = "users.usuario.autenticado"
TOPIC_AUTHENTICATION_FAILED = "users.usuario.autenticacao_falhou"
TOPIC_PROFILE_FOUND = "users.perfil.encontrado"
TOPIC_PROFILE_UPDATED = "users.perfil.atualizado"
TOPIC_PROFILE_UPDATE_FAILED = "users.perfil.atualizacao_falhou"


async def _register_user(command: RegisterCommand) -> User | None:
    """Returns the created User, or None if the email is already registered."""
    async with SessionLocal() as session:
        repository = UserRepository(session)
        if await repository.get_by_email(command.email) is not None:
            return None
        return await repository.create(
            name=command.name,
            email=command.email,
            phone=command.phone,
            password_hash=hash_password(command.password),
        )


async def _find_user_by_email(email: str) -> User | None:
    async with SessionLocal() as session:
        return await UserRepository(session).get_by_email(email)


async def handle_register(payload: dict, correlation_id: str | None) -> None:
    try:
        command = RegisterCommand.model_validate(payload)
    except ValidationError:
        return

    user = await _register_user(command)

    if user is None:
        event = RegistrationFailedEvent(
            email=command.email, reason="email_already_registered"
        )
        await producer.publish(
            TOPIC_REGISTRATION_FAILED, event.model_dump(), correlation_id
        )
        return

    event = RegisteredEvent(user_id=str(user.id), email=user.email, phone=user.phone)
    await producer.publish(TOPIC_REGISTERED, event.model_dump(), correlation_id)


async def handle_login(payload: dict, correlation_id: str | None) -> None:
    try:
        command = LoginCommand.model_validate(payload)
    except ValidationError:
        return

    user = await _find_user_by_email(command.email)

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


async def _get_profile(user_id: str) -> User | None:
    async with SessionLocal() as session:
        return await UserRepository(session).get_by_id(user_id)


async def _update_profile(command: UpdateProfileCommand) -> User | None:
    """Returns the updated User, or None if the email is already used by another user."""
    async with SessionLocal() as session:
        repository = UserRepository(session)
        user = await repository.get_by_id(command.user_id)
        existing = await repository.get_by_email(command.email)
        if existing is not None and existing.id != user.id:
            return None
        return await repository.update(
            user, name=command.name, email=command.email, phone=command.phone
        )


async def handle_get_profile(payload: dict, correlation_id: str | None) -> None:
    try:
        command = GetProfileCommand.model_validate(payload)
    except ValidationError:
        return

    user = await _get_profile(command.user_id)
    if user is None:
        return

    event = ProfileFoundEvent(
        id=str(user.id), name=user.name, email=user.email, phone=user.phone
    )
    await producer.publish(TOPIC_PROFILE_FOUND, event.model_dump(), correlation_id)


async def handle_update_profile(payload: dict, correlation_id: str | None) -> None:
    try:
        command = UpdateProfileCommand.model_validate(payload)
    except ValidationError:
        return

    user = await _update_profile(command)

    if user is None:
        event = ProfileUpdateFailedEvent(reason="email_already_registered")
        await producer.publish(
            TOPIC_PROFILE_UPDATE_FAILED, event.model_dump(), correlation_id
        )
        return

    event = ProfileUpdatedEvent(
        id=str(user.id), name=user.name, email=user.email, phone=user.phone
    )
    await producer.publish(TOPIC_PROFILE_UPDATED, event.model_dump(), correlation_id)
