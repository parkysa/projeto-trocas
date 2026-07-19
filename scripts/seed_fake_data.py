#!/usr/bin/env python3
"""Seed fake users and trade items through the BFF WebSocket API.

Usage examples:
  python seed_fake_data.py
  python seed_fake_data.py --users 20 --items-per-user 4
  python seed_fake_data.py --ws-url ws://localhost:8000/ws --password "Troca123A"

Requirements:
  pip install websockets
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import websockets


REGISTER_TOPIC = "users.usuario.cadastrar"
LOGIN_TOPIC = "users.usuario.autenticar"
CREATE_AD_TOPIC = "ads.anuncio.criar"

REGISTER_SUCCESS_TOPIC = "users.usuario.cadastrado"
REGISTER_FAILED_TOPIC = "users.usuario.cadastro_falhou"
REGISTER_VALIDATION_FAILED_TOPIC = "users.usuario.cadastrar_falhou"
LOGIN_SUCCESS_TOPIC = "users.usuario.autenticado"
LOGIN_FAILED_TOPIC = "users.usuario.autenticacao_falhou"
LOGIN_VALIDATION_FAILED_TOPIC = "users.usuario.autenticar_falhou"
CREATE_AD_SUCCESS_TOPIC = "ads.anuncio.criado"
CREATE_AD_FAILED_TOPIC = "ads.anuncio.operacao_falhou"
CREATE_AD_VALIDATION_FAILED_TOPIC = "ads.anuncio.criar_falhou"


FIRST_NAMES = [
    "Ana",
    "Bruno",
    "Carla",
    "Diego",
    "Elisa",
    "Fabio",
    "Giovana",
    "Hugo",
    "Isabela",
    "Joao",
    "Karen",
    "Lucas",
    "Marina",
    "Nicolas",
    "Olivia",
    "Paulo",
    "Quenia",
    "Rafael",
    "Sofia",
    "Thiago",
]

LAST_NAMES = [
    "Almeida",
    "Barros",
    "Cardoso",
    "Dias",
    "Esteves",
    "Ferreira",
    "Gomes",
    "Henrique",
    "Ibrahim",
    "Junqueira",
    "Klein",
    "Lopes",
    "Moraes",
    "Nascimento",
    "Oliveira",
    "Pereira",
    "Queiroz",
    "Ramos",
    "Silva",
    "Teixeira",
]

ITEM_TITLES = [
    "Livro de algoritmos",
    "Calculadora cientifica",
    "Mochila escolar",
    "Fone bluetooth",
    "Teclado mecanico",
    "Mouse gamer",
    "Jaleco tamanho M",
    "Cadeira de escritorio",
    "Monitor 24 polegadas",
    "Lampada de mesa",
    "Violao iniciante",
    "Patins semi-novo",
    "Bicicleta urbana",
    "Notebook antigo",
    "Kindle basico",
]

LOCATIONS = [
    "Sao Paulo, SP",
    "Campinas, SP",
    "Santos, SP",
    "Guarulhos, SP",
    "Sao Bernardo, SP",
    "Osasco, SP",
]

CATEGORIES = [
    "Livros",
    "Eletronicos",
    "Roupas",
    "Acessorios",
    "Casa",
    "Esporte",
    "Geral",
]

CONDITIONS = ["novo", "como_novo", "bom", "usado"]

IMAGE_ASSET_PATHS = [
    "/assets/bolsa.png",
    "/assets/fone.png",
    "/assets/garrafa.png",
    "/assets/item-exemplo.png",
    "/assets/mesa.png",
    "/assets/microondas.png",
    "/assets/mouse.png",
    "/assets/varal.png",
]


@dataclass
class FakeUser:
    name: str
    email: str
    phone: str


def build_fake_users(total: int) -> list[FakeUser]:
    users: list[FakeUser] = []
    for i in range(1, total + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"seed.user{i:03d}@troca.com"
        phone = f"(11) 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        users.append(FakeUser(name=name, email=email, phone=phone))
    return users


def build_item_payload(index: int) -> dict[str, Any]:
    title = random.choice(ITEM_TITLES)
    category = random.choice(CATEGORIES)
    condition = random.choice(CONDITIONS)
    location = random.choice(LOCATIONS)
    image_path = random.choice(IMAGE_ASSET_PATHS)

    return {
        "title": f"{title} #{index}",
        "description": f"Item em estado {condition}. Disponivel para troca por algo similar.",
        "image": image_path,
        "image_position": "50% 50%",
        "category": category,
        "condition": condition,
        "location": location,
        "trade_terms": "Aceito propostas de valor equivalente.",
    }


async def ws_request(ws_url: str, topic: str, payload: dict[str, Any], token: str | None = None) -> dict[str, Any]:
    uri = ws_url
    if token:
        sep = "&" if "?" in ws_url else "?"
        uri = f"{ws_url}{sep}token={quote(token)}"

    request_body = {"tipo": "Comando" if topic not in {"ads.anuncio.buscar"} else "Consulta", "topico": topic, "payload": payload}

    async with websockets.connect(uri, open_timeout=12, close_timeout=12) as ws:
        await ws.send(json.dumps(request_body))
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        return json.loads(raw)


async def register_or_login(ws_url: str, user: FakeUser, password: str) -> str:
    register_response = await ws_request(
        ws_url,
        REGISTER_TOPIC,
        {
            "name": user.name,
            "email": user.email,
            "phone": user.phone,
            "password": password,
        },
    )

    topico = register_response.get("topico", "")
    if topico not in {
        REGISTER_SUCCESS_TOPIC,
        REGISTER_FAILED_TOPIC,
        REGISTER_VALIDATION_FAILED_TOPIC,
    }:
        raise RuntimeError(f"Unexpected register response topic for {user.email}: {topico}")

    if topico in {REGISTER_FAILED_TOPIC, REGISTER_VALIDATION_FAILED_TOPIC}:
        reason = (register_response.get("payload") or {}).get("reason")
        if topico == REGISTER_VALIDATION_FAILED_TOPIC:
            raise RuntimeError(f"Register validation failed for {user.email}: {reason}")
        if reason != "email_already_registered":
            raise RuntimeError(f"Register failed for {user.email}: {reason}")

    login_response = await ws_request(
        ws_url,
        LOGIN_TOPIC,
        {"email": user.email, "password": password},
    )

    if login_response.get("topico") in {LOGIN_FAILED_TOPIC, LOGIN_VALIDATION_FAILED_TOPIC}:
        reason = (login_response.get("payload") or {}).get("reason")
        raise RuntimeError(
            f"Login failed for {user.email}: topic={login_response.get('topico')} reason={reason}"
        )

    if login_response.get("topico") != LOGIN_SUCCESS_TOPIC:
        raise RuntimeError(
            f"Unexpected login response topic for {user.email}: {login_response.get('topico')}"
        )

    token = ((login_response.get("payload") or {}).get("access_token"))
    if not token:
        raise RuntimeError(f"Missing access_token on login response for {user.email}")

    return str(token)


async def create_user_ads(ws_url: str, token: str, amount: int, item_offset: int) -> int:
    created = 0
    for i in range(amount):
        payload = build_item_payload(item_offset + i)
        response = await ws_request(ws_url, CREATE_AD_TOPIC, payload, token=token)
        topico = response.get("topico")

        if topico in {CREATE_AD_VALIDATION_FAILED_TOPIC, CREATE_AD_FAILED_TOPIC}:
            reason = (response.get("payload") or {}).get("reason")
            if reason == "invalid_payload":
                # Fallback for backends that still accept only title/description.
                minimal_payload = {
                    "title": payload["title"],
                    "description": payload["description"],
                }
                response = await ws_request(ws_url, CREATE_AD_TOPIC, minimal_payload, token=token)
                topico = response.get("topico")

        if topico != CREATE_AD_SUCCESS_TOPIC:
            raise RuntimeError(f"Create ad failed. Topic: {topico}, payload: {response.get('payload')}")
        created += 1
    return created


async def seed_data(ws_url: str, users_count: int, items_per_user: int, password: str) -> None:
    users = build_fake_users(users_count)
    total_items = 0

    for idx, user in enumerate(users, start=1):
        token = await register_or_login(ws_url, user, password)
        created = await create_user_ads(ws_url, token, items_per_user, item_offset=idx * 100)
        total_items += created
        print(f"[{idx}/{users_count}] user={user.email} -> items_created={created}")

    print("\nSeeding complete")
    print(f"Users processed: {users_count}")
    print(f"Items created: {total_items}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed fake users and ads via BFF WebSocket.")
    parser.add_argument("--ws-url", default="ws://localhost:8000/ws", help="BFF WebSocket URL")
    parser.add_argument("--users", type=int, default=12, help="Number of fake users")
    parser.add_argument("--items-per-user", type=int, default=3, help="Ads created per user")
    parser.add_argument("--password", default="Troca123A", help="Password used for all fake users")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.users <= 0:
        raise ValueError("--users must be greater than 0")
    if args.items_per_user <= 0:
        raise ValueError("--items-per-user must be greater than 0")


if __name__ == "__main__":
    try:
        cli_args = parse_args()
        validate_args(cli_args)
        asyncio.run(
            seed_data(
                ws_url=cli_args.ws_url,
                users_count=cli_args.users,
                items_per_user=cli_args.items_per_user,
                password=cli_args.password,
            )
        )
    except KeyboardInterrupt:
        print("\nExecution interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nError: {exc}")
        sys.exit(1)
