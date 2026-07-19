import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _ws_url(user_id: str = "user-123") -> str:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return f"/ws?token={token}"


def test_create_trade_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "trades.troca.solicitar",
                "payload": {"requester_ad_id": "ad-1", "target_ad_id": "ad-2"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "trades.troca.solicitar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_create_trade_rejects_missing_fields():
    with client.websocket_connect(_ws_url()) as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "trades.troca.solicitar",
                "payload": {"requester_ad_id": "ad-1"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "trades.troca.solicitar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_accept_trade_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "trades.troca.aceitar", "payload": {"id": "some-id"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "trades.troca.aceitar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_reject_trade_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "trades.troca.recusar", "payload": {"id": "some-id"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "trades.troca.recusar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_cancel_trade_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "trades.troca.cancelar", "payload": {"id": "some-id"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "trades.troca.cancelar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"
