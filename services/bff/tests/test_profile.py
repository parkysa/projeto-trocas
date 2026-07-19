import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _ws_url(user_id: str = "user-123") -> str:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return f"/ws?token={token}"


def test_get_profile_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"tipo": "Consulta", "topico": "users.perfil.consultar", "payload": {}})
        response = ws.receive_json()

    assert response["topico"] == "users.perfil.consultar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_get_profile_rejects_invalid_token():
    with client.websocket_connect("/ws?token=not-a-valid-token") as ws:
        ws.send_json({"tipo": "Consulta", "topico": "users.perfil.consultar", "payload": {}})
        response = ws.receive_json()

    assert response["topico"] == "users.perfil.consultar_nao_autorizado"
    assert response["payload"]["reason"] == "invalid_token"


def test_update_profile_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.perfil.atualizar",
                "payload": {"name": "Joao", "email": "joao@email.com"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.perfil.atualizar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_update_profile_rejects_missing_fields():
    with client.websocket_connect(_ws_url()) as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "users.perfil.atualizar", "payload": {"name": "Joao"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "users.perfil.atualizar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_update_profile_rejects_invalid_email():
    with client.websocket_connect(_ws_url()) as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.perfil.atualizar",
                "payload": {"name": "Joao", "email": "not-an-email"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.perfil.atualizar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"
