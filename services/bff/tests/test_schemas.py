from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_register_rejects_missing_fields():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.usuario.cadastrar",
                "payload": {"name": "Joao"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.usuario.cadastrar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_register_rejects_invalid_email():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.usuario.cadastrar",
                "payload": {
                    "name": "Joao",
                    "email": "not-an-email",
                    "password": "12345678",
                },
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.usuario.cadastrar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_login_rejects_missing_password():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.usuario.autenticar",
                "payload": {"email": "joao@email.com"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.usuario.autenticar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_login_rejects_invalid_email():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "users.usuario.autenticar",
                "payload": {"email": "not-an-email", "password": "12345678"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "users.usuario.autenticar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"
