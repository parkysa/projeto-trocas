from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_list_notifications_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Consulta", "topico": "notifications.notificacao.consultar", "payload": {}}
        )
        response = ws.receive_json()

    assert response["topico"] == "notifications.notificacao.consultar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"
