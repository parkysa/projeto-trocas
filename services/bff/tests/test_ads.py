import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _ws_url(user_id: str = "user-123") -> str:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return f"/ws?token={token}"


def test_create_ad_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "ads.anuncio.criar",
                "payload": {"title": "Notebook", "description": "Em bom estado."},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.criar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_create_ad_rejects_missing_fields():
    with client.websocket_connect(_ws_url()) as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "ads.anuncio.criar", "payload": {"title": "Notebook"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.criar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_list_ads_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Consulta", "topico": "ads.anuncio.consultar_proprios", "payload": {}}
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.consultar_proprios_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_update_ad_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "ads.anuncio.atualizar",
                "payload": {"id": "some-id", "title": "Notebook", "description": "Em bom estado."},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.atualizar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_update_ad_rejects_missing_fields():
    with client.websocket_connect(_ws_url()) as ws:
        ws.send_json(
            {
                "tipo": "Comando",
                "topico": "ads.anuncio.atualizar",
                "payload": {"id": "some-id", "title": "Notebook"},
            }
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.atualizar_falhou"
    assert response["payload"]["reason"] == "invalid_payload"


def test_delete_ad_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Comando", "topico": "ads.anuncio.remover", "payload": {"id": "some-id"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.remover_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"
