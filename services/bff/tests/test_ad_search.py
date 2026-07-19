from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_search_available_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Consulta", "topico": "ads.anuncio.consultar_disponiveis", "payload": {}}
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.consultar_disponiveis_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"


def test_search_ads_with_query_requires_authentication():
    with client.websocket_connect("/ws") as ws:
        ws.send_json(
            {"tipo": "Consulta", "topico": "ads.anuncio.buscar", "payload": {"q": "notebook"}}
        )
        response = ws.receive_json()

    assert response["topico"] == "ads.anuncio.buscar_nao_autorizado"
    assert response["payload"]["reason"] == "missing_token"
