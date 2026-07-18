import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_header(user_id: str = "user-123") -> dict:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


def test_create_ad_requires_authentication():
    response = client.post(
        "/ads", json={"title": "Notebook", "description": "Em bom estado."}
    )

    assert response.status_code == 401


def test_create_ad_rejects_missing_fields():
    response = client.post("/ads", headers=_auth_header(), json={"title": "Notebook"})

    assert response.status_code == 422


def test_list_ads_requires_authentication():
    response = client.get("/ads")

    assert response.status_code == 401


def test_update_ad_requires_authentication():
    response = client.put(
        "/ads/some-id", json={"title": "Notebook", "description": "Em bom estado."}
    )

    assert response.status_code == 401


def test_update_ad_rejects_missing_fields():
    response = client.put(
        "/ads/some-id", headers=_auth_header(), json={"title": "Notebook"}
    )

    assert response.status_code == 422


def test_delete_ad_requires_authentication():
    response = client.delete("/ads/some-id")

    assert response.status_code == 401
