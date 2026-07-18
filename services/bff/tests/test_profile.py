import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_header(user_id: str = "user-123") -> dict:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


def test_get_me_requires_authentication():
    response = client.get("/me")

    assert response.status_code == 401


def test_get_me_rejects_invalid_token():
    response = client.get("/me", headers={"Authorization": "Bearer not-a-valid-token"})

    assert response.status_code == 401


def test_update_me_requires_authentication():
    response = client.put("/me", json={"name": "Joao", "email": "joao@email.com"})

    assert response.status_code == 401


def test_update_me_rejects_missing_fields():
    response = client.put("/me", headers=_auth_header(), json={"name": "Joao"})

    assert response.status_code == 422


def test_update_me_rejects_invalid_email():
    response = client.put(
        "/me",
        headers=_auth_header(),
        json={"name": "Joao", "email": "not-an-email"},
    )

    assert response.status_code == 422
