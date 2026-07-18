import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_header(user_id: str = "user-123") -> dict:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


def test_create_trade_requires_authentication():
    response = client.post(
        "/trades", json={"requester_ad_id": "ad-1", "target_ad_id": "ad-2"}
    )

    assert response.status_code == 401


def test_create_trade_rejects_missing_fields():
    response = client.post(
        "/trades", headers=_auth_header(), json={"requester_ad_id": "ad-1"}
    )

    assert response.status_code == 422


def test_accept_trade_requires_authentication():
    response = client.post("/trades/some-id/accept")

    assert response.status_code == 401


def test_reject_trade_requires_authentication():
    response = client.post("/trades/some-id/reject")

    assert response.status_code == 401


def test_cancel_trade_requires_authentication():
    response = client.post("/trades/some-id/cancel")

    assert response.status_code == 401
