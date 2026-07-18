import jwt
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_header(user_id: str = "user-123") -> dict:
    token = jwt.encode({"sub": user_id}, "test-secret", algorithm="HS256")
    return {"Authorization": f"Bearer {token}"}


def test_search_ads_requires_authentication():
    response = client.get("/ads/search")

    assert response.status_code == 401


def test_search_ads_with_query_requires_authentication():
    response = client.get("/ads/search", params={"q": "notebook"})

    assert response.status_code == 401
