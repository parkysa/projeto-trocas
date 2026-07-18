from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_list_notifications_requires_authentication():
    response = client.get("/notifications")

    assert response.status_code == 401
