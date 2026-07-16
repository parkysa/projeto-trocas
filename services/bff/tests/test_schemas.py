from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_register_rejects_missing_fields():
    response = client.post("/register", json={"name": "Joao"})

    assert response.status_code == 422


def test_register_rejects_invalid_email():
    response = client.post(
        "/register",
        json={"name": "Joao", "email": "not-an-email", "password": "12345678"},
    )

    assert response.status_code == 422


def test_login_rejects_missing_password():
    response = client.post("/login", json={"email": "joao@email.com"})

    assert response.status_code == 422


def test_login_rejects_invalid_email():
    response = client.post(
        "/login", json={"email": "not-an-email", "password": "12345678"}
    )

    assert response.status_code == 422
