from app.security import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)


def test_hash_password_does_not_store_plaintext():
    password_hash = hash_password("12345678")

    assert password_hash != "12345678"
    assert verify_password("12345678", password_hash)


def test_verify_password_rejects_wrong_password():
    password_hash = hash_password("12345678")

    assert not verify_password("wrong-password", password_hash)


def test_create_and_decode_access_token_roundtrip():
    token = create_access_token(user_id="user-123", email="joao@email.com")

    payload = decode_access_token(token)

    assert payload["sub"] == "user-123"
    assert payload["email"] == "joao@email.com"
