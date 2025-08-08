import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

SECRET_KEY = os.environ.get("ENCRYPTION_KEY", "A" * 32).encode()  # Must be 32 bytes

def decrypt_payload(encrypted_b64: str, nonce_b64: str) -> str:
    encrypted_data = base64.b64decode(encrypted_b64)
    nonce = base64.b64decode(nonce_b64)
    
    aesgcm = AESGCM(SECRET_KEY)
    decrypted_data = aesgcm.decrypt(nonce, encrypted_data, None)
    return decrypted_data.decode("utf-8")
