import base64
import json
import os
import logging

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.core.config import settings

logger = logging.getLogger(__name__)

# AES-256-GCM requires a 32-byte key
SECRET_KEY=settings.ENCRYPTION_KEY

if not SECRET_KEY:
    raise RuntimeError("ENCRYPTION_KEY not set in environment")

if isinstance(SECRET_KEY, str):
    SECRET_KEY = SECRET_KEY.encode()

if len(SECRET_KEY) != 32:
    raise ValueError("ENCRYPTION_KEY must be exactly 32 bytes for AES-256-GCM")


class DecryptionMiddleware(BaseHTTPMiddleware):
    """Middleware to decrypt AES-GCM encrypted request body on specific endpoints"""

    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.decrypt_paths = settings.DECRYPT_PATHS or []

    async def dispatch(self, request: Request, call_next):
        if request.method == "POST" and request.url.path in self.decrypt_paths:
            try:
                body = await request.json()
                encrypted_b64 = body.get("encrypted")
                nonce_b64 = body.get("nonce")

                if not encrypted_b64 or not nonce_b64:
                    raise HTTPException(status_code=400, detail="Missing 'encrypted' or 'nonce' field")

                encrypted_data = base64.b64decode(encrypted_b64)
                nonce = base64.b64decode(nonce_b64)

                aesgcm = AESGCM(SECRET_KEY)
                decrypted_bytes = aesgcm.decrypt(nonce, encrypted_data, None)
                decrypted_str = decrypted_bytes.decode("utf-8")
                decrypted_json = json.loads(decrypted_str)

                # Override request body with decrypted content
                async def receive():
                    return {
                        "type": "http.request",
                        "body": json.dumps(decrypted_json).encode("utf-8"),
                    }

                request._receive = receive
                logger.info(f"Decryption successful for {request.url.path}")

            except Exception as e:
                logger.error(f"Decryption failed for {request.url.path}: {e}")
                raise HTTPException(status_code=400, detail=f"Decryption failed: {str(e)}")

        response = await call_next(request)
        return response
