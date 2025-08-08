import base64
import json
import logging

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.core.config import settings

logger = logging.getLogger(__name__)

# Decode and validate encryption key (must be 32 bytes after base64 decoding)
if not settings.ENCRYPTION_KEY:
    raise RuntimeError("ENCRYPTION_KEY not set in environment")

try:
    DECRYPTION_KEY = base64.b64decode(settings.ENCRYPTION_KEY)
except Exception:
    raise ValueError("ENCRYPTION_KEY must be base64-encoded")

if len(DECRYPTION_KEY) != 32:
    raise ValueError("ENCRYPTION_KEY must decode to exactly 32 bytes for AES-256-GCM")


class DecryptionMiddleware(BaseHTTPMiddleware):
    """Middleware to decrypt AES-GCM encrypted request body on specific endpoints"""

    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.decrypt_paths = settings.DECRYPT_PATHS or []

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if request.method == "POST" and path in self.decrypt_paths:
            try:
                body = await request.json()
                encrypted_b64 = body.get("encrypted")
                nonce_b64 = body.get("nonce")

                if not encrypted_b64 or not nonce_b64:
                    raise HTTPException(status_code=400, detail="Missing 'encrypted' or 'nonce' field")

                try:
                    encrypted_data = base64.b64decode(encrypted_b64)
                    nonce = base64.b64decode(nonce_b64)
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid base64 encoding in 'encrypted' or 'nonce'")

                if len(nonce) != 12:
                    raise HTTPException(status_code=400, detail="Invalid nonce length (must be 12 bytes)")

                aesgcm = AESGCM(DECRYPTION_KEY)
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
                logger.info(f"Decryption successful for path: {path}")

            except HTTPException:
                raise  # re-raise cleanly
            except Exception as e:
                logger.error(f"Decryption failed for {path}: {e}")
                raise HTTPException(status_code=400, detail="Decryption failed")

        response = await call_next(request)
        return response
