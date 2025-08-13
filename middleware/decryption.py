import base64
import json
import logging
from typing import Callable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import HTTPException
from starlette.types import ASGIApp, Scope, Receive, Send, Message

from app.core.config import settings

logger = logging.getLogger(__name__)

# Decode and validate encryption key (must be 32 bytes after base64 decoding)
if not settings.encryption_key:
    raise RuntimeError("ENCRYPTION_KEY not set in environment")

try:
    DECRYPTION_KEY = base64.b64decode(settings.encryption_key)
except Exception:
    raise ValueError("ENCRYPTION_KEY must be base64-encoded")

if len(DECRYPTION_KEY) != 32:
    raise ValueError("ENCRYPTION_KEY must decode to exactly 32 bytes for AES-256-GCM")


class DecryptionMiddleware:
    """ASGI middleware to decrypt AES-GCM encrypted request body on specific endpoints."""

    def __init__(self, app: ASGIApp):
        self.app = app
        self.decrypt_paths = settings.decrypt_paths or []

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        # Only process HTTP POST requests matching configured paths
        if (
            scope["type"] == "http"
            and scope["method"] == "POST"
            and scope["path"] in self.decrypt_paths
        ):
            logger.debug(f"DecryptionMiddleware: intercepting path {scope['path']}")

            # Read the entire request body
            body_bytes = b""
            more_body = True
            while more_body:
                message: Message = await receive()
                body_bytes += message.get("body", b"")
                more_body = message.get("more_body", False)

            try:
                # Parse JSON
                body = json.loads(body_bytes)
                logger.debug(f"Encrypted request body: {body}")

                encrypted_b64 = body.get("encrypted")
                nonce_b64 = body.get("nonce")

                if not encrypted_b64 or not nonce_b64:
                    raise HTTPException(
                        status_code=400,
                        detail="Missing 'encrypted' or 'nonce' field",
                    )

                # Decode from base64
                try:
                    encrypted_data = base64.b64decode(encrypted_b64)
                    nonce = base64.b64decode(nonce_b64)
                except Exception:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid base64 encoding in 'encrypted' or 'nonce'",
                    )

                if len(nonce) != 12:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid nonce length (must be 12 bytes)",
                    )

                # Perform AES-GCM decryption
                aesgcm = AESGCM(DECRYPTION_KEY)
                decrypted_bytes = aesgcm.decrypt(nonce, encrypted_data, None)
                decrypted_str = decrypted_bytes.decode("utf-8")
                decrypted_json = json.loads(decrypted_str)

                logger.info(
                    f"Decryption successful for path: {scope['path']}"
                )
                logger.debug(
                    f"Decrypted JSON: {json.dumps(decrypted_json, indent=2)}"
                )

                # Replace the request body with decrypted JSON
                new_body_bytes = json.dumps(decrypted_json).encode("utf-8")

                async def new_receive() -> Message:
                    return {
                        "type": "http.request",
                        "body": new_body_bytes,
                        "more_body": False,
                    }

                receive = new_receive

            except HTTPException:
                raise  # Pass FastAPI HTTP errors as-is
            except Exception as e:
                logger.error(f"Decryption failed for {scope['path']}: {e}")
                raise HTTPException(status_code=400, detail="Decryption failed")

        # Continue down the middleware/app chain
        await self.app(scope, receive, send)





