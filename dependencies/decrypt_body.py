import json

from fastapi import Request, HTTPException

from app.utils.encryption import decrypt_payload

async def decrypt_request_body(request: Request) -> dict:
    try:
        body = await request.json()
        encrypted = body.get("encrypted")
        nonce = body.get("nonce")
        
        if not encrypted or not nonce:
            raise HTTPException(status_code=400, detail="Missing encrypted data or nonce")
        
        decrypted_str = decrypt_payload(encrypted, nonce)
        decrypted_json = json.loads(decrypted_str)
        return decrypted_json
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Decryption failed: {str(e)}")
