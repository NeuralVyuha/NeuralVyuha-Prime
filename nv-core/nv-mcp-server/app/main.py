import os
import sys
import logging
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Depends
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

_base_dir = os.path.dirname(__file__)
for _p in [os.path.abspath(os.path.join(_base_dir, '../../')), os.path.abspath(os.path.join(_base_dir, '../'))]:
    if os.path.exists(os.path.join(_p, 'common')): sys.path.append(_p); break

from common.auth.middleware import get_auth_context, AuthContext
from .llm_client import chat_with_copilot, chat_with_copilot_stream
from fastapi.responses import StreamingResponse

app = FastAPI(title="NeuralVyuha Threat Copilot Server")
logging.basicConfig(level=logging.INFO)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    model_provider: str = 'ollama' # 'openai', 'claude', 'ollama'
    model_name: str = 'llama3.1'
    context: Optional[Dict[str, Any]] = None

@app.post("/copilot/chat")
async def copilot_chat(req: ChatRequest, auth: AuthContext = Depends(get_auth_context)):
    import hashlib
    import httpx
    import json
    
    # 1. Cache Check Logic
    case_id = req.context.get("caseId") if req.context else None
    user_msg = req.message

    if case_id:
        raw_str = f"{case_id}::{user_msg}"
        input_hash = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()
        
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                node_svc_url = os.environ.get("NODE_SERVICE_URL", "http://node-service:8085")
                resp = await client.get(f"{node_svc_url}/internal/investigation-cache/{input_hash}")
                if resp.status_code == 200:
                    data = resp.json()
                    saved_messages = data.get("investigation_data", {}).get("messages", [])
                    assistant_content = ""
                    for m in saved_messages:
                        if m.get("role") == "assistant":
                            assistant_content += m.get("content", "")
                    
                    async def cached_stream():
                        # Indicate Cache Hit
                        yield f"data: {{json.dumps({'type': 'thought', 'content': '⚡ Cache Hit: Retrieved verified investigation from database.'})}}\n\n"
                        yield f"data: {{json.dumps({'type': 'message', 'content': assistant_content})}}\n\n"
                        yield "data: [DONE]\n\n"
                        
                    logging.info(f"Copilot cache hit for hash {input_hash}")
                    return StreamingResponse(cached_stream(), media_type="text/event-stream")
        except Exception as e:
            logging.error(f"Cache check failed: {e}")

    # 2. Proceed with actual LLM generation if no cache hit
    try:
        return StreamingResponse(
            chat_with_copilot_stream(
                user_message=req.message,
                provider=req.model_provider,
                model_name=req.model_name,
                context=req.context
            ),
            media_type="text/event-stream"
        )
    except Exception as e:
        logging.error(f"Copilot error: {e}")
        return {"response": f"AI Engine Encountered a Critical Error: {str(e)}"}

@app.get("/healthz")
def healthz(): return {"status": "ok"}
