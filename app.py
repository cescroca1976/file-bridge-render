import os
import asyncio
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, Header
from fastapi.responses import StreamingResponse, JSONResponse

TOKEN = os.environ.get("BRIDGE_TOKEN", "")  # set this in Render
MAX_BYTES = int(os.environ.get("MAX_BYTES", str(25 * 1024 * 1024)))  # 25 MB default

app = FastAPI(title="File Bridge Relay")

# In-memory map of token -> websocket connection to the B agent
agents: Dict[str, WebSocket] = {}
# token -> lock to serialize requests to that agent
locks: Dict[str, asyncio.Lock] = {}

def require_token(x_auth_token: Optional[str]):
    if not TOKEN:
        return
    if x_auth_token != TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

@app.get("/ping")
async def ping():
    return {"ok": True}

@app.get("/status")
async def status(x_auth_token: Optional[str] = Header(default=None)):
    require_token(x_auth_token)
    return {"agent_connected": TOKEN in agents}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    # simple handshake: first message must be the token
    try:
        token_msg = await asyncio.wait_for(ws.receive_text(), timeout=10)
    except Exception:
        await ws.close(code=1008)
        return

    if TOKEN and token_msg != TOKEN:
        await ws.close(code=1008)
        return

    agents[TOKEN] = ws
    locks.setdefault(TOKEN, asyncio.Lock())

    try:
        # keep alive: just wait for disconnect; agent can also send "pong"
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        if agents.get(TOKEN) is ws:
            agents.pop(TOKEN, None)

async def agent_call(command: str, payload: Optional[bytes] = None) -> bytes:
    if TOKEN not in agents:
        raise HTTPException(status_code=503, detail="agent_not_connected")

    ws = agents[TOKEN]
    lock = locks[TOKEN]

    async with lock:
        await ws.send_text(command)
        if payload is not None:
            await ws.send_bytes(payload)
        # response: bytes (first frame) and optional second frame (not used)
        try:
            data = await asyncio.wait_for(ws.receive_bytes(), timeout=120)
        except Exception as e:
            raise HTTPException(status_code=504, detail=f"agent_timeout:{e}")
        return data

@app.get("/list")
async def list_files(x_auth_token: Optional[str] = Header(default=None)):
    require_token(x_auth_token)
    data = await agent_call("LIST")
    # data is utf-8 JSON
    return JSONResponse(content=json_loads_safe(data))

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), x_auth_token: Optional[str] = Header(default=None)):
    require_token(x_auth_token)
    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=413, detail=f"file_too_large_max_{MAX_BYTES}")
    # payload format: filename\n + bytes
    payload = (file.filename + "\n").encode("utf-8") + content
    resp = await agent_call("PUT", payload)
    return JSONResponse(content=json_loads_safe(resp))

@app.get("/download/{filename}")
async def download_file(filename: str, x_auth_token: Optional[str] = Header(default=None)):
    require_token(x_auth_token)
    if len(filename) > 255:
        raise HTTPException(status_code=400, detail="bad_filename")
    resp = await agent_call("GET", filename.encode("utf-8"))
    # resp format: first line is json header, then raw bytes after \n\n
    header, sep, body = resp.partition(b"\n\n")
    meta = json_loads_safe(header)
    if not meta.get("ok"):
        raise HTTPException(status_code=404, detail=meta.get("error","not_found"))
    out_name = meta.get("name", filename)

    async def gen():
        yield body

    return StreamingResponse(gen(), media_type="application/octet-stream",
                             headers={"Content-Disposition": f'attachment; filename="{out_name}"'})

def json_loads_safe(b: bytes):
    import json
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {"ok": False, "error": "bad_json"}
