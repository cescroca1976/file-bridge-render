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
    return {"ok": True, "version": "1.2.0-concurrency-fix"}

@app.get("/status")
async def status(x_auth_token: Optional[str] = Header(default=None)):
    require_token(x_auth_token)
    return {"agent_connected": TOKEN in agents}


# token -> Future for the pending request response
response_futures: Dict[str, asyncio.Future] = {}

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
        while True:
            # Centralized reading loop
            message = await ws.receive()
            
            if message["type"] == "websocket.disconnect":
                break
                
            data = None
            if "text" in message:
                text_data = message["text"]
                if text_data == "ping":
                    await ws.send_text("pong")
                    continue
                # If agent sends text, treat as data
                data = text_data.encode("utf-8")
            elif "bytes" in message:
                data = message["bytes"]
            
            # If we have a pending request, deliver the data
            if data is not None:
                fut = response_futures.get(TOKEN)
                if fut and not fut.done():
                    fut.set_result(data)
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WS Error: {e}")
    finally:
        if agents.get(TOKEN) is ws:
            agents.pop(TOKEN, None)
            # Cancel any pending future if disconnected
            fut = response_futures.get(TOKEN)
            if fut and not fut.done():
                fut.cancel()
            response_futures.pop(TOKEN, None)

async def agent_call(command: str, payload: Optional[bytes] = None) -> bytes:
    if TOKEN not in agents:
        raise HTTPException(status_code=503, detail="agent_not_connected")

    ws = agents[TOKEN]
    lock = locks[TOKEN]

    async with lock:
        # Register future for response
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        response_futures[TOKEN] = fut
        
        try:
            await ws.send_text(command)
            if payload is not None:
                await ws.send_bytes(payload)
            
            # Wait for response from the centralized reader
            data = await asyncio.wait_for(fut, timeout=120)
            return data
        except Exception as e:
            raise HTTPException(status_code=504, detail=f"agent_error:{e}")
        finally:
            # Clean up
            if response_futures.get(TOKEN) == fut:
                response_futures.pop(TOKEN, None)


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
