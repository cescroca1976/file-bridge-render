import os
import asyncio
from typing import Dict, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, Header, Body as settings_Body
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

# Allow any token to define a session (Multi-tenant / Room logic)
# BRIDGE_TOKEN env var is now optional, or checking could be added back for restricting creation 
# But for this use case, we want dynamic channels.

@app.get("/ping")
async def ping():
    return {"ok": True, "version": "1.4.0-multi-channel"}

@app.get("/status")
async def status(x_auth_token: Optional[str] = Header(default=None)):
    if not x_auth_token:
        return {"error": "missing_token"}
    return {"agent_connected": x_auth_token in agents, "session": x_auth_token}


# token -> Future for the pending request response
response_futures: Dict[str, asyncio.Future] = {}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    # Handshake: Client sends their Token (Room ID)
    try:
        token_msg = await asyncio.wait_for(ws.receive_text(), timeout=10)
    except Exception:
        await ws.close(code=1008)
        return

    if not token_msg:
        await ws.close(code=1008)
        return
        
    session_token = token_msg
    agents[session_token] = ws
    locks.setdefault(session_token, asyncio.Lock())
    
    print(f"Agent connected to session: {session_token}")

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
                data = text_data.encode("utf-8")
            elif "bytes" in message:
                data = message["bytes"]
            
            # Deliver data to the specific session's future
            if data is not None:
                fut = response_futures.get(session_token)
                if fut and not fut.done():
                    fut.set_result(data)
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WS Error: {e}")
    finally:
        if agents.get(session_token) is ws:
            agents.pop(session_token, None)
            # Cancel future
            fut = response_futures.get(session_token)
            if fut and not fut.done():
                fut.cancel()
            response_futures.pop(session_token, None)
        print(f"Agent disconnected from session: {session_token}")

async def agent_call(command: str, payload: Optional[bytes] = None, session_token: str = "") -> bytes:
    if not session_token:
        raise HTTPException(status_code=400, detail="missing_session_token")
    if session_token not in agents:
        raise HTTPException(status_code=503, detail="agent_not_connected")

    ws = agents[session_token]
    lock = locks[session_token]

    async with lock:
        # Register future for response
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        response_futures[session_token] = fut
        
        try:
            await ws.send_text(command)
            if payload is not None:
                await ws.send_bytes(payload)
            
            # Wait for response from the centralized reader
            data = await asyncio.wait_for(fut, timeout=120)
            return data
        except asyncio.TimeoutError:
             raise HTTPException(status_code=504, detail="agent_timeout")
        except Exception as e:
            raise HTTPException(status_code=504, detail=f"agent_error:{e}")
        finally:
            # Clean up
            if response_futures.get(session_token) == fut:
                response_futures.pop(session_token, None)


@app.get("/list")
async def list_files(x_auth_token: Optional[str] = Header(default=None)):
    if not x_auth_token: raise HTTPException(status_code=401, detail="missing_token")
    data = await agent_call("LIST", session_token=x_auth_token)
    # data is utf-8 JSON
    return JSONResponse(content=json_loads_safe(data))

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), x_auth_token: Optional[str] = Header(default=None)):
    if not x_auth_token: raise HTTPException(status_code=401, detail="missing_token")
    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=413, detail=f"file_too_large_max_{MAX_BYTES}")
    # payload format: filename\n + bytes
    payload = (file.filename + "\n").encode("utf-8") + content
    resp = await agent_call("PUT", payload, session_token=x_auth_token)
    return JSONResponse(content=json_loads_safe(resp))

@app.get("/download/{filename}")
async def download_file(filename: str, x_auth_token: Optional[str] = Header(default=None)):
    if not x_auth_token: raise HTTPException(status_code=401, detail="missing_token")
    if len(filename) > 255:
        raise HTTPException(status_code=400, detail="bad_filename")
    resp = await agent_call("GET", filename.encode("utf-8"), session_token=x_auth_token)
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

@app.post("/execute")
async def execute_command(
    command: Dict = settings_Body(...), 
    x_auth_token: Optional[str] = Header(default=None)
):
    if not x_auth_token: raise HTTPException(status_code=401, detail="missing_token")
    import json
    # Send "EXECUTE" and the JSON payload string
    # The agent must handle "EXECUTE"
    payload = json.dumps(command).encode("utf-8")
    resp_bytes = await agent_call("EXECUTE", payload, session_token=x_auth_token)
    return JSONResponse(content=json_loads_safe(resp_bytes))

def json_loads_safe(b: bytes):
    import json
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {"ok": False, "error": "bad_json"}
