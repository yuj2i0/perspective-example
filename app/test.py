import asyncio
from contextlib import asynccontextmanager
import logging
import threading
from time import sleep
from typing import Optional

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
from perspective import Server
from perspective.handlers.starlette import PerspectiveStarletteHandler
import uvicorn


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: RUF029
    yield

app = FastAPI(
    title="Hi",
    description="Hi",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def make_perspective_app(perspective_server,perspective_loop, app: FastAPI):
    def perspective_thread():
        perspective_loop.run_forever()

    thread = threading.Thread(target=perspective_thread, daemon=True)
    thread.start()

    async def websocket_handler(websocket: WebSocket):
        handler = PerspectiveStarletteHandler(
            perspective_server=perspective_server, websocket=websocket
        )
        await handler.run()

    app.add_api_websocket_route("/data", websocket_handler)

    # WARNING(jd): This has to be added HERE, for some unknown reason - can't be
    # in the main FastAPI app definition.
    app.mount("/", StaticFiles(directory=".", html=True), name="static")

    return app

def start_server(app: FastAPI, port: Optional[int] = None, threaded: bool = True):
    port_to_use = port
    logging.critical(f"Listening on http://localhost:{port_to_use}")
    if threaded:
        thread = threading.Thread(
            target=uvicorn.run,
            args=(app,),
            kwargs={"host": "0.0.0.0", "port": port_to_use},
            daemon=True,
        )
        thread.start()
        if port is None:
            return port_to_use
        return thread
    uvicorn.run(app, host="0.0.0.0", port=port_to_use)

def main():
    perspective_server = Server()
    perspective_loop = asyncio.new_event_loop()
    perspective_client = perspective_server.new_local_client(
        loop_callback=perspective_loop.call_soon_threadsafe
    )
    Table = perspective_client.table
    test1 = Table({"id": "string", "value":"float"}, index="id", name="temp_table")
    make_perspective_app(perspective_server,perspective_loop, app)
    ## Declare Perspective Table 
    
    thread = start_server(app, 8080)
    
    sleep(10) 
    ## Open localhost/index.html
    df = pd.DataFrame(
        {
            "id": ["Id 1"],
            "value": [float(500)],
        }
    )
    perspective_client.open_table("temp_table").update(df)
    print("Updated")
    thread.join()


if __name__ == "__main__":
    main()
