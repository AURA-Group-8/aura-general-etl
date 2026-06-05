import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes.dashboard import router as dashboard_router
from src.api.routes.template import router as template_router
from src.config import HOST_API, PORT_API

app = FastAPI()

print(f"API will run on http://{HOST_API}:{PORT_API}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router)
app.include_router(template_router)


@app.options("/{rest_of_path:path}")
def preflight_handler():
    return {}


@app.get("/")
def read_root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    uvicorn.run("main_api:app", host=HOST_API, port=int(PORT_API))
