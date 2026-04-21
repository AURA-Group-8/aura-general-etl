import uvicorn

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from src.api.service import (
    get_latest_template_file_path_service,
    save_bronze_file_service
)
from src.config import HOST_API, PORT_API

app = FastAPI()

print(f"API will run on http://{HOST_API}:{PORT_API}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8081",
        "http://127.0.0.1:8081",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.options("/{rest_of_path:path}")
def preflight_handler():
    return {}

@app.options("/api/v1/template")
def options_template():
    return {}

@app.get("/")
def read_root():
    return {"message": "Hello World"}

@app.get("/api/v1/template")
def get_template():
    file_path = get_latest_template_file_path_service()

    return FileResponse(
        path=file_path,
        filename="template.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.post("/api/v1/custos")
def salvar_custos(file: UploadFile = File(...)):
    message = save_bronze_file_service(file=file)
    return {"message": message}

if __name__ == "__main__":
    uvicorn.run(
        "main_api:app",
        host=HOST_API,   # ex: "0.0.0.0" se quiser acessar de outro dispositivo
        port=int(PORT_API)
    )