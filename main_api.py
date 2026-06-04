import uvicorn

from fastapi import FastAPI, File, UploadFile, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware

from src.api.service_template import (
    get_latest_template_file_path_service,
    save_bronze_file_service
)
import src.api.service_dashboard as dashboard_service

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

@app.get("/api/v1/agendamentos_semanais")
def agendamentos_semanais():
    data = dashboard_service.get_agendamentos_semanais()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)

@app.get("/api/v1/resumo")
def resumo():
    data = dashboard_service.get_resumo()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@app.get("/api/v1/faturamento")
def faturamento(periodo_meses: int = None):
    data = dashboard_service.get_faturamento(periodo_meses=periodo_meses)
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@app.get("/api/v1/top_servicos")
def top_servicos(periodo_meses: int = None):
    data = dashboard_service.get_top_servicos(periodo_meses=periodo_meses)
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@app.get("/api/v1/clientes_inativos")
def clientes_inativos():
    data = dashboard_service.get_clientes_inativos()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)

if __name__ == "__main__":
    uvicorn.run(
        "main_api:app",
        host=HOST_API,
        port=int(PORT_API)
    )
