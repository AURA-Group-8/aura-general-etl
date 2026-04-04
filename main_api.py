import uvicorn

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
import shutil

from src.api.service import get_latest_template_file_path_service, save_bronze_file_service
from src.config import HOST_API, PORT_API

app = FastAPI()

# Rota básica
@app.get("/")
def read_root():
    return {"message": "Hello World"}

# Rota com query param
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
    uvicorn.run("main_api:app", host=HOST_API, port=int(PORT_API), reload=True)