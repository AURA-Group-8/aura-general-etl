from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from src.api.service_template import (
    get_latest_template_file_path_service,
    save_bronze_xlsx_from_df,
    validate_file,
    validate_schema_with_llm,
)

router = APIRouter(
    tags=["Template"],
    prefix="",
)


@router.get("/api/v1/template")
def get_template():
    file_path = get_latest_template_file_path_service()

    return FileResponse(
        path=file_path,
        filename="template.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.post("/api/v1/custos")
def salvar_custos(file: UploadFile = File(...)):
    df = None
    try:
        df = validate_file(file)
        df = validate_schema_with_llm(df)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    message = save_bronze_xlsx_from_df(df=df)
    return {"message": message}
