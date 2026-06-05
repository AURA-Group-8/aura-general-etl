from fastapi import APIRouter, status
from fastapi.responses import JSONResponse, Response
import src.api.service_dashboard as dashboard_service


router = APIRouter(
    tags=["Dashboard"],
    prefix="",
)


@router.get("/api/v1/agendamentos_semanais")
def agendamentos_semanais():
    data = dashboard_service.get_agendamentos_semanais()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@router.get("/api/v1/resumo")
def resumo():
    data = dashboard_service.get_resumo()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@router.get("/api/v1/faturamento")
def faturamento(periodo_meses: int = None):
    data = dashboard_service.get_faturamento(periodo_meses=periodo_meses)
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@router.get("/api/v1/top_servicos")
def top_servicos(periodo_meses: int = None):
    data = dashboard_service.get_top_servicos(periodo_meses=periodo_meses)
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)


@router.get("/api/v1/clientes_inativos")
def clientes_inativos():
    data = dashboard_service.get_clientes_inativos()
    if not data:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return JSONResponse(content=data)
