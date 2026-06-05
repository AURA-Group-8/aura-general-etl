from src.infraestructure.databases import execute_warehouse_sql
import pandas as pd


def get_agendamentos_semanais():
    # Conta agendamentos por dia da semana no último mês (mês completo anterior)
    query = """
    SELECT
        DAYOFWEEK(start_datetime) num_dia_semana,
        CASE DAYOFWEEK(start_datetime)
            WHEN 1 THEN 'Domingo'
            WHEN 2 THEN 'Segunda'
            WHEN 3 THEN 'Terca'
            WHEN 4 THEN 'Quarta'
            WHEN 5 THEN 'Quinta'
            WHEN 6 THEN 'Sexta'
            WHEN 7 THEN 'Sabado'
        END AS dia_semana,
        COUNT(DISTINCT schedule_id) AS agendamentos
    FROM dados_aplicacao
    WHERE start_datetime >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')
      AND start_datetime < DATE_FORMAT(CURDATE(), '%Y-%m-01')
    GROUP BY num_dia_semana, dia_semana
    """

    resultado = execute_warehouse_sql(query)

    # Garantir que todas as chaves de dias da semana existam (em português, lowercase)
    dias = ["domingo", "segunda", "terca", "quarta", "quinta", "sexta", "sabado"]
    resultado_formatado = {d: 0 for d in dias}

    for row in resultado:
        chave = row.dia_semana.lower()
        resultado_formatado[chave] = int(row.agendamentos or 0)

    return resultado_formatado


def get_resumo():
    hoje = pd.Timestamp.today()
    mes_passado = hoje - pd.DateOffset(months=1)

    tkm_e_faturamento_por_mes = get_tkm_e_faturamento_por_mes(hoje)
    custos_por_mes = get_custos_por_mes(hoje)
    tkm_e_faturamento_por_mes_passado = get_tkm_e_faturamento_por_mes(mes_passado, hoje)
    custos_por_mes_passado = get_custos_por_mes(mes_passado, hoje)

    porcentagem_crescimento_tkm = calcular_porcentagem_crescimento(
        tkm_e_faturamento_por_mes["tkm"], tkm_e_faturamento_por_mes_passado["tkm"]
    )
    porcentagem_crescimento_receita = calcular_porcentagem_crescimento(
        tkm_e_faturamento_por_mes["total_faturado"],
        tkm_e_faturamento_por_mes_passado["total_faturado"],
    )
    porcentagem_crescimento_custos = calcular_porcentagem_crescimento(
        custos_por_mes["total_custos"], custos_por_mes_passado["total_custos"]
    )

    resumo = {
        "tkm": tkm_e_faturamento_por_mes["tkm"],
        "porcentagem_crescimento_tkm": porcentagem_crescimento_tkm,
        "receita": tkm_e_faturamento_por_mes["total_faturado"],
        "porcentagem_crescimento_receita": porcentagem_crescimento_receita,
        "custos": custos_por_mes["total_custos"],
        "porcentagem_crescimento_custos": porcentagem_crescimento_custos,
    }
    return resumo


def calcular_porcentagem_crescimento(valor_atual, valor_passado):
    if valor_passado == 0 or valor_atual == 0:
        return 0
    return ((valor_atual - valor_passado) / valor_passado) * 100


def get_tkm_e_faturamento_por_mes(
    mes_referencia: pd.Timestamp, data_limite: pd.Timestamp = None
):
    month_date_str = mes_referencia.strftime("%Y-%m-01")

    query = f"""
    SELECT
        SUM(total_price) AS total_faturado,
        SUM(total_price) / NULLIF(COUNT(DISTINCT schedule_id), 0) AS tkm
    FROM dados_aplicacao
    WHERE DATE_FORMAT(start_datetime, '%Y-%m-01') = '{month_date_str}'
    """

    if data_limite:
        data_limite_str = data_limite.strftime("%Y-%m-%d")
        query += f" AND start_datetime <= '{data_limite_str}'"

    resultado = execute_warehouse_sql(query)

    if len(resultado) == 0:
        return {"total_faturado": 0, "tkm": 0}

    row = resultado[0]

    return {"total_faturado": row.total_faturado or 0, "tkm": row.tkm or 0}


def get_custos_por_mes(mes_referencia: pd.Timestamp, data_limite: pd.Timestamp = None):
    month_date_str = mes_referencia.strftime("%Y-%m-01")

    query = f"""
    SELECT
        SUM(valor_total) AS total_custos
    FROM costs
    WHERE DATE_FORMAT(data_compra, '%Y-%m-01') = '{month_date_str}'
    """

    if data_limite:
        data_limite_str = data_limite.strftime("%Y-%m-%d")
        query += f" AND data_compra <= '{data_limite_str}'"

    resultado = execute_warehouse_sql(query)

    if len(resultado) == 0:
        return {"total_custos": 0}

    row = resultado[0]

    return {"total_custos": row.total_custos or 0}


def get_faturamento(periodo_meses: int = None):
    # Retorna faturamento por mês (primeiro dia do mês) e valor faturado
    # periodo_meses: opcional, se não informado retorna últimos 12 meses
    if periodo_meses is None:
        periodo_meses = 12

    query = f"""
    SELECT
        DATE_FORMAT(start_datetime, '%Y-%m-01') AS mes,
        SUM(total_price) AS valor
    FROM dados_aplicacao
    WHERE DATE_FORMAT(start_datetime, '%Y-%m-01') is not null
    GROUP BY mes
    ORDER BY mes DESC
    LIMIT {int(periodo_meses)}
    """

    resultado = execute_warehouse_sql(query)

    if not resultado:
        return []

    # resultado vem em ordem decrescente por mes; inverter para ordem cronológica
    lista = [
        {
            "mes": row.mes.strftime("%Y-%m-01")
            if hasattr(row.mes, "strftime")
            else str(row.mes),
            "valor": float(row.valor or 0),
        }
        for row in reversed(resultado)
    ]

    return lista


def get_top_servicos(periodo_meses: int = None):
    # Retorna top 3 serviços mais executados nos últimos x meses
    # periodo_meses: opcional, se não informado retorna últimos 3 meses
    if periodo_meses is None:
        periodo_meses = 3

    # Primeiro, obter o total de execuções no período
    query_total = f"""
    SELECT COUNT(DISTINCT schedule_id) AS total_execucoes
    FROM dados_aplicacao
    WHERE start_datetime >= DATE_SUB(CURDATE(), INTERVAL {int(periodo_meses)} MONTH)
    AND name IS NOT NULL
    """

    resultado_total = execute_warehouse_sql(query_total)
    total_execucoes = (
        resultado_total[0].total_execucoes
        if resultado_total and resultado_total[0].total_execucoes
        else 0
    )

    if total_execucoes == 0:
        return []

    # Obter top 3 serviços
    query = f"""
    SELECT
        name AS nome_servico,
        COUNT(DISTINCT schedule_id) AS qtd_realizados
    FROM dados_aplicacao
    WHERE start_datetime >= DATE_SUB(CURDATE(), INTERVAL {int(periodo_meses)} MONTH)
    AND name IS NOT NULL
    GROUP BY name
    ORDER BY qtd_realizados DESC
    LIMIT 3
    """

    resultado = execute_warehouse_sql(query)

    if not resultado:
        return []

    # Calcular porcentagem e formatar resposta
    lista = [
        {
            "nome_servico": row.nome_servico,
            "qtd_realizados": int(row.qtd_realizados or 0),
            "porcentagem_total": round(
                (float(row.qtd_realizados or 0) / total_execucoes) * 100, 2
            ),
        }
        for row in resultado
    ]

    return lista


def get_clientes_inativos():
    # Retorna clientes que fizeram seu último agendamento há mais de 3 meses
    query = """
    SELECT
        username AS nome_cliente,
        MAX(start_datetime) AS ultimo_agendamento,
        DATEDIFF(CURDATE(), MAX(start_datetime)) / 30 AS qtd_meses_ultimo_agendamento
    FROM dados_aplicacao
    WHERE username IS NOT NULL
    GROUP BY username
    HAVING DATEDIFF(CURDATE(), MAX(start_datetime)) > 90
    ORDER BY qtd_meses_ultimo_agendamento DESC
    """

    resultado = execute_warehouse_sql(query)

    if not resultado:
        return []

    lista = [
        {
            "nome_cliente": row.nome_cliente,
            "ultimo_agendamento": row.ultimo_agendamento.strftime("%Y-%m-%d")
            if hasattr(row.ultimo_agendamento, "strftime")
            else str(row.ultimo_agendamento),
            "qtd_meses_ultimo_agendamento": int(row.qtd_meses_ultimo_agendamento or 0),
        }
        for row in resultado
    ]

    return lista
