""" Regras de qualidade do projeto """

from datetime import date

from pyspark.sql import SparkSession

from src.config import Settings
from src.quality import checks
from src.quality.checks import AVISO, CheckResult

# Colunas obrigatórias da tabela final e o tipo esperado de cada uma
GOLD_SCHEMA = {
    "partida_id": "bigint",
    "temporada": "int",
    "rodada": "int",
    "encerrada": "boolean",
    "resultado": "string",
    "data_partida": "date",
    "ano": "int",
    "mes": "int",
    "mandante_id": "int",
    "visitante_id": "int",
    "gols_mandante": "int",
    "gols_visitante": "int",
    "cidade_id": "string",
    "temp_max_c": "double",
    "precipitacao_mm": "double",
    "clima_disponivel": "boolean",
    "total_gols": "int",
    "faixa_chuva": "string",
}
RESULTADOS = ["MANDANTE", "EMPATE", "VISITANTE", "A_DEFINIR"]
PARTIDAS_NA_TEMPORADA = 380  # Nº de partidas máxima por temporada no Brasileirão


def run_all_checks(spark: SparkSession, settings: Settings) -> list[CheckResult]:
    """ Executa todas as validações """

    def table(name: str):
        return spark.table(f"{settings.catalog}.{settings.schema}.{name}")

    gold, gold_name = table("gold_partidas_clima"), "gold_partidas_clima"
    clima, clima_name = table("silver_clima_diario"), "silver_clima_diario"
    inicio, fim = date(settings.season, 1, 1), date(settings.season, 12, 31)

    return [
        # Schema e volume
        checks.check_schema(gold, gold_name, GOLD_SCHEMA),
        checks.check_row_count(gold, gold_name, PARTIDAS_NA_TEMPORADA),
        # Unicidade das chaves
        checks.check_unique(gold, gold_name, ["partida_id"]),
        checks.check_unique(clima, clima_name, ["cidade_id", "data"]),
        checks.check_unique(table("silver_estadios"), "silver_estadios", ["team_id"]),
        # Nulos
        *checks.check_null_ratio(
            gold, gold_name, ["partida_id", "data_partida", "mandante_id", "visitante_id", "cidade_id"]
        ),
        # Partida encerrada
        *checks.check_null_ratio(gold, gold_name, ["gols_mandante", "gols_visitante"], where="encerrada"),
        # Clima de partida encerrada (d-2)
        *checks.check_null_ratio(
            gold, gold_name, ["temp_max_c", "precipitacao_mm"], severity=AVISO, where="encerrada"
        ),
        # Intervalos plausíveis
        checks.check_range(gold, gold_name, "gols_mandante", 0, 15),
        checks.check_range(gold, gold_name, "gols_visitante", 0, 15),
        checks.check_range(gold, gold_name, "rodada", 1, 38),
        checks.check_range(gold, gold_name, "data_partida", inicio, fim),
        checks.check_range(clima, clima_name, "temp_max_c", -10, 50),
        checks.check_range(clima, clima_name, "temp_min_c", -15, 45),
        checks.check_range(clima, clima_name, "precipitacao_mm", 0, 500),

        checks.check_accepted_values(gold, gold_name, "resultado", RESULTADOS),
    ]