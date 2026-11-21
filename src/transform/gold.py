""" Camada Gold: Partidas e clima, com as métricas de análise """

from pyspark.sql import SparkSession

from src.config import Settings
from src.transform.silver import REPO_ROOT, run_sql_file

SQL_DIR = REPO_ROOT / "sql" / "gold"

GOLD_TABLES = ["gold_partidas_clima", "gold_resumo_clima"]


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """ Cria as tabelas e devolve a contagem de linhas de cada tabela """
    for table in GOLD_TABLES:
        run_sql_file(spark, SQL_DIR / f"{table}.sql", settings)
    return {t: spark.table(f"{settings.catalog}.{settings.schema}.{t}").count() for t in GOLD_TABLES}