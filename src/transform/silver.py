""" Executa as queries de sql/silver/ """

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.config import Settings
from src.extract.weather import load_stadiums

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "sql" / "silver"
STADIUMS_CSV = REPO_ROOT / "reference" / "estadios.csv"

SILVER_TABLES = ["silver_times", "silver_partidas", "silver_arbitros", "silver_clima_diario"]

STADIUMS_SCHEMA = StructType([
    StructField("team_id", IntegerType(), nullable=False),
    StructField("time", StringType()),
    StructField("estadio", StringType()),
    StructField("cidade_id", StringType(), nullable=False),
    StructField("cidade", StringType()),
    StructField("uf", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])


def run_sql_file(spark: SparkSession, path: Path, settings: Settings) -> None:
    """ Lê um arquivo .sql, troca {catalog} e {schema} e executa no Spark """

    query = path.read_text(encoding="utf-8").format(catalog=settings.catalog, schema=settings.schema)
    spark.sql(query)


def load_stadiums_table(spark: SparkSession, settings: Settings) -> None:
    """ Grava a tabela de referência CSV do repositório como silver_estadios, já tipada """

    rows = [
        (
            int(r["team_id"]), r["time"], r["estadio"], r["cidade_id"], r["cidade"], r["uf"],
            float(r["latitude"]), float(r["longitude"]),
        )
        for r in load_stadiums(STADIUMS_CSV)
    ]
    table = f"{settings.catalog}.{settings.schema}.silver_estadios"
    spark.createDataFrame(rows, STADIUMS_SCHEMA).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table)


def build_silver(spark: SparkSession, settings: Settings) -> dict[str, int]:
    load_stadiums_table(spark, settings)
    tables = ["silver_estadios"]
    for table in SILVER_TABLES:
        run_sql_file(spark, SQL_DIR / f"{table}.sql", settings)
        tables.append(table)
    return {t: spark.table(f"{settings.catalog}.{settings.schema}.{t}").count() for t in tables}