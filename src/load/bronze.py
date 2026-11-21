""" Camada Bronze """

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Settings

BRONZE_SOURCES = {
    # O teams.json já tem um campo "season"
    "bronze_times": "football/teams/season={season}/teams.json",
    "bronze_partidas": "football/matches",
    "bronze_clima": "weather",
}


def read_raw_json(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("multiLine", True).json(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def load_bronze(spark: SparkSession, settings: Settings) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, relative_path in BRONZE_SOURCES.items():
        path = f"{settings.raw_path}/{relative_path.format(season=settings.season)}"
        full_name = f"{settings.catalog}.{settings.schema}.{table}"
        df = read_raw_json(spark, path)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
        counts[table] = spark.table(full_name).count()
    return counts