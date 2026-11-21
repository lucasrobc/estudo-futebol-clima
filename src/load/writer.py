""" Parquet (Snappy) particionado por ano/mês e copia para output/ """

import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_OUTPUT_DIR = REPO_ROOT / "output" / "partidas_clima"
FINAL_TABLE = "gold_partidas_clima"
PARTITION_COLUMNS = ["ano", "mes"]


def output_path(settings: Settings) -> str:
    return f"/Volumes/{settings.catalog}/{settings.schema}/output/partidas_clima"


def write_parquet(spark: SparkSession, settings: Settings) -> str:
    """ Grava a tabela em Parquet no Volume, particionada por ano e mês """
    path = output_path(settings)
    (
        spark.table(f"{settings.catalog}.{settings.schema}.{FINAL_TABLE}")
        .repartition(*PARTITION_COLUMNS)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(path)
    )
    return path


def copy_to_repo(source: str, target: Path = REPO_OUTPUT_DIR) -> list[Path]:
    if target.exists():
        shutil.rmtree(target)
    copied = []
    for file in sorted(Path(source).rglob("*.parquet")):
        destination = target / file.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, destination)
        copied.append(destination)
    return copied