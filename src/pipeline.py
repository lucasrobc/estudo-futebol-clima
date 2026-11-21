""" Pipeline ponta a ponta """

import logging
import time
from collections.abc import Callable
from datetime import date
from typing import Any

from pyspark.sql import SparkSession

from src.config import Settings
from src.extract import football, weather
from src.load.bronze import load_bronze
from src.load.writer import copy_to_repo, write_parquet
from src.quality.report import build_report, count_tables, raise_if_failed, save_report
from src.quality.rules import run_all_checks
from src.raw_storage import read_json
from src.transform.gold import build_gold
from src.transform.silver import STADIUMS_CSV, build_silver

logger = logging.getLogger(__name__)


def _step(name: str, func: Callable[[], Any], timings: dict[str, float]) -> Any:
    """Executa etapas do pipeline e registra no log o tempo """
    logger.info("▶ %s", name)
    start = time.perf_counter()
    result = func()
    timings[name] = round(time.perf_counter() - start, 1)
    logger.info("✔ %s em %.1fs", name, timings[name])
    return result


def extract_all(settings: Settings) -> dict[str, Any]:
    """Extrai times, partidas e clima para o Volume """
    football_client = football.build_client(settings)
    teams = read_json(football.extract_teams(football_client, settings))
    matches = football.extract_matches(football_client, settings)

    start, end = weather.weather_period(
        date.fromisoformat(teams["season"]["startDate"]),
        date.fromisoformat(teams["season"]["endDate"]),
    )
    stadiums = weather.load_stadiums(STADIUMS_CSV)
    climate = weather.extract_weather(weather.build_client(settings), settings, stadiums, start, end)
    return {"partidas": matches, "clima": climate, "periodo_clima": [str(start), str(end)]}


def run_pipeline(spark: SparkSession, settings: Settings) -> dict[str, Any]:
    timings: dict[str, float] = {}
    summary: dict[str, Any] = {}

    summary["extracao"] = _step("Extração (APIs → Volume raw)", lambda: extract_all(settings), timings)
    summary["bronze"] = _step("Bronze", lambda: load_bronze(spark, settings), timings)
    summary["silver"] = _step("Silver", lambda: build_silver(spark, settings), timings)
    summary["gold"] = _step("Gold", lambda: build_gold(spark, settings), timings)

    def quality() -> dict[str, Any]:
        report = build_report(run_all_checks(spark, settings), count_tables(spark, settings), settings)
        report["execucao"] = {"extracao": summary["extracao"], "tempo_por_etapa_s": dict(timings)}
        save_report(report)
        return report

    report = _step("Data Quality", quality, timings)
    summary["qualidade"] = {"status": report["status"], **report["resumo"]}
    raise_if_failed(report)  # Se reportar ERRO, não sobrescreve o arquivo.

    path = _step("Parquet (Snappy, ano/mes)", lambda: write_parquet(spark, settings), timings)
    copied = _step("Cópia para output/", lambda: copy_to_repo(path), timings)
    summary["parquet"] = {"volume": path, "arquivos_em_output": len(copied)}
    summary["tempo_por_etapa_s"] = timings
    return summary