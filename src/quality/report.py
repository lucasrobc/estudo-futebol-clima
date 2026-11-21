""" Relatório de execução e auditoria de qualidade em JSON """

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.config import Settings
from src.quality.checks import AVISO, ERRO, CheckResult

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_PATH = REPO_ROOT / "reports" / "data_quality_report.json"

ALL_TABLES = [
    "bronze_times", "bronze_partidas", "bronze_clima",
    "silver_estadios", "silver_times", "silver_partidas", "silver_arbitros", "silver_clima_diario",
    "gold_partidas_clima", "gold_resumo_clima",
]


class DataQualityError(Exception):
    """ Quando alguma validação de severidade falha """


def count_tables(spark: SparkSession, settings: Settings) -> dict[str, int]:
    return {t: spark.table(f"{settings.catalog}.{settings.schema}.{t}").count() for t in ALL_TABLES}


def build_report(
    results: list[CheckResult], table_counts: dict[str, int], settings: Settings
) -> dict[str, Any]:
    """ Monta o relatório de qualidade """
    errors = [r for r in results if not r.aprovado and r.severidade == ERRO]
    warnings = [r for r in results if not r.aprovado and r.severidade == AVISO]
    return {
        "projeto": "estudo-futebol-clima",
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "temporada": settings.season,
        "status": "REPROVADO" if errors else "APROVADO",
        "resumo": {
            "checks": len(results),
            "aprovados": sum(r.aprovado for r in results),
            "erros": len(errors),
            "avisos": len(warnings),
        },
        "linhas_por_tabela": table_counts,
        "checks": [r.to_dict() for r in results],
    }


def save_report(report: dict[str, Any], path: Path = REPORT_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def raise_if_failed(report: dict[str, Any]) -> None:
    """ Interrompe o pipeline se houver erro """
    if report["status"] != "APROVADO":
        failed = [f"{c['tabela']}.{c['check']}({c['coluna']})" for c in report["checks"]
                  if not c["aprovado"] and c["severidade"] == ERRO]
        raise DataQualityError(f"{len(failed)} validação(ões) reprovada(s): {failed}")