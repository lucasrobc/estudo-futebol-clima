""" Validações padrão de qualidade de dados: schema, null ratio, unicidade, intervalo e valores aceitos """

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ERRO = "ERRO"    # Reprova o pipeline
AVISO = "AVISO"  # Entra no relatório, mas não reprova o pipeline


@dataclass
class CheckResult:
    """ Registro da validação """

    tabela: str
    check: str
    aprovado: bool
    severidade: str = ERRO
    coluna: str | None = None
    detalhes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_schema(df: DataFrame, table: str, expected: dict[str, str]) -> CheckResult:
    """ Confere as colunas e tipo """
    actual = dict(df.dtypes)
    missing = sorted(set(expected) - set(actual))
    wrong_type = {
        column: {"esperado": dtype, "encontrado": actual[column]}
        for column, dtype in expected.items()
        if column in actual and actual[column] != dtype
    }
    return CheckResult(
        table, "schema", not missing and not wrong_type,
        detalhes={"colunas_verificadas": len(expected), "faltando": missing, "tipo_diferente": wrong_type},
    )


def check_null_ratio(
    df: DataFrame,
    table: str,
    columns: list[str],
    max_ratio: float = 0.0,
    severity: str = ERRO,
    where: str | None = None,
) -> list[CheckResult]:
    """ Proporção de nulos de várias colunas """
    data = df.filter(where) if where else df
    row = data.select(
        F.count(F.lit(1)).alias("_total"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns],
    ).first()
    total = row["_total"]
    results = []
    for column in columns:
        ratio = (row[column] or 0) / total if total else 0.0
        results.append(CheckResult(
            table, "null_ratio", ratio <= max_ratio, severity, column,
            {"null_ratio": round(ratio, 4), "limite": max_ratio, "linhas": total, "filtro": where},
        ))
    return results


def check_unique(df: DataFrame, table: str, keys: list[str]) -> CheckResult:
    """ Garantir que colunas-chave não se repita """
    duplicated = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return CheckResult(
        table, "unicidade", duplicated == 0, coluna=", ".join(keys),
        detalhes={"chaves_duplicadas": duplicated},
    )


def check_range(
    df: DataFrame, table: str, column: str, min_value: Any, max_value: Any, severity: str = ERRO
) -> CheckResult:
    """ Conta valores fora do mínimo e máximo """
    out_of_range = F.col(column).isNotNull() & ~F.col(column).between(min_value, max_value)
    row = df.select(
        F.min(column).alias("minimo"),
        F.max(column).alias("maximo"),
        F.sum(out_of_range.cast("int")).alias("fora"),
    ).first()
    outside = row["fora"] or 0
    return CheckResult(
        table, "intervalo", outside == 0, severity, column,
        {
            "permitido": [str(min_value), str(max_value)],
            "encontrado": [str(row["minimo"]), str(row["maximo"])],
            "fora_do_intervalo": outside,
        },
    )


def check_accepted_values(df: DataFrame, table: str, column: str, accepted: list[str]) -> CheckResult:
    invalid = (
        df.filter(~F.col(column).isin(accepted) | F.col(column).isNull())
        .select(column).distinct().limit(10).collect()
    )
    found = [r[column] for r in invalid]
    return CheckResult(
        table, "valores_aceitos", not found, coluna=column,
        detalhes={"aceitos": accepted, "invalidos": found},
    )


def check_row_count(df: DataFrame, table: str, expected: int) -> CheckResult:
    found = df.count()
    return CheckResult(
        table, "contagem_linhas", found == expected,
        detalhes={"esperado": expected, "encontrado": found},
    )