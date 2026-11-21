""" Extração do clima diário (Open-Meteo) para a cidade de cada time mandante """

import csv
import logging
from datetime import date, timedelta
from pathlib import Path

from src.config import Settings
from src.http_client import HttpClient, RateLimitExceededError
from src.raw_storage import write_json

logger = logging.getLogger(__name__)

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "wind_speed_10m_max",
    "weather_code",
]
ARCHIVE_DELAY_DAYS = 2  # Dia anterior


def build_client(settings: Settings) -> HttpClient:
    return HttpClient(
        timeout=settings.timeout_seconds,
        max_retries=settings.max_retries,
        max_retry_wait=settings.max_retry_wait_seconds,
    )


def load_stadiums(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def weather_period(
    season_start: date, season_end: date, today: date | None = None
) -> tuple[date, date]:
    today = today or date.today()
    return season_start, min(season_end, today - timedelta(days=ARCHIVE_DELAY_DAYS))


def extract_weather(
    client: HttpClient,
    settings: Settings,
    stadiums: list[dict[str, str]],
    start: date,
    end: date,
) -> dict[str, list[str]]:
    """ Uma requisição por cidade. Se a API recusar, mantém o arquivo anterior e segue para próxima cidade """
    cities = {row["cidade_id"]: row for row in stadiums}
    summary: dict[str, list[str]] = {"baixadas": [], "recusadas": []}
    for cidade_id, row in cities.items():
        path = (
            Path(settings.raw_path) / "weather" / f"season={settings.season}"
            / f"cidade_id={cidade_id}" / "weather.json"
        )
        params = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": "America/Sao_Paulo",
        }
        try:
            data = client.get_json(settings.weather_base_url, params=params)
        except RateLimitExceededError as error:
            logger.warning(
                "Clima de %s não atualizado (%s). Arquivo anterior existe? %s",
                cidade_id, error, path.exists(),
            )
            summary["recusadas"].append(cidade_id)
            continue
        write_json(path, data)
        summary["baixadas"].append(cidade_id)
        logger.info("Clima de %s: %d dias", cidade_id, len(data["daily"]["time"]))
    return summary