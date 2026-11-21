""" Extração da football-data.org: times e partidas do Brasileirão """

import logging
from pathlib import Path

from src.config import Settings
from src.http_client import HttpClient, RateLimiter
from src.raw_storage import read_json, write_json

logger = logging.getLogger(__name__)

TOTAL_MATCHDAYS = 38  # pontos corridos: 38 rodadas


def build_client(settings: Settings) -> HttpClient:
    """ Rate limit do plano grátis (10/min) """
    return HttpClient(
        timeout=settings.timeout_seconds,
        max_retries=settings.max_retries,
        max_retry_wait=settings.max_retry_wait_seconds,
        rate_limiter=RateLimiter(settings.football_calls_per_minute),
        headers={"X-Auth-Token": settings.football_api_key},
    )


def football_dir(settings: Settings) -> Path:
    return Path(settings.raw_path) / "football"


def extract_teams(client: HttpClient, settings: Settings) -> Path:
    """ Times da temporada - requisição única """
    url = f"{settings.football_base_url}/competitions/{settings.competition}/teams"
    data = client.get_json(url, params={"season": settings.season})
    path = football_dir(settings) / "teams" / f"season={settings.season}" / "teams.json"
    write_json(path, data)
    logger.info("Times: %d gravados em %s", len(data.get("teams", [])), path)
    return path


def matchday_is_closed(path: Path) -> bool:
    """ Rodada fechada quando todas as partidas estiverem finalizadas """
    if not path.exists():
        return False
    matches = read_json(path).get("matches", [])
    return bool(matches) and all(match.get("status") == "FINISHED" for match in matches)


def extract_matches(client: HttpClient, settings: Settings) -> dict[str, int]:
    """ Requisição rodada a rodada. Rodadas com retorno de todas as partidas finalizadas são puladas """
    url = f"{settings.football_base_url}/competitions/{settings.competition}/matches"
    summary = {"baixadas": 0, "puladas": 0}
    for matchday in range(1, TOTAL_MATCHDAYS + 1):
        path = (
            football_dir(settings) / "matches" / f"season={settings.season}"
            / f"matchday={matchday:02d}" / "matches.json"
        )
        if matchday_is_closed(path):
            summary["puladas"] += 1
            continue
        data = client.get_json(url, params={"season": settings.season, "matchday": matchday})
        write_json(path, data)
        summary["baixadas"] += 1
        logger.info("Rodada %02d: %d partidas", matchday, len(data.get("matches", [])))
    return summary