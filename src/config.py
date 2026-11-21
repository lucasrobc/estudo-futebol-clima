import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    """ Parâmetros do pipeline """
    
    football_api_key: str = field(repr=False)  # Segurança para chave
    football_base_url: str = "https://api.football-data.org/v4"
    weather_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    competition: str = "BSA"
    season: int = 2026
    football_calls_per_minute: int = 10
    timeout_seconds: float = 20.0
    max_retries: int = 4
    max_retry_wait_seconds: float = 120.0
    catalog: str = "workspace"
    schema: str = "fc"

    @property
    def raw_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/raw"


def load_settings() -> Settings:
    api_key = os.getenv("FOOTBALL_DATA_API_KEY")
    if not api_key:
        raise RuntimeError(
            "FOOTBALL_DATA_API_KEY não definida. No Databricks, leia o secret scope 'fc'; "
            "Local, copie .env.example para .env e preencha."
        )
    return Settings(
        football_api_key=api_key,
        season=int(os.getenv("SEASON", "2026")),
    )