"""
 HTTP resiliente:
    - timeout em toda requisição
    - rate limiting local
    - retry com backoff exponencial + jitter para falhas temporárias
    - respeita o tempo de espera pedido pela API: curto -> aguarda; longo -> RateLimitExceededError
"""

import logging
import random
import time
from collections import deque
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Erros temporários: tentar de novo
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
# Headers que dizem quantos segundos esperar
WAIT_HEADERS = ("Retry-After", "X-RequestCounter-Reset")


class RateLimitExceededError(Exception):
    """ Cota diária esgotada """

    def __init__(self, url: str, wait_seconds: float) -> None:
        super().__init__(f"Limite da API em {url}: pediu {wait_seconds:.0f}s de espera")
        self.url = url
        self.wait_seconds = wait_seconds


class RateLimiter:
    """ Monitorar no máximo de chamadas """

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()  # Últimas chamadas

    def wait(self) -> None:
        """ Aguardar bloqueio de chamada """
        while True:
            now = time.monotonic()
            # limpa da janela as chamadas que já saíram
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return
            sleep_for = self.period - (now - self._calls[0])
            logger.info("Rate limit local: aguardando %.1fs", sleep_for)
            time.sleep(sleep_for)


def _wait_from_headers(response: requests.Response) -> float | None:
    """ Identificar tempo de espera """
    for header in WAIT_HEADERS:
        value = response.headers.get(header)
        if value is not None:
            try:
                return float(value)
            except ValueError:
                return None
    return None


class HttpClient:
    """ Cria GETs que devolvem JSON, com timeout, retry, backoff e rate limiting """

    def __init__(
        self,
        timeout: float = 20.0,
        max_retries: int = 4,
        max_retry_wait: float = 120.0,
        rate_limiter: RateLimiter | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_retry_wait = max_retry_wait
        self.rate_limiter = rate_limiter
        self.session = requests.Session()  # reaproveita a conexão entre chamadas
        if headers:
            self.session.headers.update(headers)

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """ GET com retry. Devolve o JSON ou levanta exceção com a causa """
        for attempt in range(1, self.max_retries + 2):
            if self.rate_limiter:
                self.rate_limiter.wait()

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as error:
                if attempt > self.max_retries:
                    raise
                wait = self._backoff(attempt)
                logger.warning(
                    "%s em %s (tentativa %d). Nova em %.1fs",
                    type(error).__name__, url, attempt, wait,
                )
                time.sleep(wait)
                continue

            if response.ok:
                return response.json()

            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()  # 400/401/403/404: Erros que precisam de atenção

            # Tempo de espera confiável nos headers
            wait_header = _wait_from_headers(response) if response.status_code == 429 else None
            if wait_header is not None and wait_header > self.max_retry_wait:
                raise RateLimitExceededError(url, wait_header)
            if attempt > self.max_retries:
                response.raise_for_status()

            wait = wait_header if wait_header is not None else self._backoff(attempt)
            logger.warning(
                "HTTP %d em %s (tentativa %d). Nova em %.1fs",
                response.status_code, url, attempt, wait,
            )
            time.sleep(wait)

        raise RuntimeError("Fim inesperado do loop de tentativas")

    @staticmethod
    def _backoff(attempt: int) -> float:
        """ Backoff exponencial com jitter """
        return 2 ** (attempt - 1) + random.uniform(0, 1)