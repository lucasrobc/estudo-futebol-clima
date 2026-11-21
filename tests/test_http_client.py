""" Testes do cliente HTTP """

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.http_client import HttpClient, RateLimiter, RateLimitExceededError


def fake_response(status: int, body=None, headers: dict | None = None) -> MagicMock:
    """ Cria uma resposta HTTP falsa com o status """
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    response.ok = status < 400
    response.headers = headers or {}
    response.json.return_value = body
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
    return response


@patch("src.http_client.time.sleep")
def test_sucesso_na_primeira_tentativa(mock_sleep):
    client = HttpClient()
    client.session.get = MagicMock(return_value=fake_response(200, {"ok": True}))

    assert client.get_json("https://api.teste") == {"ok": True}
    assert client.session.get.call_count == 1
    mock_sleep.assert_not_called()


@patch("src.http_client.time.sleep")
def test_tenta_de_novo_em_503_e_depois_consegue(mock_sleep):
    client = HttpClient(max_retries=3)
    client.session.get = MagicMock(side_effect=[fake_response(503), fake_response(200, [1, 2])])

    assert client.get_json("https://api.teste") == [1, 2]
    assert client.session.get.call_count == 2
    mock_sleep.assert_called_once() # esperou o backoff 1x


@patch("src.http_client.time.sleep")
def test_nao_tenta_de_novo_em_403(mock_sleep):
    client = HttpClient(max_retries=3)
    client.session.get = MagicMock(return_value=fake_response(403))

    with pytest.raises(requests.HTTPError):
        client.get_json("https://api.teste")
    assert client.session.get.call_count == 1  # 403: problema que precisa de atenção
    mock_sleep.assert_not_called()


@patch("src.http_client.time.sleep")
def test_desiste_depois_do_maximo_de_tentativas(mock_sleep):
    client = HttpClient(max_retries=2)
    client.session.get = MagicMock(return_value=fake_response(500))

    with pytest.raises(requests.HTTPError):
        client.get_json("https://api.teste")
    assert client.session.get.call_count == 3  # 1ª tentativa
    assert mock_sleep.call_count == 2


@patch("src.http_client.time.sleep")
def test_respeita_retry_after_curto(mock_sleep):
    client = HttpClient()
    client.session.get = MagicMock(
        side_effect=[fake_response(429, headers={"Retry-After": "5"}), fake_response(200, {})]
    )

    client.get_json("https://api.teste")
    mock_sleep.assert_called_once_with(5.0)  # Tempo que a API pediu para aguardar


@patch("src.http_client.time.sleep")
def test_retry_after_longo_vira_excecao_sem_esperar(mock_sleep):
    client = HttpClient(max_retry_wait=120)
    client.session.get = MagicMock(
        return_value=fake_response(429, headers={"Retry-After": "11800"})
    )

    with pytest.raises(RateLimitExceededError) as erro:
        client.get_json("https://api.teste")
    assert erro.value.wait_seconds == 11800
    mock_sleep.assert_not_called()


@patch("src.http_client.time.sleep")
def test_tenta_de_novo_em_falha_de_rede(mock_sleep):
    client = HttpClient(max_retries=2)
    client.session.get = MagicMock(
        side_effect=[requests.ConnectionError("rede caiu"), fake_response(200, {"ok": True})]
    )

    assert client.get_json("https://api.teste") == {"ok": True}
    mock_sleep.assert_called_once()


def test_rate_limiter_espera_quando_a_janela_esta_cheia():
    limiter = RateLimiter(max_calls=2, period=60)
    # Relógio falso: chamadas nos segundos 0, 1 e 2; depois da espera, o relógio marca 60.5
    with patch("src.http_client.time.monotonic", side_effect=[0, 1, 2, 60.5]), patch(
        "src.http_client.time.sleep"
    ) as mock_sleep:
        limiter.wait()  # 1ª chamada: livre
        limiter.wait()  # 2ª chamada: livre
        limiter.wait()  # 3ª chamada: janela cheia -> espera a 1ª sair

    mock_sleep.assert_called_once_with(58)