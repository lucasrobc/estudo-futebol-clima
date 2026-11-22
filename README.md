# estudo-futebol-clima

> **O clima influencia o Brasileirão?** Pipeline de dados em Python + SQL no Databricks que extrai as partidas do Campeonato Brasileiro Série A 2026 e o clima diário das cidades dos jogos, cruza as duas fontes e entrega um dataset analítico em Parquet com relatório de qualidade de dados.

Projeto 1 do Desafio Python FT03.

---

## Sumário
1. [Arquitetura](#arquitetura)
2. [Fontes de dados (APIs)](#fontes-de-dados-apis)
3. [Como executar](#como-executar)
4. [Variáveis de ambiente](#variáveis-de-ambiente)
5. [Estrutura do repositório](#estrutura-do-repositório)
6. [Resiliência na extração](#resiliência-na-extração)
7. [Transformações em SQL](#transformações-em-sql)
8. [Data Quality](#data-quality)
9. [Dataset final](#dataset-final)
10. [Resultados](#resultados)
11. [Limitações conhecidas](#limitações-conhecidas)

---

## Arquitetura

Arquitetura Medallion no Unity Catalog (`workspace.fc`):

O pipeline roda como um Job do Databricks definido por código (`databricks.yml`) com duas tarefas encadeadas: `setup_unity_catalog -> pipeline_completo`.

---

## Fontes de dados (APIs)

### 1. football-data.org v4: partidas e times (Futebol)

| Item | Valor |
|---|---|
| Base URL | `https://api.football-data.org/v4` |
| Autenticação | header `X-Auth-Token` (chave gratuita em https://www.football-data.org/client/register) |
| Limite do plano grátis | 10 requisições/minuto. Header `X-RequestCounter-Reset` |
| Endpoints usados | `GET /competitions/BSA/teams?season=2026` - `GET /competitions/BSA/matches?season=2026&matchday=N` |
| Estrutura aninhada | `matches[]` -> `homeTeam`, `awayTeam`, `score.fullTime`, `score.halfTime`, `referees[]` |

### 2. Open-Meteo Historical Weather: clima diário (meio ambiente)

| Item | Valor |
|---|---|
| Base URL | `https://archive-api.open-meteo.com/v1/archive` |
| Autenticação | nenhuma |
| Limite | por IP: 600/min, 5.000/h, 10.000/dia |
| Parâmetros | `latitude`, `longitude`, `start_date`, `end_date`, `daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,wind_speed_10m_max,weather_code`, `timezone=America/Sao_Paulo` |
| Estrutura aninhada | `daily` com arrays paralelos: `time[i]` corresponde a `temperature_2m_max[i]`, `precipitation_sum[i]`... |

### Ligação entre as fontes
`reference/estadios.csv` é uma tabela de referência curada (times -> cidades -> latitude/longitude). Cada partida é ligada à cidade do time mandante e ao clima dessa cidade na data do jogo (horário de Brasília).

---

## Como executar

### Pré-requisitos
- Workspace Databricks com compute serverless (testado no Databricks Free Edition)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) autenticada:
  ```bash
  databricks auth login --host https://<seu-workspace>.cloud.databricks.com
  ```
- Chave da football-data.org guardada num secret scope:
  ```bash
  databricks secrets create-scope fc
  databricks secrets put-secret fc football_data_api_key
  ```

### Opção 1: recomendado
```bash
git clone https://github.com/lucasrobc/estudo-futebol-clima.git
cd estudo-futebol-clima
./run.sh
```
O `run.sh` valida o bundle, publica o código e o Job no workspace e executa o pipeline. No Windows, rode pelo Git Bash.

### Opção 2: interativa no Databricks
1. Crie um Git folder apontando para este repositório.
2. Rode `notebooks/01_setup_unity_catalog` (schema `workspace.fc` e volumes `raw` e `output`).
3. Rode `notebooks/99_run_pipeline` ou os notebooks `02` a `06`, camada por camada.

### Testes
`notebooks/90_testes` roda os testes unitários do cliente HTTP (`pytest`, testes com respostas simuladas, sem gastar cota da API) e uma chamada real às duas APIs.

---

## Variáveis de ambiente

| Variável | Obrigatória | Descrição |
|---|---|---|
| `FOOTBALL_DATA_API_KEY` | sim | Chave da football-data.org. No Databricks vem do secret scope `fc`. Fora do Databricks, copie `.env.example` para `.env` e preencha. |
| `SEASON` | não | Temporada do Brasileirão (`2026`). |
| `DATABRICKS_AUTH_STORAGE` | não | Só para a CLI no Windows: `plaintext` guarda o token OAuth em arquivo quando o cofre do Windows recusa. |

Nenhuma chave é versionada: o `.env` está no `.gitignore`, e o histórico foi verificado.

---

## Estrutura do repositório

```
estudo-futebol-clima/
├── src/
│   ├── config.py              # Settings a partir de variáveis de ambiente
│   ├── http_client.py         # cliente HTTP resiliente
│   ├── raw_storage.py         # grava e lê o JSON bruto no Volume
│   ├── extract/
│   │   ├── football.py        # times + partidas por rodada (incremental)
│   │   └── weather.py         # clima diário por cidade
│   ├── load/
│   │   ├── bronze.py          # JSON bruto -> tabelas Bronze
│   │   └── writer.py          # Gold -> Parquet Snappy particionado + cópia para output/
│   ├── transform/
│   │   ├── silver.py          # executa sql/silver/*.sql + tabela de referência
│   │   └── gold.py            # executa sql/gold/*.sql
│   ├── quality/
│   │   ├── checks.py          # validações genéricas
│   │   ├── rules.py           # regras do projeto
│   │   └── report.py          # relatório JSON + gate quality
│   └── pipeline.py            # orquestrador ponta a ponta
├── sql/
│   ├── silver/                
│   └── gold/                  # partidas x clima e métricas
├── notebooks/                 # 00 a 06 (camadas), 90 (testes), 99 (pipeline completo)
├── reference/estadios.csv     # time -> cidade -> coordenadas
├── tests/test_http_client.py  # testes unitários
├── output/partidas_clima/     # dataset final (.parquet)
├── reports/data_quality_report.json
├── databricks.yml             # Asset Bundle
├── run.sh                     # execução ponta a ponta
├── requirements.txt
└── .env.example
```

---

## Resiliência na extração

| Mecanismo | Como funciona |
|---|---|
| Timeout | toda requisição tem `timeout` (20 s) |
| Retry seletivo | tenta de novo só em erros temporários (429, 500, 502, 503, 504) e falhas de rede; 400, 401, 403 e 404 falham na hora |
| Backoff exponencial + jitter | esperas de ~1 s, 2 s, 4 s, 8 s, mais um valor aleatório |
| Rate limiter | janela deslizante de 10 chamadas/minuto (limite da football-data.org) |
| Retry-After | espera curta (≤ 120 s): aguarda. Espera longa (ex.: cota diária esgotada): `RateLimitExceededError`, sem travar o pipeline |
| Extração incremental | rodadas com todos os jogos encerrados não são baixadas de novo |
| Idempotência | Volume sobrescrito por arquivo, tabelas recriadas |
| Degradação controlada | se a Open-Meteo recusar uma cidade, o arquivo anterior é mantido e o pipeline segue |

Caso real encontrado no desenvolvimento: o serverless do Free Edition compartilha o IP de saída, e a Open-Meteo chegou a responder `429 Daily API request limit exceeded` com `Retry-After: 11800`.

---

## Transformações em SQL

**Desaninhamento:** partidas e times - árbitros dentro das partidas - arrays paralelos do clima.

| # | Transformação obrigatória | Onde | Como |
|---|---|---|---|
| 1 | Padronização de tipos e datas | `sql/silver/silver_partidas.sql`, `silver_clima_diario.sql` | `CAST` em todas as colunas; `utcDate` → `convert_timezone('UTC', 'America/Sao_Paulo', ...)`, `data_partida`, `ano`, `mes` |
| 2 | Tratamento de nulos | `silver_partidas.sql`, `silver_clima_diario.sql` | placar de jogo não disputado continua NULL (não é 0×0) e `resultado = 'A_DEFINIR'`; `COALESCE`/`NULLIF` em textos; temperatura média ausente = (máx + mín)/2 |
| 3 | Deduplicação | todas as tabelas Silver | `QUALIFY ROW_NUMBER() OVER (PARTITION BY chave ORDER BY mais_recente DESC) = 1` |
| 4 | Cruzamento de fontes | `sql/gold/gold_partidas_clima.sql` | partidas estádios (mandante -> cidade), clima (cidade + data). Mantém os jogos sem clima com `clima_disponivel = false` |
| 5 | Enriquecimento de métricas | `gold_partidas_clima.sql`, `gold_resumo_clima.sql` | `total_gols`, `saldo_mandante`, `pontos`, `ambos_marcaram`, `choveu` (≥ 1 mm), `faixa_chuva`, `faixa_temperatura`, `turno`; resumo por dimensão com `stack` (unpivot) |

---

## Data Quality

`src/quality/` executa e grava `reports/data_quality_report.json`:

| Tipo | Exemplos |
|---|---|
| Schema | 18 colunas obrigatórias da Gold com o tipo esperado |
| Null ratio | identificadores = 0%; placar de partida encerrada = 0%; clima de partida encerrada = AVISO |
| Unicidade | `partida_id`; `cidade_id` + `data`; `team_id` |
| Intervalo | gols 0–15, rodada 1–38, data dentro da temporada, temperatura e chuva plausíveis |
| Valores aceitos | `resultado` {MANDANTE, EMPATE, VISITANTE, A_DEFINIR} |
| Contagem | 380 partidas (38 rodadas × 10 jogos) |

- **Severidade:** `ERRO` reprova o pipeline; `AVISO` só registra.
- **Gate quality:** a validação roda antes de gravar o Parquet. Com erro, o pipeline para (`DataQualityError`) e o dataset final anterior fica intacto.
- **Relatório:** status, resumo, linhas por tabela, tempo de cada etapa do pipeline e o detalhe de cada check.

---

## Dataset final

`output/partidas_clima/`: Parquet + Snappy, particionado por `ano=2026/mes=M` (mês da partida no horário de Brasília).

- 380 linhas (1 por partida), 34 colunas: partida, times, placar, clima do dia na cidade do mandante e métricas.
- 11 partições, sem `mes=6`: o Brasileirão parou em junho para a Copa do Mundo.
- Leitura: `spark.read.parquet("output/partidas_clima")` ou `pandas.read_parquet("output/partidas_clima")`.

---

## Resultados

Análise de 21/11/2026, com 350 partidas encerradas e com clima (`gold_resumo_clima`):

| Recorte | Jogos | Gols/jogo | Vitória do mandante |
|---|---|---|---|
| **Todos os jogos** | 350 | **2,65** | 48,9% |
| Seco (< 1 mm) | 160 | 2,68 | 50,6% |
| Chuva fraca (1 a 10 mm) | 125 | 2,61 | 45,6% |
| Chuva moderada (10 a 25 mm) | 51 | 2,65 | 49,0% |
| Chuva forte (≥ 25 mm) | 14 | 2,79 | 57,1% |
| Frio (máx. < 20 °C) | 28 | 2,89 | 35,7% |
| Jogos de dia | 83 | 2,81 | 43,4% |
| Jogos à noite | 267 | 2,60 | 50,6% |

**Conclusões:**
- **Chuva:** sem efeito claro na média de gols (2,61 a 2,79, contra 2,65 no geral). A faixa "chuva forte" tem só 14 jogos, amostra pequena demais para concluir.
- **Frio:** o mandante vence menos, mas os jogos frios são em Curitiba, Porto Alegre e Chapecó, cujos times tiveram aproveitamento baixo. Provável **variável de confusão** (o time/cidade), e não efeito da temperatura.
- **Turno:** jogos de dia tiveram mais gols (2,81 contra 2,60) e mais "ambos marcam" (65% contra 56%). É o achado mais interessante, mas ainda exploratório.

---

## Limitações conhecidas
- O clima é o da cidade do mandante, não o do estádio exato de cada jogo (mando trocado ou campo neutro não são tratados).
- O clima é diário, não na hora do jogo.
- O histórico de clima tem cerca de 2 dias de atraso: os jogos mais recentes ficam sem clima até a próxima execução (registrado como AVISO).
- A football-data.org informa só o árbitro principal e apenas em parte das partidas.
- O IP de saída do serverless é compartilhado e pode esgotar a cota da Open-Meteo. O pipeline segue com o bruto já salvo; se for preciso, a extração do clima pode rodar localmente com o mesmo código.
- Uma única temporada: as conclusões são exploratórias, não causais.

---

## Tecnologias
Python 3.12 - PySpark / Spark SQL - Databricks Free Edition (Unity Catalog, Volumes, Delta Lake, Jobs, Asset Bundles, serverless) - requests - pytest
