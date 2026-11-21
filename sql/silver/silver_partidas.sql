-- Partidas do Brasileirão
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_partidas AS
WITH partidas AS (
  SELECT season, explode(matches) AS m, _ingested_at
  FROM {catalog}.{schema}.bronze_partidas
),
tipadas AS (
  SELECT
    CAST(m.id AS BIGINT)                                  AS partida_id,
    CAST(season AS INT)                                   AS temporada,
    CAST(m.matchday AS INT)                               AS rodada,
    m.status                                              AS status_api,
    COALESCE(m.status = 'FINISHED', FALSE)                AS encerrada,
    CAST(m.homeTeam.id AS INT)                            AS mandante_id,
    COALESCE(TRIM(m.homeTeam.name), 'Não informado')      AS mandante,
    CAST(m.awayTeam.id AS INT)                            AS visitante_id,
    COALESCE(TRIM(m.awayTeam.name), 'Não informado')      AS visitante,
    CAST(m.score.fullTime.home AS INT)                    AS gols_mandante,
    CAST(m.score.fullTime.away AS INT)                    AS gols_visitante,
    CAST(m.score.halfTime.home AS INT)                    AS gols_mandante_1t,
    CAST(m.score.halfTime.away AS INT)                    AS gols_visitante_1t,
    CASE m.score.winner
      WHEN 'HOME_TEAM' THEN 'MANDANTE'
      WHEN 'AWAY_TEAM' THEN 'VISITANTE'
      WHEN 'DRAW'      THEN 'EMPATE'
      ELSE 'A_DEFINIR'
    END                                                   AS resultado,
    to_timestamp_ntz(replace(replace(m.utcDate, 'T', ' '), 'Z', ''))      AS data_hora_utc,
    to_timestamp_ntz(replace(replace(m.lastUpdated, 'T', ' '), 'Z', ''))  AS atualizado_em_utc,
    _ingested_at
  FROM partidas
),
com_datas AS (
  SELECT
    *,
    convert_timezone('UTC', 'America/Sao_Paulo', data_hora_utc)  AS data_hora_brasilia
  FROM tipadas
)
SELECT
  partida_id, temporada, rodada, status_api, encerrada,
  mandante_id, mandante, visitante_id, visitante,
  gols_mandante, gols_visitante, gols_mandante_1t, gols_visitante_1t, resultado,
  data_hora_utc,
  data_hora_brasilia,
  CAST(data_hora_brasilia AS DATE)  AS data_partida,
  YEAR(data_hora_brasilia)          AS ano,
  MONTH(data_hora_brasilia)         AS mes,
  atualizado_em_utc,
  _ingested_at
FROM com_datas
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY partida_id
  ORDER BY atualizado_em_utc DESC, _ingested_at DESC
) = 1