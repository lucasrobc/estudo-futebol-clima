-- Árbitros por partida
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_arbitros AS
SELECT
  CAST(m.id AS BIGINT)                          AS partida_id,
  CAST(r.id AS BIGINT)                          AS arbitro_id,
  COALESCE(TRIM(r.name), 'Não informado')       AS nome,
  COALESCE(r.type, 'NAO_INFORMADO')             AS funcao,
  COALESCE(r.nationality, 'Não informado')      AS nacionalidade,
  _ingested_at
FROM {catalog}.{schema}.bronze_partidas
LATERAL VIEW explode(matches) p AS m
LATERAL VIEW explode(m.referees) a AS r
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.id, r.id ORDER BY _ingested_at DESC) = 1