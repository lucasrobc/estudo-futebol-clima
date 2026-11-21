-- Times da temporada
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_times AS
WITH times AS (
  SELECT explode(teams) AS t, _ingested_at
  FROM {catalog}.{schema}.bronze_times
)
SELECT
  CAST(t.id AS INT)                                     AS team_id,
  TRIM(t.name)                                          AS nome,
  TRIM(t.shortName)                                     AS nome_curto,
  UPPER(TRIM(t.tla))                                    AS sigla,
  COALESCE(NULLIF(TRIM(t.venue), ''), 'Não informado')  AS estadio,
  CAST(t.founded AS INT)                                AS ano_fundacao,
  _ingested_at
FROM times
-- Se o mesmo time aparecer mais de uma vez, fica a versão mais recente
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY _ingested_at DESC) = 1