-- Clima diário por cidade
CREATE OR REPLACE TABLE {catalog}.{schema}.silver_clima_diario AS
WITH arrays AS (
  SELECT
    cidade_id,
    daily.time                 AS dia,
    daily.temperature_2m_max   AS temp_max,
    daily.temperature_2m_min   AS temp_min,
    daily.temperature_2m_mean  AS temp_media,
    daily.precipitation_sum    AS precipitacao,
    daily.rain_sum             AS chuva,
    daily.wind_speed_10m_max   AS vento_max,
    daily.weather_code         AS codigo_tempo,
    _ingested_at
  FROM {catalog}.{schema}.bronze_clima
),
dias AS (
  SELECT
    cidade_id,
    _ingested_at,
    explode(arrays_zip(dia, temp_max, temp_min, temp_media, precipitacao, chuva, vento_max, codigo_tempo)) AS d
  FROM arrays
)
SELECT
  cidade_id,
  CAST(d.dia AS DATE)                                               AS data,
  ROUND(d.temp_max, 1)                                              AS temp_max_c,
  ROUND(d.temp_min, 1)                                              AS temp_min_c,
  ROUND(COALESCE(d.temp_media, (d.temp_max + d.temp_min) / 2), 1)   AS temp_media_c,
  ROUND(d.precipitacao, 1)                                          AS precipitacao_mm,
  ROUND(d.chuva, 1)                                                 AS chuva_mm,
  ROUND(d.vento_max, 1)                                             AS vento_max_kmh,
  CAST(d.codigo_tempo AS INT)                                       AS codigo_tempo_wmo,
  _ingested_at
FROM dias
QUALIFY ROW_NUMBER() OVER (PARTITION BY cidade_id, d.dia ORDER BY _ingested_at DESC) = 1