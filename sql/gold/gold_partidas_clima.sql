-- Clima da cidade do mandante no dia do jogo
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_partidas_clima AS
WITH base AS (
  SELECT
    p.partida_id, p.temporada, p.rodada, p.encerrada, p.resultado,
    p.data_hora_brasilia, p.data_partida, p.ano, p.mes,
    p.mandante_id, p.mandante, p.visitante_id, p.visitante,
    p.gols_mandante, p.gols_visitante,
    e.cidade_id, e.cidade, e.uf,
    c.temp_max_c, c.temp_min_c, c.temp_media_c, c.precipitacao_mm, c.vento_max_kmh, c.codigo_tempo_wmo
  FROM {catalog}.{schema}.silver_partidas AS p
  JOIN {catalog}.{schema}.silver_estadios AS e
    ON e.team_id = p.mandante_id
  LEFT JOIN {catalog}.{schema}.silver_clima_diario AS c
    ON c.cidade_id = e.cidade_id
   AND c.data = p.data_partida
)
SELECT
  *,
  temp_max_c IS NOT NULL                    AS clima_disponivel,
  gols_mandante + gols_visitante            AS total_gols,
  gols_mandante - gols_visitante            AS saldo_mandante,
  CASE resultado WHEN 'MANDANTE' THEN 3 WHEN 'EMPATE' THEN 1 WHEN 'VISITANTE' THEN 0 END  AS pontos_mandante,
  CASE resultado WHEN 'VISITANTE' THEN 3 WHEN 'EMPATE' THEN 1 WHEN 'MANDANTE' THEN 0 END  AS pontos_visitante,
  gols_mandante > 0 AND gols_visitante > 0  AS ambos_marcaram,
  precipitacao_mm >= 1.0                    AS choveu,  -- 1 mm é limite usado em meteorologia para dia de chuva
  CASE
    WHEN precipitacao_mm IS NULL THEN NULL
    WHEN precipitacao_mm < 1     THEN '1. Seco (< 1 mm)'
    WHEN precipitacao_mm < 10    THEN '2. Chuva fraca (1 a 10 mm)'
    WHEN precipitacao_mm < 25    THEN '3. Chuva moderada (10 a 25 mm)'
    ELSE '4. Chuva forte (>= 25 mm)'
  END                                       AS faixa_chuva,
  CASE
    WHEN temp_max_c IS NULL THEN NULL
    WHEN temp_max_c < 20    THEN '1. Frio (< 20 °C)'
    WHEN temp_max_c < 25    THEN '2. Ameno (20 a 25 °C)'
    WHEN temp_max_c < 30    THEN '3. Quente (25 a 30 °C)'
    ELSE '4. Muito quente (>= 30 °C)'
  END                                       AS faixa_temperatura,
  CASE WHEN HOUR(data_hora_brasilia) < 18 THEN 'Dia' ELSE 'Noite' END  AS turno
FROM base