-- Estatísticas se o clima influencia o Brasileirão "
CREATE OR REPLACE TABLE {catalog}.{schema}.gold_resumo_clima AS
WITH jogos AS (
  SELECT *
  FROM {catalog}.{schema}.gold_partidas_clima
  WHERE encerrada AND clima_disponivel
),
longo AS (
  -- Cada partida vira 4 linhas, uma por dimensão analisada
  SELECT j.*, d.dimensao, d.faixa
  FROM jogos AS j
  LATERAL VIEW stack(
    4,
    'geral',       'Todos os jogos',
    'chuva',       faixa_chuva,
    'temperatura', faixa_temperatura,
    'turno',       turno
  ) d AS dimensao, faixa
)
SELECT
  dimensao,
  faixa,
  COUNT(*)                                                    AS jogos,
  ROUND(AVG(total_gols), 2)                                   AS media_gols,
  ROUND(100 * COUNT_IF(resultado = 'MANDANTE')  / COUNT(*), 1)  AS pct_vitoria_mandante,
  ROUND(100 * COUNT_IF(resultado = 'EMPATE')    / COUNT(*), 1)  AS pct_empate,
  ROUND(100 * COUNT_IF(resultado = 'VISITANTE') / COUNT(*), 1)  AS pct_vitoria_visitante,
  ROUND(100 * COUNT_IF(ambos_marcaram)          / COUNT(*), 1)  AS pct_ambos_marcaram
FROM longo
GROUP BY dimensao, faixa