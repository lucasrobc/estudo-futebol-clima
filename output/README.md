# output/
Dataset final do pipeline em `partidas_clima/`: Parquet com compressão Snappy, particionado por `ano=AAAA/mes=M`.

- 1 linha por partida do Brasileirão 2026, com o clima da cidade do mandante no dia do jogo e as métricas da camada Gold.
- Gerado pelo notebook `notebooks/06_output_parquet` ou pelo pipeline completo.
- Leitura: `spark.read.parquet("output/partidas_clima")` ou `pandas.read_parquet("output/partidas_clima")`.