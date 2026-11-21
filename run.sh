#!/usr/bin/env bash
# Executa o pipeline completo no Databricks:  ./run.sh
#
# Pré-requisitos:
#   1. Databricks CLI instalada e autenticada:  databricks auth login --host <url-do-workspace>
#   2. Secret scope "fc" com a chave da API:    databricks secrets create-scope fc
#                                               databricks secrets put-secret fc football_data_api_key
set -euo pipefail
cd "$(dirname "$0")"  # Força rodar a partir da raiz do repositório

echo "==> 1/3 Validando o bundle (databricks.yml)"
databricks bundle validate

echo "==> 2/3 Publicando o código e o Job no workspace"
databricks bundle deploy

echo "==> 3/3 Executando o pipeline (aguarda terminar)"
databricks bundle run pipeline_futebol_clima