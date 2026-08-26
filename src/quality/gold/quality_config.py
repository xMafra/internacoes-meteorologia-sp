MUNICIPIO_ESTACAO_PATH = "/home/jovyan/work/data/gold/municipio_estacao"
FATO_INTERNACAO_PATH = "/home/jovyan/work/data/gold/fato_internacao"
FATO_METEOROLOGIA_PATH = "/home/jovyan/work/data/gold/fato_internacao_meteorologia"

SILVER_SIH_PATH = "/home/jovyan/work/data/silver/sih"
SILVER_IBGE_PATH = "/home/jovyan/work/data/silver/ibge/2022"
SILVER_CID10_PATH = "/home/jovyan/work/data/silver/cid10"
SILVER_INMET_DIARIO_PATH = "/home/jovyan/work/data/silver/inmet_diario"

TOTAL_INTERNACOES_ESPERADO = 8_409_047
MUNICIPIOS_ESPERADOS = 645
ESTACOES_ESPERADAS = 40
DIAGNOSTICOS_DISTINTOS_ESPERADOS = 9_573
CATEGORIAS_CID_COMPLEMENTARES = ["U09", "U10"]
DATA_INMET_INICIO = "2023-01-01"
DATA_INMET_FIM = "2025-12-31"

# Limiares inicialmente informativos; podem virar ERROR futuramente.
MIN_COBERTURA_TEMPERATURA = 0.90
MIN_COBERTURA_UMIDADE = 0.90
MIN_COBERTURA_PRECIPITACAO = 0.80
MIN_COBERTURA_VENTO = 0.80
