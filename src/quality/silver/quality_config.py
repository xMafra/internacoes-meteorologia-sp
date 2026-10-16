from src.common.study_period import (
    ANALYSIS_YEARS,
    DATA_FIM,
    DATA_INICIO,
    DATA_INMET_FIM,
    DATA_INMET_INICIO,
    INMET_YEARS,
    SIH_COMPETENCES,
)

SIH_PATH = "/home/jovyan/work/data/silver/sih"
IBGE_PATH = "/home/jovyan/work/data/silver/ibge/2022"
CID10_PATH = "/home/jovyan/work/data/silver/cid10"
INMET_PATH = "/home/jovyan/work/data/silver/inmet"
INMET_DIARIO_PATH = "/home/jovyan/work/data/silver/inmet_diario"

ANOS = list(ANALYSIS_YEARS)
ANOS_INMET = list(INMET_YEARS)
ANOS_SIH = sorted({ano for ano, _ in SIH_COMPETENCES})

SIH_TOTAL_ESPERADO = None
IBGE_MUNICIPIOS_ESPERADOS = 645
CID10_CODIGOS_ESPERADOS = 12_451
INMET_ESTACOES_ESPERADAS = None
INMET_HORARIO_ESPERADO = None
INMET_DIARIO_ESPERADO = None
