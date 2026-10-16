"""Períodos oficiais e janelas técnicas do estudo.

Este módulo é a única fonte de verdade para datas e listas de anos. As
variáveis de ambiente existem para reprocessamentos controlados, mas os
defaults reproduzem o recorte metodológico do TCC.
"""

from __future__ import annotations

from datetime import date, timedelta
import os


def _date_env(nome: str, default: str) -> date:
    valor = os.getenv(nome, default)
    try:
        return date.fromisoformat(valor)
    except ValueError as erro:
        raise ValueError(f"{nome} deve estar no formato AAAA-MM-DD: {valor!r}") from erro


def _competencia_env(nome: str, default: str) -> tuple[int, int]:
    valor = os.getenv(nome, default)
    try:
        ano_texto, mes_texto = valor.split("-", 1)
        competencia = (int(ano_texto), int(mes_texto))
    except (TypeError, ValueError) as erro:
        raise ValueError(f"{nome} deve estar no formato AAAA-MM: {valor!r}") from erro
    if not 1 <= competencia[1] <= 12:
        raise ValueError(f"Mês inválido em {nome}: {valor!r}")
    return competencia


ANALYSIS_START_DATE = _date_env("TCC_ANALYSIS_START", "2015-01-01")
ANALYSIS_END_DATE = _date_env("TCC_ANALYSIS_END", "2025-12-31")

# D-7 de 01/01/2015 exige 25/12/2014. O suporte técnico não integra o
# universo analítico nem a dimensão de tempo.
INMET_TECHNICAL_START_DATE = ANALYSIS_START_DATE - timedelta(days=7)
INMET_TECHNICAL_END_DATE = ANALYSIS_END_DATE

# O início por competência coincide com o início analítico: uma AIH não
# pode ser apresentada em competência anterior à data de internação. A janela
# posterior alcança o último arquivo publicado e será avaliada pela auditoria
# de completude de 2025. Em 30/09/2026, julho/2026 era a última competência
# RDSP disponível; agosto retornava arquivo inexistente.
SIH_COMPETENCE_START = _competencia_env("TCC_SIH_COMPETENCE_START", "2015-01")
SIH_COMPETENCE_END = _competencia_env("TCC_SIH_COMPETENCE_END", "2026-07")


if ANALYSIS_START_DATE > ANALYSIS_END_DATE:
    raise ValueError("O início do período analítico não pode superar o fim.")
if SIH_COMPETENCE_START > SIH_COMPETENCE_END:
    raise ValueError("A competência SIH inicial não pode superar a final.")


DATA_INICIO = ANALYSIS_START_DATE.isoformat()
DATA_FIM = ANALYSIS_END_DATE.isoformat()
DATA_INMET_INICIO = INMET_TECHNICAL_START_DATE.isoformat()
DATA_INMET_FIM = INMET_TECHNICAL_END_DATE.isoformat()

ANALYSIS_YEARS = tuple(range(ANALYSIS_START_DATE.year, ANALYSIS_END_DATE.year + 1))
INMET_YEARS = tuple(
    range(INMET_TECHNICAL_START_DATE.year, INMET_TECHNICAL_END_DATE.year + 1)
)
EXPECTED_ANALYSIS_DAYS = (ANALYSIS_END_DATE - ANALYSIS_START_DATE).days + 1


def iter_competences(
    inicio: tuple[int, int] = SIH_COMPETENCE_START,
    fim: tuple[int, int] = SIH_COMPETENCE_END,
):
    """Gera competências inclusivas em ordem cronológica."""
    if inicio > fim:
        raise ValueError("A competência inicial não pode superar a final.")
    ano, mes = inicio
    while (ano, mes) <= fim:
        yield ano, mes
        if mes == 12:
            ano, mes = ano + 1, 1
        else:
            mes += 1


SIH_COMPETENCES = tuple(iter_competences())
EXPECTED_SIH_FILES = len(SIH_COMPETENCES)


def periodo_sih_kwargs() -> dict[str, int]:
    """Argumentos compatíveis com download/validação Bronze SIH."""
    return {
        "ano_inicio": SIH_COMPETENCE_START[0],
        "mes_inicio": SIH_COMPETENCE_START[1],
        "ano_fim": SIH_COMPETENCE_END[0],
        "mes_fim": SIH_COMPETENCE_END[1],
    }
