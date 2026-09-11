"""Wrappers Bronze com recorte fixo; imports e I/O somente em runtime."""

from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory


PERIODO_SIH = dict(ano_inicio=2023, mes_inicio=1, ano_fim=2025, mes_fim=12)
ANOS_INMET = (2023, 2024, 2025)
ANO_IBGE = 2022


def bronze_sih() -> list[str]:
    from src.bronze.sih import download_sih_periodo

    return [str(p) for p in download_sih_periodo(**PERIODO_SIH)]


def bronze_inmet() -> list[str]:
    from src.bronze.inmet import download_inmet

    return [str(download_inmet(ano)) for ano in ANOS_INMET]


def bronze_ibge() -> str:
    from src.bronze.ibge import download_malha_sp_2022

    return str(download_malha_sp_2022())


def bronze_cid10() -> list[str]:
    from src.bronze.cid10 import (
        ARQUIVOS_ESPERADOS, extrair_cid10, obter_caminho_zip,
        obter_diretorio_bronze,
    )

    arquivo_zip = obter_caminho_zip()
    if not arquivo_zip.is_file():
        raise FileNotFoundError(f"ZIP CID-10 local não encontrado: {arquivo_zip}")
    raiz = obter_diretorio_bronze()
    destinos = [raiz / nome for nome in ARQUIVOS_ESPERADOS]
    # Preserva inclusive arquivos inválidos para diagnóstico pelo DQ.
    ausentes = [p for p in destinos if not p.exists() and not p.is_symlink()]
    if ausentes:
        # O extrator original sobrescreve; reutilizá-lo em staging evita
        # alterar CSVs já provisionados na Bronze.
        with TemporaryDirectory(prefix="bronze_cid10_") as temporario:
            staging = Path(temporario)
            extrair_cid10(arquivo_zip, staging)
            for destino in ausentes:
                with (staging / destino.name).open("rb") as origem:
                    with destino.open("xb") as saida:
                        copyfileobj(origem, saida)
    return [str(p) for p in destinos]


def _exigir_aprovacao(report) -> None:
    from src.quality.bronze.common import print_report

    print_report(report)
    if not report.aprovado:
        raise ValueError(f"DQ Bronze {report.fonte} reprovado; consulte os checks no log.")


def dq_bronze_sih() -> None:
    from src.quality.bronze.dq_sih import validar_sih

    _exigir_aprovacao(validar_sih(**PERIODO_SIH))


def dq_bronze_inmet() -> None:
    from src.quality.bronze.dq_inmet import validar_inmet

    _exigir_aprovacao(validar_inmet(ano_inicio=2023, ano_fim=2025))


def dq_bronze_ibge() -> None:
    from src.quality.bronze.dq_ibge import validar_ibge

    _exigir_aprovacao(validar_ibge(ano=ANO_IBGE))


def dq_bronze_cid10() -> None:
    from src.quality.bronze.dq_cid10 import validar_cid10

    _exigir_aprovacao(validar_cid10())
