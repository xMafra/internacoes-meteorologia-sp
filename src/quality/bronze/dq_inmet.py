from __future__ import annotations

import argparse
import csv
import io
import re
from pathlib import Path
from zipfile import BadZipFile, ZipFile

try:
    from .common import BRONZE_ROOT, Report, check_duplicados, check_temporarios, print_report
except ImportError:
    from common import BRONZE_ROOT, Report, check_duplicados, check_temporarios, print_report


RAIZ_INMET = BRONZE_ROOT / "inmet"


def _csv_legivel(zip_ref: ZipFile, membro: str, ano: int) -> tuple[bool, str]:
    with zip_ref.open(membro) as bruto:
        texto = io.TextIOWrapper(bruto, encoding="latin-1", newline="")
        linhas = [texto.readline() for _ in range(10)]
    if not any(linhas):
        return False, "CSV vazio"
    cabecalho = next((linha for linha in linhas if "DATA" in linha.upper() and ";" in linha), "")
    estrutura = len(next(csv.reader([cabecalho], delimiter=";"), [])) >= 2
    texto_amostra = "".join(linhas)
    ano_encontrado = bool(re.search(rf"(?:^|\D){ano}(?:\D|$)", texto_amostra)) or str(ano) in membro
    return estrutura and ano_encontrado, f"cabeçalho={estrutura}, ano={ano_encontrado}"


def validar_inmet(ano_inicio: int, ano_fim: int, raiz: Path = RAIZ_INMET) -> Report:
    if ano_inicio > ano_fim:
        raise ValueError("O ano inicial não pode ser posterior ao final.")
    report = Report("inmet")
    esperados = [raiz / str(ano) / f"{ano}.zip" for ano in range(ano_inicio, ano_fim + 1)]
    encontrados = [arquivo for arquivo in esperados if arquivo.is_file()]
    ausentes = [arquivo for arquivo in esperados if not arquivo.is_file()]
    report.add("quantidade_arquivos", not ausentes, f"esperados={len(esperados)}, encontrados={len(encontrados)}, ausentes={len(ausentes)}")
    for arquivo in ausentes:
        report.add("arquivo_esperado", False, "arquivo não encontrado", arquivo)

    for arquivo in encontrados:
        tamanho = arquivo.stat().st_size
        report.add("arquivo_nao_vazio", tamanho > 0, f"{tamanho} bytes", arquivo)
        ano = int(arquivo.stem)
        report.add("ano_nome_diretorio", arquivo.parent.name == arquivo.stem, f"ano identificado={ano}", arquivo)
        try:
            with ZipFile(arquivo) as zip_ref:
                corrompido = zip_ref.testzip()
                csvs = [nome for nome in zip_ref.namelist() if nome.upper().endswith(".CSV")]
                report.add("zip_integro", corrompido is None, f"membro corrompido={corrompido}", arquivo)
                report.add("estrutura_minima", bool(csvs), f"arquivos CSV={len(csvs)}", arquivo)
                erros = []
                for membro in csvs:
                    ok, detalhe = _csv_legivel(zip_ref, membro, ano)
                    if not ok:
                        erros.append(f"{membro}: {detalhe}")
                report.add("csvs_legiveis_e_ano", not erros, "todos legíveis" if not erros else "; ".join(erros), arquivo)
        except (BadZipFile, OSError) as erro:
            report.add("zip_integro", False, f"{type(erro).__name__}: {erro}", arquivo)

    check_duplicados(report, encontrados)
    check_temporarios(report, raiz)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Data quality dos arquivos anuais INMET na Bronze.")
    parser.add_argument("--ano-inicio", type=int, required=True)
    parser.add_argument("--ano-fim", type=int, required=True)
    args = parser.parse_args()
    report = validar_inmet(args.ano_inicio, args.ano_fim)
    print_report(report)
    return 0 if report.aprovado else 1


if __name__ == "__main__":
    raise SystemExit(main())
