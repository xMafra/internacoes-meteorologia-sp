from __future__ import annotations

import argparse
import tempfile
from datetime import date
from pathlib import Path

from dbfread import DBF
from pyreaddbc.readdbc import dbc2dbf

try:
    from .common import BRONZE_ROOT, Report, check_duplicados, check_temporarios, print_report
except ImportError:
    from common import BRONZE_ROOT, Report, check_duplicados, check_temporarios, print_report


RAIZ_SIH = BRONZE_ROOT / "sih"


def periodos(inicio: tuple[int, int], fim: tuple[int, int]):
    atual = inicio
    while atual <= fim:
        yield atual
        atual = (atual[0] + 1, 1) if atual[1] == 12 else (atual[0], atual[1] + 1)


def caminho_esperado(raiz: Path, ano: int, mes: int) -> Path:
    return raiz / str(ano) / f"RDSP{ano % 100:02d}{mes:02d}.dbc"


def validar_sih(
    ano_inicio: int,
    mes_inicio: int,
    ano_fim: int,
    mes_fim: int,
    raiz: Path = RAIZ_SIH,
) -> Report:
    if (ano_inicio, mes_inicio) > (ano_fim, mes_fim):
        raise ValueError("O período inicial não pode ser posterior ao final.")
    if not all(1 <= mes <= 12 for mes in (mes_inicio, mes_fim)):
        raise ValueError("Os meses devem estar entre 1 e 12.")

    report = Report("sih")
    esperados = [caminho_esperado(raiz, ano, mes) for ano, mes in periodos(
        (ano_inicio, mes_inicio), (ano_fim, mes_fim)
    )]
    encontrados = [arquivo for arquivo in esperados if arquivo.is_file()]
    ausentes = [arquivo for arquivo in esperados if not arquivo.is_file()]
    report.add(
        "quantidade_arquivos",
        not ausentes,
        f"esperados={len(esperados)}, encontrados={len(encontrados)}, ausentes={len(ausentes)}",
    )
    for arquivo in ausentes:
        report.add("arquivo_esperado", False, "arquivo não encontrado", arquivo)

    for arquivo in encontrados:
        tamanho = arquivo.stat().st_size
        report.add("arquivo_nao_vazio", tamanho > 0, f"{tamanho} bytes", arquivo)
        partes = arquivo.stem.upper()
        ano_nome = 2000 + int(partes[4:6])
        mes_nome = int(partes[6:8])
        coerente = arquivo.parent.name == str(ano_nome) and arquivo == caminho_esperado(
            raiz, ano_nome, mes_nome
        )
        report.add("periodo_nome_diretorio", coerente, f"período identificado={ano_nome}-{mes_nome:02d}", arquivo)
        if not tamanho:
            continue
        try:
            with tempfile.TemporaryDirectory(prefix="dq_sih_") as temporario:
                dbf = Path(temporario) / f"{arquivo.stem}.dbf"
                dbc2dbf(str(arquivo), str(dbf))
                tabela = DBF(dbf, load=False, encoding="latin-1")
                campos = tabela.field_names
                primeiro = next(iter(tabela), None)
            ok = bool(campos) and primeiro is not None
            report.add("dbc_legivel", ok, f"campos={len(campos)}, possui_registro={primeiro is not None}", arquivo)
        except Exception as erro:
            report.add("dbc_legivel", False, f"{type(erro).__name__}: {erro}", arquivo)

    check_duplicados(report, encontrados)
    check_temporarios(report, raiz)
    return report


def main() -> int:
    hoje = date.today()
    parser = argparse.ArgumentParser(description="Data quality dos arquivos mensais SIH na Bronze.")
    parser.add_argument("--ano-inicio", type=int, required=True)
    parser.add_argument("--mes-inicio", type=int, default=1)
    parser.add_argument("--ano-fim", type=int, default=hoje.year)
    parser.add_argument("--mes-fim", type=int, default=hoje.month)
    args = parser.parse_args()
    report = validar_sih(args.ano_inicio, args.mes_inicio, args.ano_fim, args.mes_fim)
    print_report(report)
    return 0 if report.aprovado else 1


if __name__ == "__main__":
    raise SystemExit(main())
