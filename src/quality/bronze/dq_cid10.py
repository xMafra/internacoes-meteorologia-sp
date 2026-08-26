from __future__ import annotations

import argparse
import csv
import hashlib
import io
from pathlib import Path
from zipfile import BadZipFile, ZipFile

try:
    from .common import BRONZE_ROOT, Report, check_temporarios, print_report
except ImportError:
    from common import BRONZE_ROOT, Report, check_temporarios, print_report


RAIZ_CID10 = BRONZE_ROOT / "cid10"
NOMES_ESPERADOS = {
    "CID-10-CAPITULOS.CSV",
    "CID-10-GRUPOS.CSV",
    "CID-10-CATEGORIAS.CSV",
    "CID-10-SUBCATEGORIAS.CSV",
}


def _cabecalho_csv(stream) -> tuple[bool, str]:
    texto = io.TextIOWrapper(stream, encoding="latin-1", newline="")
    primeira = texto.readline()
    colunas = next(csv.reader([primeira], delimiter=";"), []) if primeira else []
    return len(colunas) >= 2, f"colunas no cabeçalho={len(colunas)}"


def validar_cid10(raiz: Path = RAIZ_CID10) -> Report:
    report = Report("cid10")
    arquivo_zip = raiz / "CID10CSV.zip"
    report.add("arquivo_fonte", arquivo_zip.is_file(), "arquivo encontrado" if arquivo_zip.is_file() else "arquivo não encontrado", arquivo_zip)
    nomes_zip: dict[str, str] = {}
    hashes_zip: dict[str, str] = {}
    if arquivo_zip.is_file():
        tamanho = arquivo_zip.stat().st_size
        report.add("arquivo_nao_vazio", tamanho > 0, f"{tamanho} bytes", arquivo_zip)
        try:
            with ZipFile(arquivo_zip) as zip_ref:
                corrompido = zip_ref.testzip()
                nomes_zip = {Path(nome).name.upper(): nome for nome in zip_ref.namelist() if not nome.endswith("/")}
                ausentes_zip = sorted(NOMES_ESPERADOS - nomes_zip.keys())
                report.add("zip_integro", corrompido is None, f"membro corrompido={corrompido}", arquivo_zip)
                report.add("arquivos_no_zip", not ausentes_zip, "todos presentes" if not ausentes_zip else f"ausentes={ausentes_zip}", arquivo_zip)
                for nome in sorted(NOMES_ESPERADOS & nomes_zip.keys()):
                    with zip_ref.open(nomes_zip[nome]) as stream:
                        conteudo = stream.read()
                    hashes_zip[nome] = hashlib.sha256(conteudo).hexdigest()
                    with io.BytesIO(conteudo) as stream:
                        ok, detalhe = _cabecalho_csv(stream)
                    report.add("csv_zip_legivel", ok, detalhe, nome)
        except (BadZipFile, OSError, UnicodeError, csv.Error) as erro:
            report.add("zip_integro", False, f"{type(erro).__name__}: {erro}", arquivo_zip)

    extraidos = []
    for nome in sorted(NOMES_ESPERADOS):
        caminho = raiz / nome
        extraidos.append(caminho)
        existe = caminho.is_file()
        report.add("csv_extraido", existe, "arquivo encontrado" if existe else "arquivo não encontrado", caminho)
        if existe:
            tamanho = caminho.stat().st_size
            report.add("csv_nao_vazio", tamanho > 0, f"{tamanho} bytes", caminho)
            try:
                with caminho.open("rb") as stream:
                    ok, detalhe = _cabecalho_csv(stream)
                report.add("csv_legivel", ok, detalhe, caminho)
                if nome in hashes_zip:
                    hash_extraido = hashlib.sha256(caminho.read_bytes()).hexdigest()
                    report.add(
                        "csv_corresponde_ao_zip",
                        hash_extraido == hashes_zip[nome],
                        "conteúdo idêntico ao arquivo-fonte"
                        if hash_extraido == hashes_zip[nome]
                        else "conteúdo difere do arquivo-fonte",
                        caminho,
                    )
            except (OSError, UnicodeError, csv.Error) as erro:
                report.add("csv_legivel", False, f"{type(erro).__name__}: {erro}", caminho)
    check_temporarios(report, raiz)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Data quality dos arquivos CID-10 na Bronze.")
    parser.parse_args()
    report = validar_cid10()
    print_report(report)
    return 0 if report.aprovado else 1


if __name__ == "__main__":
    raise SystemExit(main())
