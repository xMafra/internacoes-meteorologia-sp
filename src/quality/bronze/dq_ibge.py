from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import BadZipFile, ZipFile

try:
    from .common import BRONZE_ROOT, Report, check_temporarios, print_report
except ImportError:
    from common import BRONZE_ROOT, Report, check_temporarios, print_report


RAIZ_IBGE = BRONZE_ROOT / "ibge"
EXTENSOES_MINIMAS = {".shp", ".shx", ".dbf", ".prj"}


def validar_ibge(ano: int = 2022, raiz: Path = RAIZ_IBGE) -> Report:
    report = Report("ibge")
    arquivo = raiz / str(ano) / f"SP_Municipios_{ano}.zip"
    report.add("arquivo_esperado", arquivo.is_file(), "arquivo encontrado" if arquivo.is_file() else "arquivo não encontrado", arquivo)
    if arquivo.is_file():
        tamanho = arquivo.stat().st_size
        report.add("arquivo_nao_vazio", tamanho > 0, f"{tamanho} bytes", arquivo)
        report.add("ano_nome_diretorio", arquivo.parent.name == str(ano) and str(ano) in arquivo.name, f"ano esperado={ano}", arquivo)
        try:
            with ZipFile(arquivo) as zip_ref:
                corrompido = zip_ref.testzip()
                membros = [Path(nome) for nome in zip_ref.namelist() if not nome.endswith("/")]
                por_base: dict[str, set[str]] = {}
                for membro in membros:
                    por_base.setdefault(membro.stem.lower(), set()).add(membro.suffix.lower())
                conjuntos = [extensoes for extensoes in por_base.values() if EXTENSOES_MINIMAS <= extensoes]
                report.add("zip_integro", corrompido is None, f"membro corrompido={corrompido}", arquivo)
                report.add("estrutura_shapefile", bool(conjuntos), f"conjuntos completos={len(conjuntos)}", arquivo)
        except (BadZipFile, OSError) as erro:
            report.add("zip_integro", False, f"{type(erro).__name__}: {erro}", arquivo)
    check_temporarios(report, raiz)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Data quality da malha IBGE na Bronze.")
    parser.add_argument("--ano", type=int, default=2022)
    args = parser.parse_args()
    report = validar_ibge(args.ano)
    print_report(report)
    return 0 if report.aprovado else 1


if __name__ == "__main__":
    raise SystemExit(main())
