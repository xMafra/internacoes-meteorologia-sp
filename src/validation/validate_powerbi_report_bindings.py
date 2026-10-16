"""Valida se todos os campos usados pelo report.json existem no modelo TMDL."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "Dashboard" / "Dash.Report" / "report.json"
TABLES = ROOT / "Dashboard" / "Dash.SemanticModel" / "definition" / "tables"


def _identificador(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("'") and texto.endswith("'"):
        return texto[1:-1].replace("''", "'")
    return texto


def ler_modelo() -> dict[str, set[str]]:
    modelo: dict[str, set[str]] = {}
    declaracao = re.compile(r"^\s*(column|measure)\s+('(?:''|[^'])*'|[^\s=]+)")
    for caminho in TABLES.glob("*.tmdl"):
        linhas = caminho.read_text(encoding="utf-8-sig").splitlines()
        tabela = None
        for linha in linhas:
            if linha.startswith("table "):
                tabela = _identificador(linha.removeprefix("table "))
                modelo[tabela] = set()
                break
        if tabela is None:
            continue
        for linha in linhas:
            correspondencia = declaracao.match(linha)
            if correspondencia:
                modelo[tabela].add(_identificador(correspondencia.group(2)))
    return modelo


def selecoes(relatorio: dict) -> list[tuple[str, str, str]]:
    encontrados: list[tuple[str, str, str]] = []
    for pagina in relatorio.get("sections", []):
        pagina_nome = pagina.get("displayName", pagina.get("name", "?"))
        for contenedor in pagina.get("visualContainers", []):
            configuracao = contenedor.get("config")
            if not configuracao:
                continue
            visual = json.loads(configuracao).get("singleVisual", {})
            titulo = visual.get("visualType", "visual")
            consulta = visual.get("prototypeQuery", {})
            fontes = {
                fonte.get("Name"): fonte.get("Entity")
                for fonte in consulta.get("From", [])
                if fonte.get("Name") and fonte.get("Entity")
            }
            for selecao in consulta.get("Select", []):
                campo = selecao.get("Column") or selecao.get("Measure")
                if not campo:
                    continue
                referencia = campo.get("Expression", {}).get("SourceRef", {})
                origem = referencia.get("Entity") or fontes.get(referencia.get("Source"))
                propriedade = campo.get("Property")
                if origem and propriedade:
                    encontrados.append((origem, propriedade, f"{pagina_nome} / {titulo}"))
    return encontrados


def main() -> int:
    modelo = ler_modelo()
    relatorio = json.loads(REPORT.read_text(encoding="utf-8-sig"))
    erros = []
    for tabela, propriedade, origem in selecoes(relatorio):
        if tabela not in modelo:
            erros.append(f"Tabela ausente: {tabela} ({origem})")
        elif propriedade not in modelo[tabela]:
            erros.append(f"Campo ausente: {tabela}[{propriedade}] ({origem})")
    if erros:
        print("BINDINGS_POWER_BI=FAIL")
        print("\n".join(sorted(set(erros))))
        return 1
    print(f"BINDINGS_POWER_BI=PASS; campos_referenciados={len(selecoes(relatorio))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
