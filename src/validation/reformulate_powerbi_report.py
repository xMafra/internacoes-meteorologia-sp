"""Reconstrói o report.json legado em seis páginas orientadas às perguntas.

O script preserva o envelope PBIP e o tema do relatório, substituindo apenas
as seções e seus visuais. A geração determinística facilita auditoria e rollback.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "Dashboard" / "Dash.Report" / "report.json"
BG = "#F2F5F9"
TEXT = "#17324D"
MUTED = "#526579"
BLUE = "#357ABD"
ORANGE = "#D97732"
TEAL = "#087F8C"
PURPLE = "#7866A6"
BORDER = "#DDE5EE"
WHITE = "#FFFFFF"


def literal(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


def color(value: str) -> dict:
    return {"solid": {"color": literal(f"'{value}'")}}


def visual_name(page: int, index: int) -> str:
    return f"p{page:02d}v{index:02d}000000000000000"[:20]


def container(config: dict, x: int, y: int, w: int, h: int, z: int = 5000, filters=None) -> dict:
    config["layouts"] = [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h, "tabOrder": z}}]
    return {
        "config": json.dumps(config, ensure_ascii=False, separators=(",", ":")),
        "filters": json.dumps(filters or [], ensure_ascii=False, separators=(",", ":")),
        "height": float(h), "width": float(w), "x": float(x), "y": float(y), "z": float(z),
    }


def textbox(page: int, index: int, text: str, x: int, y: int, w: int, h: int, *,
            size: int = 10, font_color: str = TEXT, weight: str = "normal",
            background: str | None = None, z: int = 5000) -> dict:
    run = {"value": text, "textStyle": {"fontFamily": "Segoe UI", "fontSize": f"{size}pt", "color": font_color, "fontWeight": weight}}
    cfg = {
        "name": visual_name(page, index),
        "singleVisual": {
            "visualType": "textbox", "drillFilterOtherVisuals": True,
            "objects": {"general": [{"properties": {"paragraphs": [{"textRuns": [run]}]}}]},
            "vcObjects": {
                "background": [{"properties": {"show": literal("true" if background else "false"), "color": color(background or BG), "transparency": literal("0L" if background else "100L")}}],
                "border": [{"properties": {"show": literal("false")}}],
                "visualHeader": [{"properties": {"show": literal("false")}}],
            },
        },
    }
    return container(cfg, x, y, w, h, z)


def field(entity: str, prop: str, kind: str = "Measure", label: str | None = None) -> dict:
    return {"entity": entity, "property": prop, "kind": kind, "label": label or prop, "queryRef": f"{entity}.{prop}"}


def build_query(roles: dict[str, list[dict]]) -> tuple[dict, dict]:
    fields = []
    for values in roles.values():
        for item in values:
            if item not in fields:
                fields.append(item)
    entities = []
    for item in fields:
        if item["entity"] not in entities:
            entities.append(item["entity"])
    source = {entity: f"s{i}" for i, entity in enumerate(entities)}
    projections = {role: [{"queryRef": item["queryRef"], **({"active": True} if role in ("Category", "Rows", "Columns", "Values") and i == 0 else {})} for i, item in enumerate(values)] for role, values in roles.items()}
    select = []
    for item in fields:
        expression = {"Expression": {"SourceRef": {"Source": source[item["entity"]]}}, "Property": item["property"]}
        select.append({item["kind"]: expression, "Name": item["queryRef"], "NativeReferenceName": item["label"]})
    query = {
        "Version": 2,
        "From": [{"Name": source[e], "Entity": e, "Type": 0} for e in entities],
        "Select": select,
    }
    return projections, query


def vc_objects(title: str) -> dict:
    return {
        "title": [{"properties": {"show": literal("true"), "text": literal(f"'{title}'"), "fontColor": color(TEXT), "fontFamily": literal("'Segoe UI Semibold'"), "fontSize": literal("11L"), "alignment": literal("'left'"), "titleWrap": literal("true")}}],
        "background": [{"properties": {"show": literal("true"), "color": color(WHITE), "transparency": literal("0L")}}],
        "border": [{"properties": {"show": literal("true"), "color": color(BORDER), "radius": literal("8L"), "width": literal("1L")}}],
        "dropShadow": [{"properties": {"show": literal("false")}}],
        "visualHeader": [{"properties": {"foreground": color(MUTED), "show": literal("true")}}],
    }


def visual(page: int, index: int, visual_type: str, title: str, roles: dict[str, list[dict]],
           x: int, y: int, w: int, h: int, *, default_color: str = TEAL, filters=None) -> dict:
    projections, query = build_query(roles)
    objects = {
        "dataPoint": [{"properties": {"defaultColor": color(default_color)}}],
        "labels": [{"properties": {"show": literal("false"), "fontFamily": literal("'Segoe UI'"), "fontSize": literal("9L"), "color": color(TEXT)}}],
        "categoryAxis": [{"properties": {"fontFamily": literal("'Segoe UI'"), "fontSize": literal("9L"), "labelColor": color(MUTED), "titleColor": color(MUTED)}}],
        "valueAxis": [{"properties": {"fontFamily": literal("'Segoe UI'"), "fontSize": literal("9L"), "labelColor": color(MUTED), "titleColor": color(MUTED), "gridlineColor": color(BORDER)}}],
        "legend": [{"properties": {"fontFamily": literal("'Segoe UI'"), "fontSize": literal("9L"), "labelColor": color(MUTED), "showTitle": literal("false")}}],
    }
    cfg = {
        "name": visual_name(page, index),
        "singleVisual": {
            "visualType": visual_type, "projections": projections, "prototypeQuery": query,
            "drillFilterOtherVisuals": True, "objects": objects, "vcObjects": vc_objects(title),
        },
    }
    return container(cfg, x, y, w, h, 5000 + index * 100, filters)


def card(page: int, index: int, title: str, metric: dict, x: int, y: int, w: int, h: int, *, accent: str = TEAL) -> dict:
    projections, query = build_query({"Data": [metric]})
    cfg = {
        "name": visual_name(page, index),
        "singleVisual": {
            "visualType": "cardVisual", "projections": projections, "prototypeQuery": query,
            "drillFilterOtherVisuals": True,
            "objects": {
                "label": [{"properties": {"show": literal("false")}, "selector": {"id": "default"}}],
                "value": [{"properties": {"fontFamily": literal("'Segoe UI Semibold'"), "fontSize": literal("25L"), "fontColor": color(accent), "horizontalAlignment": literal("'center'")}, "selector": {"id": "default"}}],
            },
            "vcObjects": vc_objects(title),
        },
    }
    return container(cfg, x, y, w, h, 5000 + index * 100)


def slicer(page: int, index: int, title: str, item: dict, x: int, y: int, w: int, h: int) -> dict:
    projections, query = build_query({"Values": [item]})
    cfg = {
        "name": visual_name(page, index),
        "singleVisual": {
            "visualType": "slicer", "projections": projections, "prototypeQuery": query,
            "drillFilterOtherVisuals": True,
            "objects": {
                "data": [{"properties": {"mode": literal("'Dropdown'")}}],
                "header": [{"properties": {"text": literal(f"'{title}'"), "show": literal("true"), "textSize": literal("10L"), "fontFamily": literal("'Segoe UI Semibold'"), "fontColor": color(TEXT)}}],
                "items": [{"properties": {"textSize": literal("9L"), "fontFamily": literal("'Segoe UI'"), "fontColor": color(MUTED)}}],
            },
            "vcObjects": vc_objects(""),
        },
    }
    cfg["singleVisual"]["vcObjects"]["title"] = [{"properties": {"show": literal("false")}}]
    return container(cfg, x, y, w, h, 5000 + index * 100)


def header(page: int, title: str, subtitle: str) -> list[dict]:
    return [
        textbox(page, 0, "", 0, 0, 1280, 100, background=TEXT, z=1000),
        textbox(page, 1, title, 24, 18, 1210, 42, size=22, font_color=WHITE, weight="bold", z=2000),
        textbox(page, 2, subtitle, 24, 60, 1210, 34, size=10, font_color="#D0DCE8", z=3000),
    ]


def footer(page: int, text: str, index: int = 90) -> dict:
    return textbox(page, index, f"Análise descritiva de associação. {text}", 24, 681, 1232, 32, size=8, font_color=MUTED, background=WHITE, z=19000)


def page_config() -> str:
    cfg = {"objects": {"background": [{"properties": {"color": color(BG), "transparency": literal("0L")}}], "outspace": [{"properties": {"color": color(BG)}}]}}
    return json.dumps(cfg, separators=(",", ":"))


def make_page(number: int, name: str, visuals: list[dict], filters=None) -> dict:
    return {
        "config": page_config(), "displayName": f"{number} - {name}", "displayOption": 1,
        "filters": json.dumps(filters or [], ensure_ascii=False, separators=(",", ":")),
        "height": 720.0, "name": f"pagina{number:02d}0000000000000"[:20], "width": 1280.0,
        "visualContainers": visuals,
    }


def equals_filter(entity: str, prop: str, value: int) -> dict:
    source = "f"
    return {
        "expression": {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
        "filter": {"Version": 2, "From": [{"Name": source, "Entity": entity, "Type": 0}], "Where": [{"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}}, "Right": {"Literal": {"Value": f"{value}L"}}}}}]},
        "type": "Advanced", "howCreated": 1,
    }


def build_pages() -> list[dict]:
    p1 = header(1, "Como as internações variam segundo a temperatura?", "Contraste intramunicipal entre dias frios (<20 °C) e quentes (≥25 °C), 2015–2025; agregado estadual exibido apenas como visão secundária.")
    p1 += [
        card(1, 4, "Mediana intramunicipal", field("analitico_contraste_municipio", "Mediana Contraste Intramunicipal"), 24, 120, 216, 90, accent=TEAL),
        card(1, 5, "Q1", field("analitico_contraste_municipio", "Q1 Contraste Intramunicipal"), 252, 120, 216, 90, accent=BLUE),
        card(1, 6, "Q3", field("analitico_contraste_municipio", "Q3 Contraste Intramunicipal"), 480, 120, 216, 90, accent=ORANGE),
        card(1, 7, "Municípios válidos", field("analitico_contraste_municipio", "Municípios Válidos"), 708, 120, 216, 90, accent=PURPLE),
        card(1, 8, "Agregado estadual (secundário)", field("fato_municipio_dia", "Variação Percentual Frio Quente"), 936, 120, 320, 90, accent=MUTED),
        visual(1, 9, "columnChart", "Municípios por classe de contraste", {"Category": [field("analitico_contraste_municipio", "Classe Contraste", "Column")], "Y": [field("analitico_contraste_municipio", "% Municípios por Classe")]}, 24, 226, 392, 220, default_color=TEAL),
        visual(1, 10, "lineClusteredColumnComboChart", "Faixas térmicas: agregado estadual × mediana municipal", {"Category": [field("analitico_temperatura_faixa", "faixa_temperatura", "Column", "Faixa de temperatura")], "Y": [field("analitico_temperatura_faixa", "Média Agregada por Faixa")], "Y2": [field("analitico_temperatura_faixa", "Mediana Municipal por Faixa")]}, 432, 226, 824, 220, default_color=BLUE),
        textbox(1, 11, "Evidência principal: cada município elegível contribui uma vez para mediana e quartis. Visão secundária: a média agregada dos município-dias é influenciada pela composição territorial. Elegibilidade fixa: ≥30 dias frios e ≥30 dias quentes; aproximadamente zero: entre -1% e +1%.", 24, 462, 1232, 104, size=10, font_color=TEXT, background=WHITE),
        footer(1, "O agregado estadual é influenciado pela composição territorial."),
    ]

    p2 = header(2, "A associação permanece consistente entre 2015 e 2025?", "Mediana, Q1 e Q3 do contraste intramunicipal por ano; valores acima de zero indicam maior média em dias frios.")
    p2 += [
        card(2, 4, "Maior contraste observado (ano)", field("analitico_temporal_ano", "Maior Contraste Observado Ano"), 24, 120, 260, 90, accent=ORANGE),
        card(2, 5, "Menor contraste observado (ano)", field("analitico_temporal_ano", "Menor Contraste Observado Ano"), 296, 120, 260, 90, accent=BLUE),
        textbox(2, 6, "A série anual não recebe slicer de ano: selecionar um único ano destruiria a comparação 2015–2025.", 572, 120, 684, 90, size=10, font_color=MUTED, background=WHITE),
        visual(2, 7, "lineChart", "Mediana e intervalo interquartil por ano", {"Category": [field("analitico_temporal_ano", "ano", "Column", "Ano")], "Y": [field("analitico_temporal_ano", "Q1 Contraste Anual"), field("analitico_temporal_ano", "Mediana Contraste Anual"), field("analitico_temporal_ano", "Q3 Contraste Anual"), field("analitico_temporal_ano", "Linha Zero Temporal")]}, 24, 226, 780, 260, default_color=TEAL),
        visual(2, 8, "clusteredColumnChart", "Municípios positivos e negativos por ano", {"Category": [field("analitico_temporal_ano", "ano", "Column", "Ano")], "Y": [field("analitico_temporal_ano", "% Positivos Anual"), field("analitico_temporal_ano", "% Negativos Anual")]}, 820, 226, 436, 260, default_color=PURPLE),
        visual(2, 9, "lineClusteredColumnComboChart", "Sazonalidade mensal: internações e temperatura (medianas municipais)", {"Category": [field("analitico_sazonalidade_mes", "mes_abreviado", "Column", "Mês")], "Y": [field("analitico_sazonalidade_mes", "Mediana Municipal Internações Mês")], "Y2": [field("analitico_sazonalidade_mes", "Mediana Municipal Temperatura Mês")]}, 24, 502, 1232, 170, default_color=TEAL),
        footer(2, "A variação temporal é descritiva e não implica causalidade."),
    ]

    cid_chapter = field("dim_cid", "descricao_capitulo", "Column", "Capítulo")
    cid_group = field("dim_cid", "descricao_grupo", "Column", "Grupo")
    cid_category = field("dim_cid", "descricao_categoria", "Column", "Categoria")
    p3 = header(3, "Quais diagnósticos apresentam maiores contrastes?", "Hierarquia CID-10 com mediana intramunicipal, quartis, amostra e volume; use drilldown Capítulo → Grupo → Categoria.")
    p3 += [
        slicer(3, 4, "Capítulo", cid_chapter, 24, 112, 216, 56),
        slicer(3, 5, "Grupo", cid_group, 252, 112, 300, 56),
        slicer(3, 6, "Categoria", cid_category, 564, 112, 360, 56),
        textbox(3, 7, "Percentuais relativos devem ser lidos junto ao volume e ao número de municípios válidos.", 936, 112, 320, 56, size=9, font_color=MUTED, background=WHITE),
        visual(3, 8, "matrix", "Hierarquia CID: contraste, dispersão, amostra e volume", {"Rows": [cid_chapter, cid_group, cid_category], "Values": [field("analitico_cid_resumo", "Mediana Contraste CID"), field("analitico_cid_resumo", "Q1 Contraste CID"), field("analitico_cid_resumo", "Q3 Contraste CID"), field("analitico_cid_resumo", "Municípios Válidos CID"), field("analitico_cid_resumo", "% Municípios Positivos CID"), field("analitico_cid_resumo", "Volume Internações CID Analítico")]}, 24, 184, 760, 488),
        visual(3, 9, "scatterChart", "Capítulos: contraste × volume", {"Details": [cid_chapter], "X": [field("analitico_cid_resumo", "Mediana Contraste CID")], "Y": [field("analitico_cid_resumo", "Volume Internações CID Analítico")], "Size": [field("analitico_cid_resumo", "Municípios Válidos CID")], "Tooltips": [field("analitico_cid_resumo", "Q1 Contraste CID"), field("analitico_cid_resumo", "Q3 Contraste CID"), field("analitico_cid_resumo", "% Municípios Positivos CID")]}, 800, 184, 456, 238, default_color=PURPLE),
        visual(3, 10, "barChart", "Ranking por mediana intramunicipal", {"Category": [cid_chapter], "Y": [field("analitico_cid_resumo", "Mediana Contraste CID")], "Tooltips": [field("analitico_cid_resumo", "Volume Internações CID Analítico"), field("analitico_cid_resumo", "Municípios Válidos CID")]}, 800, 434, 456, 238, default_color=TEAL),
        footer(3, "Diferenças relativas devem ser interpretadas junto ao volume e à amostra."),
    ]

    p4 = header(4, "O contraste varia segundo faixa etária e sexo?", "Mediana intramunicipal de internações por município-dia; filtros CID são opcionais e não representam taxas populacionais.")
    p4 += [
        slicer(4, 4, "Capítulo CID (opcional)", cid_chapter, 24, 112, 260, 56),
        slicer(4, 5, "Grupo CID (opcional)", cid_group, 296, 112, 340, 56),
        slicer(4, 6, "Categoria CID (opcional)", cid_category, 648, 112, 392, 56),
        textbox(4, 7, "Sem seleção CID: todas as internações.", 1052, 112, 204, 56, size=9, font_color=MUTED, background=WHITE),
        visual(4, 8, "matrix", "Matriz idade × sexo: mediana do contraste", {"Rows": [field("analitico_demografia_resumo", "faixa_etaria", "Column", "Faixa etária")], "Columns": [field("analitico_demografia_resumo", "sexo_descricao", "Column", "Sexo")], "Values": [field("analitico_demografia_resumo", "Mediana Contraste Demográfico"), field("analitico_demografia_resumo", "Municípios Válidos Demográfico")]}, 24, 184, 596, 488),
        visual(4, 9, "clusteredColumnChart", "Contraste por faixa etária e sexo", {"Category": [field("analitico_demografia_resumo", "faixa_etaria", "Column", "Faixa etária")], "Series": [field("analitico_demografia_resumo", "sexo_descricao", "Column", "Sexo")], "Y": [field("analitico_demografia_resumo", "Mediana Contraste Demográfico")], "Tooltips": [field("analitico_demografia_resumo", "Q1 Contraste Demográfico"), field("analitico_demografia_resumo", "Q3 Contraste Demográfico"), field("analitico_demografia_resumo", "Municípios Válidos Demográfico"), field("analitico_demografia_resumo", "Volume Internações Demográfico")]}, 636, 184, 620, 488, default_color=PURPLE),
        footer(4, "Resultados não representam taxas populacionais por idade ou sexo."),
    ]

    p5 = header(5, "A associação muda com a temperatura de dias anteriores?", "Comparação intramunicipal de D0, D-1, D-3 e D-7, com elegibilidade calculada separadamente em cada defasagem.")
    p5 += [
        slicer(5, 4, "Alvo analítico", field("analitico_lag_resumo", "alvo_analitico", "Column", "Alvo analítico"), 24, 112, 620, 56),
        textbox(5, 5, "Sem seleção: internações totais. Selecione Capítulo X, J09-J18 ou J20-J22 para recortes respiratórios.", 660, 112, 596, 56, size=9, font_color=MUTED, background=WHITE),
        visual(5, 6, "lineChart", "Mediana e intervalo interquartil por defasagem", {"Category": [field("analitico_lag_resumo", "defasagem", "Column", "Defasagem")], "Y": [field("analitico_lag_resumo", "Q1 Contraste Lag"), field("analitico_lag_resumo", "Mediana Contraste Lag"), field("analitico_lag_resumo", "Q3 Contraste Lag"), field("analitico_lag_resumo", "Linha Zero Lag")], "Tooltips": [field("analitico_lag_resumo", "Mediana Média Frio Lag"), field("analitico_lag_resumo", "Mediana Média Quente Lag"), field("analitico_lag_resumo", "Municípios Válidos Lag")]}, 24, 184, 760, 300, default_color=TEAL),
        visual(5, 7, "clusteredColumnChart", "Municípios positivos e negativos por defasagem", {"Category": [field("analitico_lag_resumo", "defasagem", "Column", "Defasagem")], "Y": [field("analitico_lag_resumo", "% Positivos Lag"), field("analitico_lag_resumo", "% Negativos Lag")]}, 800, 184, 456, 300, default_color=PURPLE),
        visual(5, 8, "matrix", "Resumo auditável por defasagem", {"Rows": [field("analitico_lag_resumo", "defasagem", "Column", "Defasagem")], "Values": [field("analitico_lag_resumo", "Mediana Contraste Lag"), field("analitico_lag_resumo", "Q1 Contraste Lag"), field("analitico_lag_resumo", "Q3 Contraste Lag"), field("analitico_lag_resumo", "Municípios Válidos Lag"), field("analitico_lag_resumo", "% Positivos Lag"), field("analitico_lag_resumo", "% Negativos Lag")]}, 24, 500, 1232, 172),
        footer(5, "Defasagens não representam efeitos causais ou período de latência."),
    ]

    mun = field("analitico_contraste_municipio", "nome_municipio", "Column", "Município")
    p6 = header(6, "Como o contraste varia entre os municípios paulistas?", "Métricas intramunicipais com filtro fixo de ≥30 dias frios e ≥30 dias quentes; contraste percentual exibido junto ao volume.")
    p6 += [
        slicer(6, 4, "Município", field("dim_municipio", "Dados.nome_municipio", "Column", "Município"), 24, 112, 420, 56),
        textbox(6, 5, "Escala conceitual: azul = contraste negativo; neutro ≈ zero; laranja = positivo. Não indica melhor/pior.", 460, 112, 796, 56, size=9, font_color=MUTED, background=WHITE),
        visual(6, 6, "azureMap", "Contraste intramunicipal no território", {"Y": [field("analitico_contraste_municipio", "latitude_municipio", "Column", "Latitude")], "X": [field("analitico_contraste_municipio", "longitude_municipio", "Column", "Longitude")], "Legend": [field("analitico_contraste_municipio", "Classe Contraste", "Column")], "Size": [field("analitico_contraste_municipio", "Volume Internações Município")], "Tooltips": [mun, field("analitico_contraste_municipio", "Contraste Relativo Município"), field("analitico_contraste_municipio", "Média Frio Município"), field("analitico_contraste_municipio", "Média Quente Município"), field("analitico_contraste_municipio", "Diferença Absoluta Município"), field("analitico_contraste_municipio", "Dias Frios Município"), field("analitico_contraste_municipio", "Dias Quentes Município")]}, 24, 184, 620, 326, default_color=TEAL),
        visual(6, 7, "scatterChart", "Volume total × contraste relativo", {"Details": [mun], "X": [field("analitico_contraste_municipio", "Volume Internações Município")], "Y": [field("analitico_contraste_municipio", "Contraste Relativo Município")], "Tooltips": [field("analitico_contraste_municipio", "Média Frio Município"), field("analitico_contraste_municipio", "Média Quente Município"), field("analitico_contraste_municipio", "Dias Frios Município"), field("analitico_contraste_municipio", "Dias Quentes Município")]}, 660, 184, 596, 326, default_color=PURPLE),
        visual(6, 8, "matrix", "Ranking ordenável: contraste, diferença e volume", {"Rows": [mun], "Values": [field("analitico_contraste_municipio", "Contraste Relativo Município"), field("analitico_contraste_municipio", "Diferença Absoluta Município"), field("analitico_contraste_municipio", "Volume Internações Município"), field("analitico_contraste_municipio", "Média Frio Município"), field("analitico_contraste_municipio", "Média Quente Município"), field("analitico_contraste_municipio", "Dias Frios Município"), field("analitico_contraste_municipio", "Dias Quentes Município")]}, 24, 526, 1232, 146),
        footer(6, "Cada município é comparado consigo mesmo."),
    ]
    page6_filter = [equals_filter("analitico_contraste_municipio", "contraste_relativo_valido", 1)]

    return [
        make_page(1, "Temperatura e internações", p1),
        make_page(2, "Consistência ao longo do tempo", p2),
        make_page(3, "Diagnósticos / CID", p3),
        make_page(4, "Faixa etária e sexo", p4),
        make_page(5, "Defasagem temporal", p5),
        make_page(6, "Análise territorial", p6, page6_filter),
    ]


def main() -> int:
    report = json.loads(REPORT.read_text(encoding="utf-8-sig"))
    report["sections"] = build_pages()
    config = json.loads(report["config"])
    config["activeSectionIndex"] = 0
    report["config"] = json.dumps(config, ensure_ascii=False, separators=(",", ":"))
    REPORT.write_text(json.dumps(report, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Relatório reformulado: {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
