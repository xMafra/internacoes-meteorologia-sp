from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


Level = Literal["ERROR", "WARNING", "INFO"]


@dataclass(frozen=True)
class ValidationResult:
    nome: str
    passou: bool
    detalhe: str
    nivel: Level = "ERROR"
    metrica: float | int | str | None = None

    def as_dict(self) -> dict:
        return {
            "nome": self.nome,
            "passou": self.passou,
            "nivel": self.nivel,
            "detalhe": self.detalhe,
            "metrica": self.metrica,
        }


@dataclass
class QualityReport:
    dataset: str
    resultados: list[ValidationResult] = field(default_factory=list)

    @property
    def aprovado(self) -> bool:
        return not any(not item.passou and item.nivel == "ERROR" for item in self.resultados)

    def add(self, nome: str, passou: bool, detalhe: str, nivel: Level = "ERROR", metrica=None) -> None:
        self.resultados.append(ValidationResult(nome, passou, detalhe, nivel, metrica))

    def as_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "status": "PASS" if self.aprovado else "FAIL",
            "erros": sum(not r.passou and r.nivel == "ERROR" for r in self.resultados),
            "warnings": sum(not r.passou and r.nivel == "WARNING" for r in self.resultados),
            "resultados": [r.as_dict() for r in self.resultados],
        }

    def exigir_aprovacao(self) -> None:
        if not self.aprovado:
            falhas = "; ".join(r.nome for r in self.resultados if not r.passou and r.nivel == "ERROR")
            raise ValueError(f"Falha no Data Quality de {self.dataset}: {falhas}")


def imprimir_relatorio(report: QualityReport) -> None:
    print("=" * 60)
    print(f"DATA QUALITY — {report.dataset.upper()}")
    print("=" * 60)
    for item in report.resultados:
        status = "INFO" if item.nivel == "INFO" else ("PASS" if item.passou else item.nivel)
        print(f"[{status}] {item.nome}: {item.detalhe}")
    print(f"\nSTATUS FINAL: {'PASS' if report.aprovado else 'FAIL'}")
    print("=" * 60)


def validar_colunas_obrigatorias(df: DataFrame, colunas: Iterable[str], report: QualityReport) -> bool:
    faltantes = sorted(set(colunas) - set(df.columns))
    report.add("Schema obrigatório", not faltantes, "todas presentes" if not faltantes else f"ausentes={faltantes}")
    return not faltantes


def validar_tipos(df: DataFrame, tipos: dict[str, str], report: QualityReport) -> None:
    atuais = {campo.name: campo.dataType.simpleString() for campo in df.schema.fields}
    divergentes = {nome: (tipo, atuais.get(nome)) for nome, tipo in tipos.items() if atuais.get(nome) != tipo}
    report.add("Tipos das colunas", not divergentes, "tipos esperados" if not divergentes else f"divergentes={divergentes}")


def validar_quantidade_registros(df: DataFrame, esperado: int, report: QualityReport, nivel: Level = "ERROR") -> int:
    total = df.count()
    report.add("Quantidade de registros", total == esperado, f"encontrado={total:,}, esperado={esperado:,}", nivel, total)
    return total


def validar_quantidade_distinta(df: DataFrame, coluna: str, esperado: int, report: QualityReport, nome: str | None = None) -> int:
    total = df.select(coluna).distinct().count()
    report.add(nome or f"Valores distintos de {coluna}", total == esperado, f"encontrado={total}, esperado={esperado}", metrica=total)
    return total


def validar_chave_unica(df: DataFrame, colunas: list[str], report: QualityReport) -> int:
    duplicados = df.groupBy(*colunas).count().filter(F.col("count") > 1).count()
    report.add(f"Chave {' + '.join(colunas)}", duplicados == 0, f"grupos duplicados={duplicados}", metrica=duplicados)
    return duplicados


def validar_nulos(df: DataFrame, colunas: Iterable[str], report: QualityReport, nivel: Level = "ERROR") -> int:
    expressoes = [F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c) for c in colunas]
    linha = df.agg(*expressoes).first().asDict()
    total = sum(linha.values())
    report.add(f"Nulos/vazios em {', '.join(colunas)}", total == 0, f"por_coluna={linha}", nivel, total)
    return total


def validar_intervalo(df: DataFrame, coluna: str, minimo, maximo, report: QualityReport, nivel: Level = "ERROR") -> int:
    invalidos = df.filter(F.col(coluna).isNotNull() & ((F.col(coluna) < minimo) | (F.col(coluna) > maximo))).count()
    report.add(f"Intervalo de {coluna}", invalidos == 0, f"fora_de_[{minimo}, {maximo}]={invalidos}", nivel, invalidos)
    return invalidos


def validar_valores_aceitos(df: DataFrame, coluna: str, aceitos: Iterable, report: QualityReport) -> int:
    valores = list(aceitos)
    invalidos = df.filter(F.col(coluna).isNull() | ~F.col(coluna).isin(valores)).count()
    report.add(f"Domínio de {coluna}", invalidos == 0, f"inválidos={invalidos}, aceitos={valores}", metrica=invalidos)
    return invalidos


def validar_periodo(df: DataFrame, coluna: str, inicio, fim, report: QualityReport) -> tuple:
    linha = df.agg(F.min(coluna).alias("inicio"), F.max(coluna).alias("fim")).first()
    atual = (str(linha["inicio"]), str(linha["fim"]))
    esperado = (str(inicio), str(fim))
    report.add(f"Período de {coluna}", atual == esperado, f"encontrado={atual[0]} a {atual[1]}, esperado={esperado[0]} a {esperado[1]}")
    return atual


def validar_condicao(df: DataFrame, condicao_invalida, nome: str, report: QualityReport, nivel: Level = "ERROR") -> int:
    invalidos = df.filter(condicao_invalida).count()
    report.add(nome, invalidos == 0, f"registros inválidos={invalidos}", nivel, invalidos)
    return invalidos


def registrar_cobertura(df: DataFrame, colunas: Iterable[str], report: QualityReport) -> None:
    total = df.count()
    linha = df.agg(*[F.count(c).alias(c) for c in colunas]).first().asDict()
    for coluna, validos in linha.items():
        percentual = 100.0 * validos / total if total else 0.0
        report.add(f"Cobertura {coluna}", True, f"{percentual:.2f}% ({validos:,}/{total:,})", "INFO", percentual)
