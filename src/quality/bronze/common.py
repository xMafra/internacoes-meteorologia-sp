from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[3]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"


@dataclass(frozen=True)
class Check:
    nome: str
    ok: bool
    detalhe: str
    arquivo: str | None = None

    def as_dict(self) -> dict:
        return {
            "nome": self.nome,
            "ok": self.ok,
            "detalhe": self.detalhe,
            "arquivo": self.arquivo,
        }


@dataclass
class Report:
    fonte: str
    checks: list[Check] = field(default_factory=list)

    @property
    def aprovado(self) -> bool:
        return all(check.ok for check in self.checks)

    def add(
        self,
        nome: str,
        ok: bool,
        detalhe: str,
        arquivo: Path | str | None = None,
    ) -> None:
        self.checks.append(
            Check(nome, ok, detalhe, str(arquivo) if arquivo else None)
        )

    def as_dict(self) -> dict:
        return {
            "fonte": self.fonte,
            "aprovado": self.aprovado,
            "total_checks": len(self.checks),
            "total_falhas": sum(not check.ok for check in self.checks),
            "checks": [check.as_dict() for check in self.checks],
        }


def sha256(caminho: Path) -> str:
    digest = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            digest.update(bloco)
    return digest.hexdigest()


def check_duplicados(report: Report, arquivos: Iterable[Path]) -> None:
    hashes: dict[str, list[Path]] = {}
    for caminho in arquivos:
        if caminho.is_file() and caminho.stat().st_size:
            hashes.setdefault(sha256(caminho), []).append(caminho)

    grupos = [grupo for grupo in hashes.values() if len(grupo) > 1]
    detalhe = (
        "; ".join(", ".join(str(item) for item in grupo) for grupo in grupos)
        if grupos
        else "nenhum conteúdo duplicado"
    )
    report.add("arquivos_duplicados", not grupos, detalhe)


def check_temporarios(report: Report, raiz: Path) -> None:
    temporarios = sorted(raiz.rglob("*.tmp")) if raiz.exists() else []
    report.add(
        "downloads_interrompidos",
        not temporarios,
        "nenhum arquivo temporário" if not temporarios else ", ".join(map(str, temporarios)),
    )


def print_report(report: Report) -> None:
    status = "APROVADO" if report.aprovado else "REPROVADO"
    print(f"DQ BRONZE {report.fonte.upper()}: {status}")
    for check in report.checks:
        marcador = "OK" if check.ok else "FALHA"
        alvo = f" [{check.arquivo}]" if check.arquivo else ""
        print(f"[{marcador}] {check.nome}{alvo}: {check.detalhe}")

