"""Validação sintática sem criar arquivos ``__pycache__``."""

import ast
from pathlib import Path


ROOTS = (
    Path("/home/jovyan/work/src"),
    Path("/opt/airflow/dags"),
    Path("/home/jovyan/work/tests"),
)


def main():
    arquivos = [arquivo for raiz in ROOTS for arquivo in raiz.rglob("*.py")]
    for arquivo in arquivos:
        ast.parse(arquivo.read_text(encoding="utf-8"), filename=str(arquivo))
    print(f"AST_OK={len(arquivos)}")


if __name__ == "__main__":
    main()
