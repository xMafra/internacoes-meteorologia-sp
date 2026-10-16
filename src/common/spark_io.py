"""Operações de escrita Spark seguras em volumes montados.

O Hadoop pode falhar ao limpar diretórios Parquet existentes em bind mounts
do Windows. A saída é gravada em staging e só substitui a versão publicada
depois que a escrita termina com sucesso.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import stat
from typing import Iterator
from uuid import uuid4


def _remover_diretorio(caminho: Path) -> None:
    if caminho.exists():
        def remover_somente_leitura(func, nome, _erro):
            os.chmod(nome, stat.S_IWRITE)
            func(nome)

        shutil.rmtree(caminho, onerror=remover_somente_leitura)


def _remover_ou_reter(caminho: Path) -> None:
    """Remove o derivado ou o retém com nome único se o mount negar unlink."""
    try:
        _remover_diretorio(caminho)
    except PermissionError:
        retido = caminho.with_name(
            f"{caminho.name}.__retido__.{uuid4().hex}"
        )
        caminho.rename(retido)
        print(
            "AVISO: o volume não permitiu apagar a versão anterior; "
            f"ela foi retida em {retido}."
        )


@contextmanager
def destino_parquet_atomico(caminho: str | Path) -> Iterator[str]:
    """Fornece um staging e publica-o com rollback da versão anterior."""

    destino = Path(caminho)
    staging = destino.with_name(f"{destino.name}.__staging__")
    anterior = destino.with_name(f"{destino.name}.__anterior__")

    _remover_ou_reter(staging)
    _remover_ou_reter(anterior)
    destino.parent.mkdir(parents=True, exist_ok=True)

    try:
        yield str(staging)

        if not staging.exists():
            raise RuntimeError(
                f"A escrita não produziu o staging esperado: {staging}"
            )

        if destino.exists():
            destino.rename(anterior)

        try:
            staging.rename(destino)
        except Exception:
            if anterior.exists() and not destino.exists():
                anterior.rename(destino)
            raise

        _remover_ou_reter(anterior)
    except Exception:
        _remover_ou_reter(staging)
        raise
