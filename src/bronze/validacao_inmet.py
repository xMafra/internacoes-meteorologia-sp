from pathlib import Path


def listar_arquivos_inmet(
    ano_inicio: int,
    ano_fim: int,
) -> list[Path]:
    """
    Lista os arquivos ZIP do INMET presentes na camada Bronze.

    A função percorre o período informado e retorna os arquivos
    anuais encontrados na estrutura data/bronze/inmet/.
    """

    raiz_bronze = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "inmet"
    )

    arquivos = []

    for ano in range(ano_inicio, ano_fim + 1):
        caminho = raiz_bronze / str(ano) / f"{ano}.zip"

        if caminho.exists():
            arquivos.append(caminho)

    return arquivos


def validar_completude_inmet(
    ano_inicio: int,
    ano_fim: int,
) -> dict:
    """
    Valida a completude dos arquivos anuais do INMET na Bronze.

    Compara os anos esperados com os anos encontrados e identifica
    quais arquivos estão ausentes.
    """

    arquivos = listar_arquivos_inmet(
        ano_inicio,
        ano_fim,
    )

    anos_esperados = set(range(ano_inicio, ano_fim + 1))

    anos_encontrados = {
        int(arquivo.parent.name)
        for arquivo in arquivos
    }

    anos_ausentes = sorted(
        anos_esperados - anos_encontrados
    )

    return {
        "total_esperado": len(anos_esperados),
        "total_encontrado": len(arquivos),
        "total_ausente": len(anos_ausentes),
        "anos_ausentes": anos_ausentes,
        "completo": len(anos_ausentes) == 0,
    }