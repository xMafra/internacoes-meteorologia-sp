from pathlib import Path


def listar_arquivos_sih(
    ano_inicio: int,
    mes_inicio: int,
    ano_fim: int,
    mes_fim: int,
) -> list[Path]:
    """
    Lista os arquivos SIH presentes na camada Bronze
    dentro do período informado.

    A função procura pelos arquivos RDSP*.dbc organizados
    por ano e retorna os caminhos encontrados.
    """

    raiz_bronze = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "sih"
    )

    arquivos = []

    ano = ano_inicio
    mes = mes_inicio

    while (ano, mes) <= (ano_fim, mes_fim):
        nome_arquivo = f"RDSP{str(ano)[-2:]}{mes:02d}.dbc"
        caminho = raiz_bronze / str(ano) / nome_arquivo

        if caminho.exists():
            arquivos.append(caminho)

        if mes == 12:
            ano += 1
            mes = 1
        else:
            mes += 1

    return arquivos


def validar_completude_sih(
    ano_inicio: int,
    mes_inicio: int,
    ano_fim: int,
    mes_fim: int,
) -> dict:
    """
    Valida a completude dos arquivos SIH na camada Bronze.

    Compara a quantidade de arquivos esperados com a quantidade
    de arquivos encontrados no período informado e identifica
    quais arquivos estão ausentes.
    """

    arquivos = listar_arquivos_sih(
        ano_inicio,
        mes_inicio,
        ano_fim,
        mes_fim,
    )

    esperados = []

    ano = ano_inicio
    mes = mes_inicio

    while (ano, mes) <= (ano_fim, mes_fim):
        nome_arquivo = f"RDSP{str(ano)[-2:]}{mes:02d}.dbc"

        esperados.append(
            Path(
                "data",
                "bronze",
                "sih",
                str(ano),
                nome_arquivo,
            )
        )

        if mes == 12:
            ano += 1
            mes = 1
        else:
            mes += 1

    encontrados = {
        arquivo.relative_to(
            Path(__file__).resolve().parents[2]
        )
        for arquivo in arquivos
    }

    esperados_set = set(esperados)

    ausentes = sorted(esperados_set - encontrados)

    return {
        "total_esperado": len(esperados),
        "total_encontrado": len(arquivos),
        "total_ausente": len(ausentes),
        "arquivos_ausentes": ausentes,
        "completo": len(ausentes) == 0,
    }