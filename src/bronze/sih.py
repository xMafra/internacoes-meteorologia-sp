from pathlib import Path
from urllib.error import URLError
from urllib.request import urlretrieve


BASE_URL = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados"


def download_sih(ano: int, mes: int) -> Path:
    """
    Baixa o arquivo mensal de AIH Reduzida (RD) do SIH-SP
    disponibilizado pelo DATASUS.

    O arquivo é salvo na camada Bronze, organizado por ano.
    Caso o arquivo já exista localmente, o download não é repetido.
    """

    if not isinstance(ano, int):
        raise TypeError("O ano deve ser um inteiro.")

    if not isinstance(mes, int):
        raise TypeError("O mês deve ser um inteiro.")

    if ano < 2008:
        raise ValueError("O ano deve ser maior ou igual a 2008.")

    if mes < 1 or mes > 12:
        raise ValueError("O mês deve estar entre 1 e 12.")

    ano_2_digitos = str(ano)[-2:]
    mes_2_digitos = f"{mes:02d}"

    nome_arquivo = f"RDSP{ano_2_digitos}{mes_2_digitos}.dbc"

    destino = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "sih"
        / str(ano)
        / nome_arquivo
    )

    destino.parent.mkdir(parents=True, exist_ok=True)

    if destino.exists():
        print(f"Arquivo já existe: {destino}")
        return destino

    url = f"{BASE_URL}/{nome_arquivo}"
    temporario = destino.with_suffix(".dbc.tmp")

    print(f"Baixando: {url}")

    try:
        urlretrieve(url, temporario)
        temporario.replace(destino)

    except URLError as erro:
        if temporario.exists():
            temporario.unlink()

        raise RuntimeError(
            f"Não foi possível baixar o arquivo {nome_arquivo} "
            f"do DATASUS."
        ) from erro

    except Exception:
        if temporario.exists():
            temporario.unlink()

        raise

    print(f"Arquivo salvo em: {destino}")

    return destino


def download_sih_periodo(
    ano_inicio: int,
    mes_inicio: int,
    ano_fim: int,
    mes_fim: int,
) -> list[Path]:
    """
    Baixa todos os arquivos mensais do SIH-SP dentro de um período.

    O período é inclusivo, ou seja, tanto o mês inicial quanto
    o mês final são processados.

    A função reutiliza download_sih() para cada mês, mantendo
    a lógica de download e validação centralizada em uma única função.
    """

    if not isinstance(ano_inicio, int):
        raise TypeError("O ano inicial deve ser um inteiro.")

    if not isinstance(mes_inicio, int):
        raise TypeError("O mês inicial deve ser um inteiro.")

    if not isinstance(ano_fim, int):
        raise TypeError("O ano final deve ser um inteiro.")

    if not isinstance(mes_fim, int):
        raise TypeError("O mês final deve ser um inteiro.")

    if mes_inicio < 1 or mes_inicio > 12:
        raise ValueError("O mês inicial deve estar entre 1 e 12.")

    if mes_fim < 1 or mes_fim > 12:
        raise ValueError("O mês final deve estar entre 1 e 12.")

    if ano_inicio < 2008:
        raise ValueError("O ano inicial deve ser maior ou igual a 2008.")

    if ano_fim < 2008:
        raise ValueError("O ano final deve ser maior ou igual a 2008.")

    periodo_inicio = (ano_inicio, mes_inicio)
    periodo_fim = (ano_fim, mes_fim)

    if periodo_inicio > periodo_fim:
        raise ValueError(
            "O período inicial não pode ser posterior ao período final."
        )

    arquivos = []

    ano = ano_inicio
    mes = mes_inicio

    while (ano, mes) <= periodo_fim:
        caminho = download_sih(ano, mes)
        arquivos.append(caminho)

        if mes == 12:
            ano += 1
            mes = 1
        else:
            mes += 1

    return arquivos