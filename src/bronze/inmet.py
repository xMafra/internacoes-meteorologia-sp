from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos"

USER_AGENT = "Mozilla/5.0"


def download_inmet(ano: int) -> Path:
    """
    Baixa o arquivo anual de dados históricos das estações
    automáticas do INMET.

    O arquivo ZIP original é preservado na camada Bronze,
    organizado por ano.

    Caso o arquivo já exista localmente, o download não é repetido.
    """

    if not isinstance(ano, int):
        raise TypeError("O ano deve ser um inteiro.")

    if ano < 2000:
        raise ValueError("O ano deve ser maior ou igual a 2000.")

    nome_arquivo = f"{ano}.zip"

    destino = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "inmet"
        / str(ano)
        / nome_arquivo
    )

    destino.parent.mkdir(parents=True, exist_ok=True)

    if destino.exists():
        print(f"Arquivo já existe: {destino}")
        return destino

    url = f"{BASE_URL}/{nome_arquivo}"

    temporario = destino.with_suffix(".zip.tmp")

    requisicao = Request(
        url,
        headers={"User-Agent": USER_AGENT},
    )

    print(f"Baixando: {url}")

    try:
        with urlopen(requisicao) as resposta:
            with open(temporario, "wb") as arquivo:
                while True:
                    bloco = resposta.read(1024 * 1024)

                    if not bloco:
                        break

                    arquivo.write(bloco)

        temporario.replace(destino)

    except URLError as erro:
        if temporario.exists():
            temporario.unlink()

        raise RuntimeError(
            f"Não foi possível baixar o arquivo {nome_arquivo} "
            f"do INMET."
        ) from erro

    except Exception:
        if temporario.exists():
            temporario.unlink()

        raise

    print(f"Arquivo salvo em: {destino}")

    return destino