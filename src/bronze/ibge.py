from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


URL_MALHA_SP_2022 = (
    "https://geoftp.ibge.gov.br/"
    "organizacao_do_territorio/malhas_territoriais/"
    "malhas_municipais/municipio_2022/UFs/SP/"
    "SP_Municipios_2022.zip"
)

USER_AGENT = "Mozilla/5.0"


def download_malha_sp_2022() -> Path:
    """
    Baixa a malha municipal de São Paulo de 2022 do IBGE.

    O arquivo ZIP original é preservado na camada Bronze.
    Caso o arquivo já exista localmente, o download não é repetido.
    """

    destino = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "ibge"
        / "2022"
        / "SP_Municipios_2022.zip"
    )

    destino.parent.mkdir(parents=True, exist_ok=True)

    if destino.exists():
        print(f"Arquivo já existe: {destino}")
        return destino

    requisicao = Request(
        URL_MALHA_SP_2022,
        headers={"User-Agent": USER_AGENT},
    )

    temporario = destino.with_suffix(".zip.tmp")

    print(f"Baixando: {URL_MALHA_SP_2022}")

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
            "Não foi possível baixar a malha municipal de "
            "São Paulo de 2022 do IBGE."
        ) from erro

    except Exception:
        if temporario.exists():
            temporario.unlink()

        raise

    print(f"Arquivo salvo em: {destino}")

    return destino