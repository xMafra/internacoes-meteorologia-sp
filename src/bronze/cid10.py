from pathlib import Path
from zipfile import ZipFile


# ============================================================
# CONFIGURAÇÕES
# ============================================================

NOME_ARQUIVO_ZIP = "CID10CSV.zip"

ARQUIVOS_ESPERADOS = [
    "CID-10-CAPITULOS.CSV",
    "CID-10-GRUPOS.CSV",
    "CID-10-CATEGORIAS.CSV",
    "CID-10-SUBCATEGORIAS.CSV",
]


# ============================================================
# CAMINHOS
# ============================================================

def obter_diretorio_bronze() -> Path:
    """
    Retorna o diretório da Bronze da CID-10.

    Estrutura esperada:

    projeto/
    ├── data/
    │   └── bronze/
    │       └── cid10/
    │           └── CID10CSV.zip
    └── src/
        └── bronze/
            └── cid10.py
    """

    return (
        Path(__file__).resolve().parents[2]
        / "data"
        / "bronze"
        / "cid10"
    )


def obter_caminho_zip() -> Path:
    """
    Retorna o caminho completo do arquivo CID10CSV.zip.
    """

    return obter_diretorio_bronze() / NOME_ARQUIVO_ZIP


# ============================================================
# EXTRAÇÃO
# ============================================================

def extrair_cid10(caminho_zip: Path, diretorio_saida: Path) -> list[Path]:
    """
    Extrai os arquivos CSV da CID-10.

    Os arquivos são extraídos diretamente para:

    data/bronze/cid10/
    """

    print(f"Arquivo encontrado: {caminho_zip}")
    print(f"Tamanho: {caminho_zip.stat().st_size / 1024:.2f} KB")

    diretorio_saida.mkdir(
        parents=True,
        exist_ok=True
    )

    with ZipFile(caminho_zip, "r") as zip_ref:

        arquivos_zip = zip_ref.namelist()

        print()
        print("Arquivos encontrados no ZIP:")

        for arquivo in arquivos_zip:
            print(f" - {arquivo}")

        nomes_arquivos = {
            Path(arquivo).name.upper(): arquivo
            for arquivo in arquivos_zip
            if not arquivo.endswith("/")
        }

        arquivos_extraidos = []

        print()
        print("Extraindo arquivos esperados...")

        for nome_esperado in ARQUIVOS_ESPERADOS:

            nome_normalizado = nome_esperado.upper()

            if nome_normalizado not in nomes_arquivos:

                raise FileNotFoundError(
                    f"Arquivo esperado não encontrado no ZIP: "
                    f"{nome_esperado}"
                )

            nome_original = nomes_arquivos[nome_normalizado]

            destino = diretorio_saida / nome_esperado

            with zip_ref.open(nome_original) as arquivo_origem:
                with open(destino, "wb") as arquivo_destino:
                    arquivo_destino.write(
                        arquivo_origem.read()
                    )

            arquivos_extraidos.append(destino)

            print(f" - {nome_esperado}")

    return arquivos_extraidos


# ============================================================
# PROCESSAMENTO
# ============================================================

def processar_cid10() -> list[Path]:
    """
    Executa o processamento completo da Bronze CID-10.
    """

    print("=" * 70)
    print("INICIANDO BRONZE CID-10")
    print("=" * 70)

    diretorio_bronze = obter_diretorio_bronze()
    caminho_zip = obter_caminho_zip()

    print()
    print(f"Diretório Bronze: {diretorio_bronze}")
    print(f"Arquivo ZIP: {caminho_zip}")

    arquivos_extraidos = extrair_cid10(
        caminho_zip=caminho_zip,
        diretorio_saida=diretorio_bronze,
    )

    print()
    print("=" * 70)
    print("RESUMO BRONZE CID-10")
    print("=" * 70)

    print(f"Arquivo fonte: {caminho_zip.name}")
    print("Arquivos extraídos:")

    for arquivo in arquivos_extraidos:
        print(f" - {arquivo.name}")

    print()
    print(f"Saída: {diretorio_bronze}")
    print()
    print("BRONZE CID-10 CONCLUÍDA")
    print("=" * 70)

    return arquivos_extraidos


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    processar_cid10()
