from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# CONFIGURAÇÕES
# ============================================================

ARQUIVOS = {
    "capitulos": "CID-10-CAPITULOS.CSV",
    "grupos": "CID-10-GRUPOS.CSV",
    "categorias": "CID-10-CATEGORIAS.CSV",
    "subcategorias": "CID-10-SUBCATEGORIAS.CSV",
}


BASE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "bronze"
    / "cid10"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TCC-Validacao-Bronze-CID10")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

# IMPORTANTE:
# reduz drasticamente o log do Spark
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# LEITURA
# ============================================================

def ler_csv(nome_arquivo):

    caminho = str(BASE_PATH / nome_arquivo)

    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(caminho)
    )


# ============================================================
# LEITURA DOS ARQUIVOS
# ============================================================

dataframes = {}

for nome, arquivo in ARQUIVOS.items():

    caminho = BASE_PATH / arquivo

    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {caminho}"
        )

    dataframes[nome] = ler_csv(arquivo)


# ============================================================
# RESULTADOS DA VALIDAÇÃO
# ============================================================

resultados = []


for nome, df in dataframes.items():

    total = df.count()

    colunas = df.columns

    primeira_coluna = colunas[0]

    distintos = (
        df.select(primeira_coluna)
        .distinct()
        .count()
    )

    nulos = (
        df.filter(
            col(primeira_coluna).isNull()
            | (col(primeira_coluna) == "")
        )
        .count()
    )

    duplicados = total - distintos

    resultados.append(
        {
            "nome": nome.upper(),
            "total": total,
            "distintos": distintos,
            "nulos": nulos,
            "duplicados": duplicados,
            "colunas": colunas,
        }
    )


# ============================================================
# IMPRESSÃO FINAL
# ============================================================

print()
print("=" * 70)
print("VALIDAÇÃO BRONZE CID-10")
print("=" * 70)

print()
print(f"Diretório: {BASE_PATH}")

print()
print("=" * 70)
print("RESUMO")
print("=" * 70)

for resultado in resultados:

    print()
    print(resultado["nome"])

    print(
        f"Registros: {resultado['total']}"
    )

    print(
        f"Códigos distintos: {resultado['distintos']}"
    )

    print(
        f"Códigos nulos/vazios: {resultado['nulos']}"
    )

    print(
        f"Duplicidades: {resultado['duplicados']}"
    )

    print(
        f"Colunas: {resultado['colunas']}"
    )


# ============================================================
# AMOSTRAS IMPORTANTES
# ============================================================

print()
print("=" * 70)
print("AMOSTRAS DOS DADOS")
print("=" * 70)


for nome in [
    "capitulos",
    "grupos",
    "categorias",
    "subcategorias",
]:

    print()
    print(f"--- {nome.upper()} ---")

    df = dataframes[nome]

    # Coleta apenas 5 registros para não gerar
    # um volume desnecessário de saída.
    registros = df.limit(5).collect()

    for registro in registros:
        print(registro.asDict())


# ============================================================
# FINAL
# ============================================================

print()
print("=" * 70)
print("VALIDAÇÃO BRONZE CID-10 CONCLUÍDA")
print("=" * 70)

spark.stop()