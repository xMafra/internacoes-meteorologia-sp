import sys

from pyspark.sql import SparkSession


# ============================================================
# CONFIGURAÇÃO DO PYTHON PATH
# ============================================================

sys.path.insert(
    0,
    "/home/jovyan/work",
)


# ============================================================
# IMPORTS DO PROJETO
# ============================================================

from src.silver.sih import criar_spark, processar_sih
from src.common.study_period import EXPECTED_SIH_FILES, SIH_COMPETENCES


# ============================================================
# PERÍODOS DO PIPELINE
# ============================================================

def obter_periodos() -> list[tuple[int, int]]:
    """
    Retorna todas as competências do SIH utilizadas no TCC.

    A janela inclui o recorte analítico e a cauda posterior necessária para
    capturar internações de 2025 processadas em 2026.
    """

    return list(SIH_COMPETENCES)


# ============================================================
# PROCESSAMENTO EM LOTE
# ============================================================

def processar_lote(
    spark: SparkSession,
    periodos: list[tuple[int, int]],
) -> None:
    """
    Processa todas as competências informadas.

    Caso um período apresente erro, os demais continuam sendo
    processados.

    Ao final, se pelo menos um período tiver falhado, uma exceção
    é lançada para que o Airflow marque a task como FAILED.
    """

    sucessos = []
    falhas = []

    total_periodos = len(periodos)

    print()
    print("=" * 70)
    print("PROCESSAMENTO EM LOTE - SILVER SIH")
    print("=" * 70)
    print(
        f"Total de períodos: "
        f"{total_periodos}"
    )
    print()

    # --------------------------------------------------------
    # PROCESSAMENTO DAS COMPETÊNCIAS
    # --------------------------------------------------------

    for indice, (ano, mes) in enumerate(
        periodos,
        start=1,
    ):

        print()
        print(
            f"[{indice}/{total_periodos}] "
            f"Processando {ano}/{mes:02d}"
        )

        try:

            processar_sih(
                ano=ano,
                mes=mes,
                spark=spark,
            )

            sucessos.append(
                (ano, mes)
            )

            print(
                f"[OK] {ano}/{mes:02d}"
            )

        except Exception as erro:

            falhas.append(
                (
                    ano,
                    mes,
                    str(erro),
                )
            )

            print(
                f"[ERRO] {ano}/{mes:02d}"
            )

            print(
                f"Motivo: {erro}"
            )

    # ========================================================
    # RESUMO FINAL
    # ========================================================

    print()
    print("=" * 70)
    print("RESUMO FINAL DO PROCESSAMENTO")
    print("=" * 70)

    print()
    print(
        f"Períodos processados com sucesso: "
        f"{len(sucessos)}"
    )

    for ano, mes in sucessos:

        print(
            f"  [OK] {ano}/{mes:02d}"
        )

    print()
    print(
        f"Períodos com erro: "
        f"{len(falhas)}"
    )

    for ano, mes, erro in falhas:

        print(
            f"  [ERRO] {ano}/{mes:02d}"
        )

        print(
            f"         {erro}"
        )

    print()

    print(
        f"Total esperado: "
        f"{total_periodos}"
    )

    print(
        f"Total concluído: "
        f"{len(sucessos)}"
    )

    print(
        f"Total com erro: "
        f"{len(falhas)}"
    )

    print("=" * 70)

    # ========================================================
    # VALIDAÇÃO FINAL
    # ========================================================

    if falhas:

        periodos_com_erro = ", ".join(
            f"{ano}/{mes:02d}"
            for ano, mes, _ in falhas
        )

        raise RuntimeError(
            "Falha no processamento da Silver SIH. "
            f"Períodos com erro: {periodos_com_erro}"
        )

    if len(sucessos) != total_periodos:

        raise RuntimeError(
            "Quantidade de períodos concluídos diferente "
            "da quantidade esperada. "
            f"Esperado={total_periodos}, "
            f"Concluído={len(sucessos)}."
        )

    print()
    print(
        "TODOS OS PERÍODOS FORAM PROCESSADOS "
        "COM SUCESSO."
    )

    print(
        f"Total de competências concluídas: "
        f"{len(sucessos)}"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = criar_spark()

    spark.sparkContext.setLogLevel(
        "ERROR"
    )

    try:

        periodos = obter_periodos()

        if len(periodos) != EXPECTED_SIH_FILES:

            raise RuntimeError(
                "Quantidade inesperada de competências SIH. "
                f"Esperado={EXPECTED_SIH_FILES}, obtido={len(periodos)}."
            )

        processar_lote(
            spark,
            periodos,
        )

    finally:

        spark.stop()
