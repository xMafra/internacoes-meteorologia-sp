import sys

from pyspark.sql import SparkSession

sys.path.insert(
    0,
    "/home/jovyan/work",
)

from src.silver.sih import criar_spark, processar_sih


def obter_periodos() -> list[tuple[int, int]]:
    """
    Retorna os períodos que ainda precisam ser processados.

    Janeiro, fevereiro e março de 2023 já foram processados
    e validados anteriormente.
    """

    periodos = (
        [
            (2023, mes)
            for mes in range(4, 13)
        ]
        + [
            (ano, mes)
            for ano in range(2024, 2026)
            for mes in range(1, 13)
        ]
    )

    return periodos


def processar_lote(
    spark: SparkSession,
    periodos: list[tuple[int, int]],
) -> None:
    """
    Processa todos os períodos informados.

    Um erro em determinado mês não interrompe os períodos
    seguintes.
    """

    sucessos = []
    falhas = []

    total_periodos = len(periodos)

    print()
    print("=" * 70)
    print("PROCESSAMENTO EM LOTE - SILVER SIH")
    print("=" * 70)
    print(f"Total de períodos: {total_periodos}")
    print()

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
                (ano, mes, str(erro))
            )

            print(
                f"[ERRO] {ano}/{mes:02d}"
            )

            print(
                f"Motivo: {erro}"
            )

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
        f"Total esperado: {total_periodos}"
    )

    print(
        f"Total concluído: {len(sucessos)}"
    )

    print(
        f"Total com erro: {len(falhas)}"
    )

    print("=" * 70)

    if falhas:
        print()
        print(
            "ATENÇÃO: existem períodos com erro."
        )
        print(
            "Os períodos concluídos permanecem "
            "disponíveis na Silver."
        )
    else:
        print()
        print(
            "TODOS OS PERÍODOS FORAM PROCESSADOS "
            "COM SUCESSO."
        )


if __name__ == "__main__":
    spark = criar_spark()

    try:
        periodos = obter_periodos()

        processar_lote(
            spark,
            periodos,
        )

    finally:
        spark.stop()