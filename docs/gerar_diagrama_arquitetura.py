from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


SAIDA = Path(__file__).resolve().parent / "assets" / "arquitetura_pipeline.png"
SAIDA.parent.mkdir(parents=True, exist_ok=True)

fig, ax = plt.subplots(figsize=(16, 9), dpi=180)
ax.set_xlim(0, 16)
ax.set_ylim(0, 9)
ax.axis("off")
fig.patch.set_facecolor("#f7f9fc")
ax.set_facecolor("#f7f9fc")


def caixa(x, y, w, h, titulo, texto, cor):
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.03,rounding_size=0.12",
        facecolor=cor, edgecolor="#263238", linewidth=1.2,
    )
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h - 0.28, titulo, ha="center", va="top", fontsize=11, weight="bold")
    ax.text(x + w / 2, y + h / 2 - 0.1, texto, ha="center", va="center", fontsize=8.5, linespacing=1.35)


def seta(x1, y1, x2, y2, estilo="-"):
    ax.add_patch(FancyArrowPatch(
        (x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=13,
        linewidth=1.4, color="#455a64", linestyle=estilo,
    ))


caixa(0.3, 5.5, 2.1, 2.4, "FONTES PÚBLICAS", "DATASUS / SIH\nINMET\nIBGE\nCID-10", "#e3f2fd")
caixa(3.0, 5.5, 2.5, 2.4, "BRONZE", "Arquivos brutos\nDBC • ZIP • CSV\nPreservação da origem", "#d7ccc8")
caixa(6.2, 5.5, 2.5, 2.4, "SILVER", "SIH tratado\nIBGE padronizado\nCID-10 hierárquica\nINMET horário e diário", "#cfd8dc")
caixa(9.4, 5.5, 2.7, 2.4, "GOLD", "municipio_estacao\nfato_internacao\nfato_internacao_meteorologia", "#fff3cd")
caixa(12.8, 5.5, 2.7, 2.4, "CONSUMO", "Power BI / análises\nCamada analítica pronta\npara visualização", "#dcedc8")

seta(2.4, 6.7, 3.0, 6.7)
seta(5.5, 6.7, 6.2, 6.7)
seta(8.7, 6.7, 9.4, 6.7)
seta(12.1, 6.7, 12.8, 6.7)

caixa(3.0, 2.4, 2.5, 1.7, "DQ BRONZE", "Chegada • integridade\nlegibilidade • completude\nduplicidade de arquivos", "#efebe9")
caixa(6.2, 2.4, 2.5, 1.7, "DQ SILVER", "Schema • tipos • chaves\ndomínios • padronização\ncobertura como métrica", "#eceff1")
caixa(9.4, 2.4, 2.7, 1.7, "DQ GOLD", "Cardinalidade • integridade\ndos joins • granularidade\ncobertura analítica", "#fff8e1")

seta(4.25, 5.5, 4.25, 4.1)
seta(7.45, 5.5, 7.45, 4.1)
seta(10.75, 5.5, 10.75, 4.1)

caixa(4.9, 0.35, 6.2, 1.1, "ORQUESTRAÇÃO — PRÓXIMA ETAPA", "Airflow: ingestão → DQ Bronze → Silver → DQ Silver → Gold → DQ Gold", "#ede7f6")
seta(4.25, 2.4, 5.5, 1.45, ":")
seta(7.45, 2.4, 7.75, 1.45, ":")
seta(10.75, 2.4, 10.5, 1.45, ":")

ax.text(8, 8.65, "Arquitetura completa do pipeline de dados do TCC", ha="center", fontsize=17, weight="bold", color="#263238")
ax.text(8, 8.25, "Arquitetura Medallion com Data Quality transversal e execução em Spark/Docker", ha="center", fontsize=10.5, color="#546e7a")

plt.tight_layout()
fig.savefig(SAIDA, bbox_inches="tight", facecolor=fig.get_facecolor())
print(SAIDA)
