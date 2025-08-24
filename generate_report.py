# -*- coding: utf-8 -*-
import os
import glob
import logging
from datetime import datetime, timedelta
import re

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from sklearn.linear_model import LinearRegression

# ---------------------------------------------------------------------
# Configuración de logging (sin prints)
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ReportGenerator")

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------
def _slugify_filename(name: str) -> str:
    """
    Convierte un nombre arbitrario a un nombre de archivo seguro.
    """
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    # quitar cualquier caracter no alfanumérico, guion o subrayado
    name = re.sub(r"[^a-zA-Z0-9_\-]", "", name)
    return name[:200] if len(name) > 200 else name

def _ensure_docs_placeholder(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    index_path = os.path.join(output_dir, "index.html")
    if not os.path.exists(index_path):
        with open(index_path, "w", encoding="utf-8") as f:
            f.write("<!doctype html><meta charset='utf-8'><title>PreciosSuperPY</title>"
                    "<p>No hay datos disponibles aún.</p>")

class PriceReportGenerator:
    def __init__(self, data_dir: str = "SUPERSCRAP", output_dir: str = "docs"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    # --------------------------------------------------------------
    # Carga de datos
    # --------------------------------------------------------------
    def load_data(self) -> pd.DataFrame | None:
        csv_files = glob.glob(os.path.join(self.data_dir, "*.csv"))
        if not csv_files:
            logger.warning("No se encontraron archivos CSV en %s", self.data_dir)
            return None

        dfs = []
        for fp in csv_files:
            try:
                # Intento robusto de lectura (utf-8 y fallback a latin-1)
                try:
                    df = pd.read_csv(fp, parse_dates=["FechaConsulta"])
                except Exception:
                    df = pd.read_csv(fp, parse_dates=["FechaConsulta"], encoding="latin-1")
                # Normalización de columnas mínimas requeridas
                expected = {"FechaConsulta", "Producto", "Supermercado", "Precio"}
                missing = expected - set(df.columns)
                if missing:
                    logger.warning("Archivo %s omitido (faltan columnas: %s)", fp, ", ".join(sorted(missing)))
                    continue

                # Coerción de Precio a numérico
                df["Precio"] = pd.to_numeric(df["Precio"], errors="coerce")
                df = df.dropna(subset=["Precio", "FechaConsulta", "Producto", "Supermercado"])
                # Filtrar precios no positivos
                df = df[df["Precio"] > 0]
                dfs.append(df)
                logger.info("Cargado %s con %d registros útiles", fp, len(df))
            except Exception as e:
                logger.exception("Error cargando %s: %s", fp, e)

        if not dfs:
            return None
        return pd.concat(dfs, ignore_index=True)

    # --------------------------------------------------------------
    # Preparación de datos
    # --------------------------------------------------------------
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # Requiere historial mínimo por producto
        counts = df["Producto"].value_counts()
        popular = counts[counts > 5].index
        df = df[df["Producto"].isin(popular)].copy()

        # Fecha diaria
        df["Fecha"] = pd.to_datetime(df["FechaConsulta"].dt.date)

        # Precio promedio por fecha, producto y supermercado
        daily = (
            df.groupby(["Fecha", "Producto", "Supermercado"], as_index=False)["Precio"]
              .mean()
              .sort_values(["Producto", "Supermercado", "Fecha"])
        )
        return daily

    # --------------------------------------------------------------
    # Proyección lineal simple (por producto, no por supermercado)
    # --------------------------------------------------------------
    def generate_projection(self, data: pd.DataFrame, product_name: str, days: int = 7):
        prod = data[data["Producto"] == product_name].copy()
        if len(prod) < 3:
            return None

        # Agregar por día para el producto (promedio entre supermercados)
        per_day = prod.groupby("Fecha", as_index=False)["Precio"].mean().sort_values("Fecha")
        if per_day["Fecha"].nunique() < 3:
            return None

        # Regressión lineal sobre ordinal de fecha
        per_day["FechaOrdinal"] = per_day["Fecha"].map(datetime.toordinal)
        X = per_day["FechaOrdinal"].to_numpy().reshape(-1, 1)
        y = per_day["Precio"].to_numpy()

        model = LinearRegression()
        model.fit(X, y)

        last_date = per_day["Fecha"].max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, days + 1)]
        future_ord = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)
        future_prices = model.predict(future_ord)

        return list(zip(future_dates, future_prices))

    # --------------------------------------------------------------
    # Gráfico por producto (líneas por supermercado + proyección)
    # --------------------------------------------------------------
    def create_price_evolution_chart(self, data: pd.DataFrame, product_name: str, output_path: str) -> bool:
        prod = data[data["Producto"] == product_name].copy()
        if prod.empty:
            return False

        plt.figure(figsize=(12, 6))

        for market in prod["Supermercado"].dropna().unique():
            m = prod[prod["Supermercado"] == market]
            if m.empty:
                continue
            plt.plot(m["Fecha"], m["Precio"], marker="o", linewidth=2, label=str(market))

        proj = self.generate_projection(data, product_name)
        if proj:
            dates, prices = zip(*proj)
            plt.plot(dates, prices, linestyle="--", linewidth=2, label="Proyección")
            # anotaciones
            for d, p in proj:
                plt.annotate(f"{p:.0f}", (d, p), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

        plt.title(f"Evolución de Precios: {product_name}")
        plt.xlabel("Fecha")
        plt.ylabel("Precio (Gs)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))

        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close()
        return True

    # --------------------------------------------------------------
    # HTML (index) con los gráficos Top-N
    # --------------------------------------------------------------
    def create_html_report(self, data: pd.DataFrame, top_n: int = 20) -> None:
        counts = data["Producto"].value_counts()
        top_products = list(counts.index[:top_n])

        cards = []
        for prod in top_products:
            fname = _slugify_filename(prod) + ".png"
            fpath = os.path.join(self.output_dir, fname)
            ok = self.create_price_evolution_chart(data, prod, fpath)
            if ok:
                cards.append((prod, fname))

        last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html = [
            "<!doctype html>",
            "<html lang='es'><head><meta charset='utf-8'>",
            "<meta name='viewport' content='width=device-width,initial-scale=1'>",
            "<title>Reporte de Evolución de Precios</title>",
            "<style>",
            "body{font-family:Arial,Helvetica,sans-serif;background:#f5f5f5;margin:0;padding:1rem}",
            ".wrap{max-width:1100px;margin:0 auto}",
            ".header{background:#2c3e50;color:#fff;padding:1.25rem 1.5rem;border-radius:10px}",
            ".grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px;margin-top:16px}",
            ".card{background:#fff;border-radius:10px;box-shadow:0 2px 6px rgba(0,0,0,.1);padding:12px}",
            ".title{color:#2c3e50;font-size:18px;margin:0 0 8px}",
            ".muted{color:#7f8c8d}",
            "img{max-width:100%;height:auto;border:1px solid #ddd;border-radius:6px}",
            "</style></head><body><div class='wrap'>",
            "<div class='header'><h1>Reporte de Evolución de Precios</h1>",
            "<p class='muted'>Análisis de precios de supermercados con proyecciones</p>",
            f"<p class='muted'>Última actualización: {last_update}</p>",
            "</div><div class='grid'>"
        ]

        for prod, fname in cards:
            html.append("<div class='card'>")
            html.append(f"<h2 class='title'>{prod}</h2>")
            html.append(f"<img src='{fname}' alt='Evolución de precios para {prod}'>")
            html.append("</div>")

        html.extend(["</div></div></body></html>"])
        with open(os.path.join(self.output_dir, "index.html"), "w", encoding="utf-8") as f:
            f.write("\n".join(html))
        logger.info("Reporte HTML generado con %d gráficos", len(cards))

    # --------------------------------------------------------------
    # Pipeline completo
    # --------------------------------------------------------------
    def generate_report(self) -> bool:
        _ensure_docs_placeholder(self.output_dir)
        df = self.load_data()
        if df is None or df.empty:
            logger.warning("Sin datos: se mantiene página de marcador de posición.")
            return False

        data = self.prepare_data(df)
        if data is None or data.empty:
            logger.warning("Datos insuficientes para análisis.")
            return False

        self.create_html_report(data, top_n=20)
        logger.info("Reporte generado en %s", os.path.abspath(self.output_dir))
        return True


if __name__ == "__main__":
    gen = PriceReportGenerator()
    ok = gen.generate_report()
    # No prints (solo logging). Si no hay datos, el placeholder queda activo.
