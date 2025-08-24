# PreciosSuperPY

Automatización para generar diariamente un informe de evolución y **proyección** de precios por producto y supermercado, y publicarlo en GitHub Pages.

## Estructura esperada

```
SUPERSCRAP/
  *.csv   # con las columnas: FechaConsulta, Producto, Supermercado, Precio
docs/
  index.html  # generado automáticamente
.github/
  workflows/
    report.yml
generate_report.py
requirements.txt
```

> **CSV esperados**:  
> - `FechaConsulta`: fecha y hora de la consulta (parseable por pandas)  
> - `Producto`: nombre del producto  
> - `Supermercado`: nombre del supermercado  
> - `Precio`: valor numérico del precio (Gs)

## Ejecución local

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python generate_report.py
```

El resultado se escribirá en `docs/`. Si el repositorio tiene GitHub Pages habilitado desde **GitHub Actions**, se desplegará automáticamente con el workflow incluido.

## Programación

El workflow se ejecuta todos los días a las 09:00 PYT (≈ 12:00 UTC) y bajo demanda (workflow_dispatch).
