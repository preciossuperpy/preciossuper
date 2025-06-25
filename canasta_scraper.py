#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
canasta_scraper.py — Scraper unificado de precios (CLI / GitHub Actions)
Autor: Diego B. Meza · Revisión: 2025-06-25
Compatible: Python 3.7+
"""

import os
import sys
import glob
import re
import unicodedata
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from typing import List, Dict, Sequence, Callable, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# 0) Configuración desde variables de entorno (con fallback para OUT_DIR)
# ─────────────────────────────────────────────────────────────────────────────
 # ─────────────────────────────────────────────────────────────────────────────
 # 0) Configuración desde variables de entorno (con fallback para OUT_DIR)
 # ─────────────────────────────────────────────────────────────────────────────
 SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
 CREDS_RAW       = os.getenv("GOOGLE_CREDS")           # JSON de service-account
-OUT_DIR         = os.getenv("OUT_DIR") or "/tmp/csvs"  # fallback si está vacío
+OUT_DIR         = os.getenv("OUT_DIR") or "/tmp/csvs"  # fallback si está vacío

+# ────────────────────────────────────────────────────────────────────────────
+# Desactivar guardado de CSV cuando corra en GitHub Actions
+SAVE_CSV = os.getenv("GITHUB_ACTIONS", "false").lower() != "true"
+# ────────────────────────────────────────────────────────────────────────────

 # volcamos las credenciales a un JSON temporal
 with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
     tmp.write(CREDS_RAW.encode())
     CREDS_JSON = tmp.name

# volcamos las credenciales a un JSON temporal
with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
    tmp.write(CREDS_RAW.encode())
    CREDS_JSON = tmp.name

WORKSHEET_NAME  = "maestro"
MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]
PATTERN_DAILY = os.path.join(OUT_DIR, "*_canasta_*.csv")

# ─────────────────────────────────────────────────────────────────────────────
# 1) Normalización y filtrado
# ─────────────────────────────────────────────────────────────────────────────
def strip_accents(txt: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(ch) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(tok.lower()) for tok in _token_re.findall(txt)]

EXCLUDE_WORDS = [
    "pañal","pañales","toallita","algodón","curita","gasas","jeringa",
    "termometro","ibuprofeno","paracetamol","ampolla","inyectable",
    "dental","facial","crema","locion","shampoo","perfume","maquillaje",
    "labial","rimel","colonia","esmalte","herramienta","martillo","clavo",
    "taladro","pintura","cable","juguete","bicicleta","detergente",
    "lavavajilla","alfajor","alfajores","celulitis","corporal","fusifar",
    "estrias","aciclovir"
]
EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_WORDS}

def is_excluded(name: str) -> bool:
    return any(tok in EXCLUDE_SET for tok in tokenize(name))

GROUP_BY_PRODUCT: Dict[str, List[str]] = {
    "Carnicería": ["carne","vacuno","cerdo","pollo","ave","pescado",
                   "marisco","hamburguesa","menudencia","mortadela"],
    "Panadería":  ["pan","panificado","bizcocho","budin","torta","galleta",
                   "prepizza","pizza","chipa","tostada","molde"],
    "Huevos":     ["huevo","huevos"],
    "Lácteos":    ["leche","yogur","queso","manteca","crema","quesillo"]
}
GROUP_TOKENS = {
    grupo: {strip_accents(w) for w in palabras}
    for grupo, palabras in GROUP_BY_PRODUCT.items()
}

def assign_group(name: str) -> str | None:
    toks = set(tokenize(name))
    for grupo, kws in GROUP_TOKENS.items():
        if toks & kws:
            return grupo
    return None

def norm_price(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    txt = re.sub(r"[^\d,\.]", "", str(val))
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return 0.0

def _first_price(node: BeautifulSoup, selectors: List[str] = None) -> float:
    sels = selectors or [
        "span.price ins span.amount",
        "span.price > span.amount",
        "span.woocommerce-Price-amount",
        "span.amount","bdi","[data-price]"
    ]
    for sel in sels:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get("data-price",""))
            if p > 0:
                return p
    return 0.0

def _build_session() -> requests.Session:
    retry = Retry(
        total=3, backoff_factor=1.2,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","HEAD"),
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

# ─────────────────────────────────────────────────────────────────────────────
# 2) Scrapers HTML
# ─────────────────────────────────────────────────────────────────────────────
KEYWORDS_SUPER = set().union(*GROUP_TOKENS.values())

class HtmlSiteScraper:
    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.session = _build_session()

    def category_urls(self) -> List[str]:
        raise NotImplementedError

    def parse_category(self, url: str) -> List[dict]:
        raise NotImplementedError

    def scrape(self) -> List[dict]:
        urls = self.category_urls()
        if not urls:
            return []
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        registros: List[dict] = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = {pool.submit(self.parse_category, u): u for u in urls}
            for fut in as_completed(futures):
                for row in fut.result():
                    row["FechaConsulta"] = fecha
                    registros.append(row)
        return registros

    def save_csv(self, registros: List[dict]) -> None:
        if not registros:
            return
        os.makedirs(OUT_DIR, exist_ok=True)
        fname = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(registros).to_csv(os.path.join(OUT_DIR, fname), index=False)

class StockScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("stock", "https://www.stock.com.py")

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = {
            urljoin(self.base_url, a["href"])
            for a in soup.select('a[href*="/category/"]')
            if any(k in a["href"].lower() for k in KEYWORDS_SUPER)
        }
        return list(urls)

    def parse_category(self, url: str) -> List[dict]:
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for p in soup.select("div.product-item"):
            nm = p.select_one("h2.product-title")
            if not nm:
                continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre):
                continue
            grupo = assign_group(nombre)
            if not grupo:
                continue
            precio = _first_price(p, ["span.price-label","span.price"])
            rows.append({
                "Supermercado": "Stock",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Grupo": grupo,
            })
        return rows

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("superseis", "https://www.superseis.com.py")

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = {
            urljoin(self.base_url, a["href"])
            for a in soup.select('a.collapsed[href*="/category/"]')
            if any(k in a["href"].lower() for k in KEYWORDS_SUPER)
        }
        return list(urls)

    def parse_category(self, url: str) -> List[dict]:
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for a in soup.select("a.product-title-link"):
            nombre = a.get_text(" ", strip=True)
            if is_excluded(nombre):
                continue
            grupo = assign_group(nombre)
            if not grupo:
                continue
            cont = a.find_parent("div", class_="product-item") or a
            precio = _first_price(cont, ["span.price-label","span.price"])
            rows.append({
                "Supermercado": "Superseis",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Grupo": grupo,
            })
        return rows

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("salemma", "https://www.salemmaonline.com.py")

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = {
            urljoin(self.base_url, a["href"])
            for a in soup.find_all("a", href=True)
            if any(k in a["href"].lower() for k in KEYWORDS_SUPER)
        }
        return list(urls)

    def parse_category(self, url: str) -> List[dict]:
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for f in soup.select("form.productsListForm"):
            nombre = f.find("input", {"name":"name"}).get("value","")
            if is_excluded(nombre):
                continue
            grupo = assign_group(nombre)
            if not grupo:
                continue
            precio = norm_price(f.find("input",{"name":"price"}).get("value",""))
            rows.append({
                "Supermercado": "Salemma",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Grupo": grupo,
            })
        return rows

class AreteScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("arete", "https://www.arete.com.py")

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for sel in ("#departments-menu","#menu-departments-menu-1"):
            menu = soup.select_one(sel)
            if not menu:
                continue
            for a in menu.select('a[href^="catalogo/"]'):
                href = a["href"].split("?")[0].lower()
                if any(k in href for k in KEYWORDS_SUPER):
                    urls.add(urljoin(self.base_url+"/", a["href"]))
        return list(urls)

    def parse_category(self, url: str) -> List[dict]:
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for p in soup.select("div.product"):
            nm = p.select_one("h2.ecommercepro-loop-product__title")
            if not nm:
                continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre):
                continue
            grupo = assign_group(nombre)
            if not grupo:
                continue
            precio = _first_price(p)
            rows.append({
                "Supermercado": "Arete",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Grupo": grupo,
            })
        return rows

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = "losjardines"
        self.base_url = "https://losjardinesonline.com.py"

class BiggieScraper:
    name, API, TAKE = "biggie","https://api.app.biggie.com.py/api/articles",100
    GROUPS = ["carniceria","panaderia","huevos","lacteos"]
    session = _build_session()

    def fetch_group(self, grp: str) -> List[dict]:
        rows, skip = [], 0
        while True:
            js = self.session.get(
                self.API,
                params={"take":self.TAKE,"skip":skip,"classificationName":grp},
                timeout=REQ_TIMEOUT
            ).json()
            for it in js.get("items", []):
                nombre = it.get("name","")
                if is_excluded(nombre):
                    continue
                grupo = assign_group(nombre) or grp.capitalize()
                rows.append({
                    "Supermercado": "Biggie",
                    "CategoríaURL": grp,
                    "Producto": nombre.upper(),
                    "Precio": norm_price(it.get("price",0)),
                    "Grupo": grupo,
                })
            skip += self.TAKE
            if skip >= js.get("count",0):
                break
        return rows

    def scrape(self) -> List[dict]:
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        for grp in self.GROUPS:
            for item in self.fetch_group(grp):
                item["FechaConsulta"] = fecha
                out.append(item)
        return out

    def save_csv(self, rows: List[dict]) -> None:
        if not rows:
            return
        os.makedirs(OUT_DIR, exist_ok=True)
        fname = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fname), index=False)

# ─────────────────────────────────────────────────────────────────────────────
# 3) Orquestador y Google Sheets
# ─────────────────────────────────────────────────────────────────────────────
SCRAPERS: Dict[str, Callable[[], object]] = {
    "stock":       StockScraper,
    "superseis":   SuperseisScraper,
    "salemma":     SalemmaScraper,
    "arete":       AreteScraper,
    "losjardines": JardinesScraper,
    "biggie":      BiggieScraper,
}

def _parse_args(argv: Sequence[str] | None = None) -> List[str]:
    if argv is None:
        return list(SCRAPERS.keys())
    flat: List[str] = []
    for a in argv:
        flat.extend(a if isinstance(a,(list,tuple)) else [a])
    if any(x in ("-h","--help") for x in flat):
        print("Uso: python canasta_scraper.py [sitio1 sitio2 …]")
        sys.exit(0)
    sel = [x for x in flat if x in SCRAPERS]
    return sel or list(SCRAPERS.keys())

def _open_sheet():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="40")
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False)
    df.dropna(how="all", inplace=True)
    return ws, df

def _write_sheet(ws, df: pd.DataFrame) -> None:
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)

def main(argv: Sequence[str] | None = None) -> int:
    sitios = _parse_args(argv if argv is not None else sys.argv[1:])
    all_records: List[dict] = []

        for key in sitios:
        scraper = SCRAPERS[key]()
        recs = scraper.scrape()
+       if SAVE_CSV:
+           scraper.save_csv(recs)
        all_records.extend(recs)
        print(f"• {key:<12}: {len(recs):>5} filas")


    if not all_records:
        print("Sin datos nuevos.")
        return 0

    df_all = pd.concat(
        [pd.read_csv(fp, dtype=str) for fp in glob.glob(PATTERN_DAILY)],
        ignore_index=True, sort=False
    )
    df_all["Grupo"] = df_all["Grupo"].map(strip_accents).fillna("")
    df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce")

    ws, df_prev = _open_sheet()
    base = pd.concat([df_prev, df_all], ignore_index=True, sort=False)

    base["FechaConsulta"] = pd.to_datetime(base["FechaConsulta"], errors="coerce", dayfirst=True)
    base.sort_values("FechaConsulta", inplace=True)
    base["FechaConsulta"] = base["FechaConsulta"].dt.strftime("%Y-%m-%d")
    base.drop_duplicates(KEY_COLS, keep="first", inplace=True)
    base.reset_index(drop=True, inplace=True)

    if "ID" in base.columns:
        base.drop(columns=["ID"], inplace=True)
    base.insert(0, "ID", range(1, len(base) + 1))

    _write_sheet(ws, base)
    print(f"✅ Hoja '{WORKSHEET_NAME}' actualizada: {len(base)} filas totales")
    return 0

if __name__ == "__main__":
    sys.exit(main())
