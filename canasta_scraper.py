# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – versión compatible Py 3.7+
Autor  : Diego B. Meza · Revisión: 2025-06-24
"""

from __future__ import annotations
from typing import List, Dict, Sequence, Callable, Set
from typing_extensions import TypedDict

import os, sys, glob, re, unicodedata
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.colab import drive, auth
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials


import os, json, tempfile

SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
CREDS_RAW       = os.getenv("GOOGLE_CREDS")           # JSON completo
OUT_DIR         = os.getenv("OUT_DIR", "/tmp/csv")    # en runner

# Volcar las credenciales a un archivo temporal
tmp_creds = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
tmp_creds.write(CREDS_RAW.encode())
tmp_creds.close()
CREDS_JSON = tmp_creds.name






SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8"
WORKSHEET_NAME  = "maestro"

MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]

# ────────────────── 1. Normalización texto ─────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

EXCLUDE_PRODUCT_WORDS = [
    # higiene / farmacia
    "pañal", "pañales", "toallita", "algodón", "curita", "gasas", "jeringa",
    "termometro", "ibuprofeno", "paracetamol", "ampolla", "inyectable",
    "dental", "facial", "crema", "locion", "shampoo",
    # cosmética
    "perfume", "maquillaje", "labial", "rimel", "colonia", "esmalte",
    # ferretería
    "herramienta", "martillo", "clavo", "taladro", "pintura", "cable",
    # varios
    "juguete", "bicicleta", "detergente", "lavavajilla", "alfajor", "alfajores",
    "celulitis", "corporal", "fusifar", "estrias", "aciclovir"
]
EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_PRODUCT_WORDS}

def is_excluded(name: str) -> bool:
    return any(tok in EXCLUDE_SET for tok in tokenize(name))

GROUP_BY_PRODUCT: Dict[str, List[str]] = {
    "Carnicería": ["carne", "vacuno", "cerdo", "pollo", "ave", "pescado",
                   "marisco", "hamburguesa", "menudencia", "mortadela"],
    "Panadería":  ["pan", "panificado", "bizcocho", "budin", "torta", "galleta",
                   "prepizza", "pizza", "chipa", "tostada", "molde"],
    "Huevos":     ["huevo", "huevos"],
    "Lácteos":    ["leche", "yogur", "queso", "manteca", "crema", "quesillo"]
}
GROUP_TOKENS = {g: {strip_accents(w) for w in ws}
                for g, ws in GROUP_BY_PRODUCT.items()}

def assign_group(name: str) -> str | None:
    toks = set(tokenize(name))
    for g, ks in GROUP_TOKENS.items():
        if toks & ks:
            return g
    return None

def norm_price(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    txt = re.sub(r"[^\d,\.]", "", str(val))
    txt = txt.replace(".", "").replace(",", ".")
    try:  return float(txt)
    except ValueError: return 0.0

def _first_price(node: BeautifulSoup,
                 sels: List[str] = None) -> float:
    sels = sels or ["span.price ins span.amount", "span.price > span.amount",
                    "span.woocommerce-Price-amount", "span.amount", "bdi",
                    "[data-price]"]
    for s in sels:
        el = node.select_one(s)
        if el:
            p = norm_price(el.get_text() or el.get("data-price", ""))
            if p > 0:
                return p
    return 0.0

def _build_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "HEAD"), raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123 Safari/537.36")
    s.mount("http://", ad); s.mount("https://", ad)
    return s

# ────────────────── 2. Scrapers HTML ───────────────────────────
KEYWORDS_SUPER = (GROUP_TOKENS["Carnicería"] | GROUP_TOKENS["Panadería"] |
                  GROUP_TOKENS["Huevos"]     | GROUP_TOKENS["Lácteos"])

class HtmlSiteScraper:
    def __init__(self, name, base):
        self.name = name; self.base_url = base.rstrip("/")
        self.session = _build_session()

    def category_urls(self):  raise NotImplementedError
    def parse_category(self, url): raise NotImplementedError

    def scrape(self):
        urls = self.category_urls()
        if not urls: return []
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out   = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futs = {pool.submit(self.parse_category, u): u for u in urls}
            for f in as_completed(futs):
                for row in f.result():
                    row["FechaConsulta"] = fecha
                    out.append(row)
        return out

    def save_csv(self, rows):
        if not rows: return
        os.makedirs(OUT_DIR, exist_ok=True)
        fn = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("stock", "https://www.stock.com.py")

    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for a in soup.select('a[href*="/category/"]'):
            href = a["href"].lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)

    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for p in soup.select("div.product-item"):
            nm = p.select_one("h2.product-title")
            if not nm: continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            precio = _first_price(p, ["span.price-label", "span.price"])
            rows.append({"Supermercado":"Stock","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("superseis","https://www.superseis.com.py")

    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for a in soup.select('a.collapsed[href*="/category/"]'):
            href = a["href"].lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)

    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for a in soup.select("a.product-title-link"):
            nombre = a.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            cont = a.find_parent("div", class_="product-item") or a
            precio = _first_price(cont, ["span.price-label", "span.price"])
            rows.append({"Supermercado":"Superseis","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("salemma","https://www.salemmaonline.com.py")

    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)

    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for f in soup.select("form.productsListForm"):
            nombre = f.find("input", {"name":"name"}).get("value","")
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            precio = norm_price(f.find("input",{"name":"price"}).get("value",""))
            rows.append({"Supermercado":"Salemma","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("arete","https://www.arete.com.py")

    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls=set()
        for sel in ("#departments-menu","#menu-departments-menu-1"):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                href = a["href"].split("?")[0].lower()
                if any(k in href for k in KEYWORDS_SUPER):
                    urls.add(urljoin(self.base_url+"/", a["href"]))
        return list(urls)

    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows=[]
        for p in soup.select("div.product"):
            nm = p.select_one("h2.ecommercepro-loop-product__title")
            if not nm: continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            precio = _first_price(p)
            rows.append({"Supermercado":"Arete","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class JardinesScraper(AreteScraper):
    def __init__(self): super().__init__(); self.name="losjardines"; self.base_url="https://losjardinesonline.com.py"

# ────────────────── 3. Biggie (API) ────────────────────────────
class BiggieScraper:
    name, API, TAKE = "biggie","https://api.app.biggie.com.py/api/articles",100
    GROUPS = ["carniceria","panaderia","huevos","lacteos"]
    session=_build_session()

    def fetch_group(self, grp):
        rows, skip = [], 0
        while True:
            js = self.session.get(self.API, params=dict(take=self.TAKE, skip=skip,
                         classificationName=grp), timeout=REQ_TIMEOUT).json()
            for it in js.get("items", []):
                nombre = it.get("name", "")
                if is_excluded(nombre): continue
                grupo = assign_group(nombre) or grp.capitalize()
                rows.append({"Supermercado":"Biggie","CategoríaURL":grp,
                             "Producto":nombre.upper(),
                             "Precio":norm_price(it.get("price",0)),
                             "Grupo":grupo})
            skip += self.TAKE
            if skip >= js.get("count", 0): break
        return rows

    def scrape(self):
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows=[]
        for g in self.GROUPS:
            for item in self.fetch_group(g):
                item["FechaConsulta"]=fecha
                rows.append(item)
        return rows

    def save_csv(self, rows):
        if not rows: return
        os.makedirs(OUT_DIR, exist_ok=True)
        fn = f"biggie_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

# ────────────────── 4. Gestor de sitios ────────────────────────
SCRAPERS:Dict[str,Callable]={
    "stock":StockScraper,"superseis":SuperseisScraper,"salemma":SalemmaScraper,
    "arete":AreteScraper,"losjardines":JardinesScraper,"biggie":BiggieScraper}

def _parse_args(argv=None):
    if argv is None: return list(SCRAPERS)
    if any(a in ("-h","--help") for a in argv):
        print("Uso: python script.py [sitio1 sitio2 …]"); sys.exit(0)
    sel=[a for a in argv if a in SCRAPERS]; return sel or list(SCRAPERS)

# ────────────────── 5. Google Sheets ───────────────────────────
def _open_sheet():
    scopes=["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    sh   = gspread.authorize(cred).open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="40")
    df = get_as_dataframe(ws,dtype=str,header=0,evaluate_formulas=False).dropna(how="all")
    return ws, df

def _write_sheet(ws, df):
    ws.clear(); set_with_dataframe(ws, df, include_index=False)

# ────────────────── 6. Orquestador ────────────────────────────
def main(argv=None):
    objetivos=_parse_args(argv if argv is not None else sys.argv[1:])
    registros=[]
    for k in objetivos:
        sc=SCRAPERS[k](); filas=sc.scrape(); sc.save_csv(filas)
        registros.extend(filas); print(f"• {k:<10}: {len(filas):>5} filas")

    if not registros: print("Sin datos nuevos."); return 0
    df_all = pd.concat([pd.read_csv(f,dtype=str) for f in glob.glob(PATTERN_DAILY)],
                       ignore_index=True, sort=False)
    df_all["Grupo"]=df_all["Grupo"].map(strip_accents).fillna("")
    df_all["Precio"]=pd.to_numeric(df_all["Precio"], errors="coerce")

    ws, df_prev = _open_sheet()
    base=pd.concat([df_prev, df_all], ignore_index=True, sort=False)
    base["FechaConsulta"]=pd.to_datetime(base["FechaConsulta"], errors="coerce")
    base.sort_values("FechaConsulta", inplace=True)
    base["FechaConsulta"]=base["FechaConsulta"].dt.strftime("%Y-%m-%d")
    base.drop_duplicates(KEY_COLS, keep="first", inplace=True)

    # ❶ — después de drop_duplicates(...)

    if "ID" in base.columns:
      base.drop(columns=["ID"], inplace=True)

      base.insert(0,"ID",range(1,len(base)+1))
    _write_sheet(ws, base)
    print(f"✅ Hoja actualizada: {len(base)} filas totales")
    return 0

if __name__=="__main__":
    sys.exit(main())
