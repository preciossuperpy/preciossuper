# -*- coding: utf-8 -*-
"""
Scraper unificado de precios + Proyección 30 días – Py 3.7+ adaptado para GitHub Actions
Autor  : Diego B. Meza · Rev: 2025-07-05 (correcciones Superseis & rebuild_projections)
"""

import os
import re
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
BASE_DIR        = os.environ.get("BASE_DIR", os.getcwd())
OUT_DIR         = os.environ.get("OUT_DIR", os.path.join(BASE_DIR, "out"))
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL")
CREDS_JSON      = os.environ.get("CREDS_JSON_PATH", os.path.abspath("creds.json"))
WORKSHEET_NAME  = os.environ.get("WORKSHEET_NAME", "precios_supermercados")
MAX_WORKERS     = 8
REQ_TIMEOUT     = 20  # segundos por request

FORECAST_DAYS   = 30   # horizonte de proyección
LOOKBACK_DAYS   = 60   # días para ajuste del modelo

COLUMNS = [
    "ID",
    "Supermercado","Producto","Precio","Unidad",
    "Grupo","Subgrupo","FechaConsulta",
    "PrecioProj30d","TipoDato"
]
KEY_COLS = ["Supermercado","Producto","FechaConsulta","TipoDato"]

os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ─────────────── 2. Clasificación Grupo/Subgrupo ─────────────────
BROAD_GROUP_KEYWORDS = {
    "Panificados": ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":      ["naranja","manzana","banana","pera","uva","frutilla"],
    "Verduras":    ["tomate","cebolla","papa","zanahoria","lechuga","espinaca"],
    "Huevos":      ["huevo","huevos","codorniz"],
    "Lácteos":     ["leche","yogur","queso","manteca","crema"],
}
BROAD_TOKENS = {g:{strip_accents(w) for w in ws} for g,ws in BROAD_GROUP_KEYWORDS.items()}

SUBGROUP_KEYWORDS = {
    "Naranja": ["naranja","naranjas"],
    "Cebolla": ["cebolla","cebollas"],
    "Leche Entera": ["entera"],
    "Leche Descremada": ["descremada"],
    "Queso Paraguay": ["paraguay"],
    "Huevo Gallina": ["gallina"],
    "Huevo Codorniz": ["codorniz"],
}
SUB_TOKENS = {sg:{strip_accents(w) for w in ws} for sg,ws in SUBGROUP_KEYWORDS.items()}

def classify(name: str) -> (str, str):
    toks = set(tokenize(name))
    grp = next((g for g, ks in BROAD_TOKENS.items() if toks & ks), "")
    sub = next((s for s, ks in SUB_TOKENS.items() if toks & ks), "")
    return grp, sub

# ─────────────── 2.5. Filtro de exclusión ─────────────────
EXCLUDE_REGEX = re.compile(r"\b(combo|pack|disney)\b", re.I)
def is_excluded(name: str) -> bool:
    return bool(EXCLUDE_REGEX.search(name))

# ─────────────── 3. Extracción de unidad ───────────────────────
_unit_re = re.compile(
    r"(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|kilos?|g|gr|ml|cc|l(?:itro)?s?|lt|unid(?:ad)?s?|u|paq)\b",
    re.I
)
def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m:
        return ""
    val = float(m.group('val').replace(',', '.'))
    unit = m.group('unit').lower().rstrip('s')
    if unit in ('kg','kilo'):
        val *= 1000; unit_out = 'GR'
    elif unit in ('l','lt','litro'):
        val *= 1000; unit_out = 'CC'
    elif unit in ('g','gr'):
        unit_out = 'GR'
    elif unit in ('ml','cc'):
        unit_out = 'CC'
    else:
        unit_out = unit.upper()
    val_str = str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip('0').rstrip('.')
    return f"{val_str}{unit_out}"

# ─────────────── 4. Normalización de precio ────────────────────
_price_selectors = [
    "[data-price]","[data-price-final]","[data-price-amount]",
    "meta[itemprop='price']","span.price ins span.amount",
    "span.price > span.amount","span.woocommerce-Price-amount",
    "span.amount","bdi","div.price","p.price",
    "span.price-label"    # <-- agregado para Superseis
]
def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try:
        return float(txt)
    except:
        return 0.0

def _first_price(node: Tag) -> float:
    for attr in ("data-price","data-price-final","data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr]) > 0:
            return norm_price(node[attr])
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get('content','')) > 0:
        return norm_price(meta.get('content',''))
    for sel in _price_selectors:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get(sel,''))
            if p > 0:
                return p
    return 0.0

# ─────────────── 5. HTTP session robusta ───────────────────────
def _build_session() -> requests.Session:
    retry = Retry(
        total=4, backoff_factor=1.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","HEAD")
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9",
    })
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

# ─────────────── 6. Plantilla para filas y clase base ────────────
ROW_TEMPLATE = {"PrecioProj30d": np.nan, "TipoDato": "HIST"}

class HtmlSiteScraper:
    def __init__(self, name: str, base: str):
        self.name = name
        self.base_url = base.rstrip('/')
        self.session = _build_session()

    def category_urls(self) -> List[str]:
        raise NotImplementedError

    def parse_category(self, url: str) -> List[Dict]:
        raise NotImplementedError

    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows: List[Dict] = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = [pool.submit(self.parse_category, u) for u in self.category_urls()]
            for fut in as_completed(futures):
                for r in fut.result() or []:
                    if r.get("Precio",0) <= 0:
                        continue
                    r["FechaConsulta"] = ts
                    r.update(ROW_TEMPLATE)
                    rows.append(r)
        return rows

    def save_csv(self, rows: List[Dict]):
        if not rows:
            return
        fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS[1:]].to_csv(os.path.join(OUT_DIR, fn), index=False)

# ────────── 7–12. Scrapers específicos ───────────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('stock','https://www.stock.com.py')
    def category_urls(self):
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text,
                             'html.parser')
        kws = [tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [urljoin(self.base_url, a['href'])
                for a in soup.select('a[href*="/category/"]')
                if any(k in a['href'].lower() for k in kws)]
    def parse_category(self, url):
        out: List[Dict] = []
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT); resp.raise_for_status()
        except:
            return out
        soup = BeautifulSoup(resp.content,'html.parser')
        for card in soup.select('div.product-item'):
            nm_el = card.select_one('h2.product-title')
            if not nm_el:
                continue
            nombre = nm_el.get_text(' ',strip=True)
            if is_excluded(nombre):
                continue
            precio = _first_price(card)
            grp, sub = classify(nombre)
            if not grp:
                continue
            unidad = extract_unit(nombre)
            out.append({
                "Supermercado": self.name,
                "Producto":     nombre.upper(),
                "Precio":       precio,
                "Unidad":       unidad,
                "Grupo":        grp,
                "Subgrupo":     sub
            })
        return out

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('superseis','https://www.superseis.com.py')
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return []
        soup = BeautifulSoup(r.text,'html.parser')
        return list({
            urljoin(self.base_url, a['href'])
            for a in soup.find_all("a", href=True, class_="collapsed")
            if "/category/" in a["href"]
        })
    def parse_category(self, url):
        out: List[Dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return out
        soup = BeautifulSoup(r.content,'html.parser')
        for a in soup.find_all("a", class_="product-title-link"):
            nombre = a.get_text(strip=True)
            if is_excluded(nombre):
                continue
            # buscamos explícitamente span.price-label
            cont = a.find_parent("div", class_="product-item")
            tag = (cont and cont.find("span", class_="price-label")) \
                  or a.find_next("span", class_="price-label")
            precio = norm_price(tag.get_text()) if tag else 0.0
            if precio <= 0:
                continue
            grp, sub = classify(nombre)
            if not grp:
                continue
            unidad = extract_unit(nombre)
            out.append({
                "Supermercado": self.name,
                "Producto":     nombre.upper(),
                "Precio":       precio,
                "Unidad":       unidad,
                "Grupo":        grp,
                "Subgrupo":     sub
            })
        return out

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('salemma','https://www.salemmaonline.com.py')
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return []
        urls = set()
        for a in BeautifulSoup(r.content,'html.parser').find_all('a', href=True):
            h = a['href'].lower()
            if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                urls.add(urljoin(self.base_url,h))
        return list(urls)
    def parse_category(self, url):
        out: List[Dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return out
        soup = BeautifulSoup(r.content,'html.parser')
        for f in soup.select('form.productsListForm'):
            nm = f.find('input',{'name':'name'}).get('value','')
            if is_excluded(nm):
                continue
            precio = norm_price(f.find('input',{'name':'price'}).get('value',''))
            grp, sub = classify(nm)
            if not grp:
                continue
            unidad = extract_unit(nm)
            out.append({
                "Supermercado": self.name,
                "Producto":     nm.upper(),
                "Precio":       precio,
                "Unidad":       unidad,
                "Grupo":        grp,
                "Subgrupo":     sub
            })
        return out

class AreteScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__('arete','https://www.arete.com.py')
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return []
        soup = BeautifulSoup(r.content,'html.parser')
        urls = set()
        for sel in ('#departments-menu','#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h = a['href'].split('?')[0].lower()
                if any(tok in h for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                    urls.add(urljoin(self.base_url+'/',h))
        return list(urls)
    def parse_category(self, url):
        out: List[Dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except:
            return out
        soup = BeautifulSoup(r.content,'html.parser')
        for card in soup.select('div.product'):
            el = card.select_one('h2.ecommercepro-loop-product__title')
            if not el:
                continue
            nombre = el.get_text(' ',strip=True)
            if is_excluded(nombre):
                continue
            precio = _first_price(card)
            grp, sub = classify(nombre)
            if not grp:
                continue
            unidad = extract_unit(nombre)
            out.append({
                "Supermercado": self.name,
                "Producto":     nombre.upper(),
                "Precio":       precio,
                "Unidad":       unidad,
                "Grupo":        grp,
                "Subgrupo":     sub
            })
        return out

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = 'losjardines'
        self.base_url = 'https://losjardinesonline.com.py'

class BiggieScraper:
    name   = 'biggie'
    API    = 'https://api.app.biggie.com.py/api/articles'
    TAKE   = 100
    GROUPS = ['huevos','lacteos','frutas','verduras','cereales','panificados']
    def __init__(self):
        self.session = _build_session()
    def category_urls(self):
        return self.GROUPS
    def parse_category(self, grp):
        out: List[Dict] = []
        skip = 0
        while True:
            try:
                js = self.session.get(
                    self.API,
                    params={'take':self.TAKE,'skip':skip,'classificationName':grp},
                    timeout=REQ_TIMEOUT
                ).json()
            except:
                break
            for it in js.get('items',[]):
                nm = it.get('name','')
                precio = norm_price(it.get('price',0))
                if precio <= 0:
                    continue
                g, sub = classify(nm)
                unidad = extract_unit(nm)
                out.append({
                    "Supermercado": self.name,
                    "Producto":     nm.upper(),
                    "Precio":       precio,
                    "Unidad":       unidad,
                    "Grupo":        g or grp.capitalize(),
                    "Subgrupo":     sub
                })
            skip += self.TAKE
            if skip >= js.get('count',0):
                break
        return out
    def scrape(self):
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        rows: List[Dict] = []
        for grp in self.GROUPS:
            for r in self.parse_category(grp):
                r["FechaConsulta"] = ts
                r.update(ROW_TEMPLATE)
                rows.append(r)
        return rows
    def save_csv(self, rows):
        if not rows:
            return
        fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS[1:]].to_csv(os.path.join(OUT_DIR, fn), index=False)

SCRAPERS = {
    'stock':       StockScraper,
    'superseis':   SuperseisScraper,
    'salemma':     SalemmaScraper,
    'arete':       AreteScraper,
    'losjardines': JardinesScraper,
    'biggie':      BiggieScraper,
}

# ─────────────── 13. Google Sheets & Orquestador ───────────────────
def _open_sheet():
    scopes = ['https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/spreadsheets']
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc   = gspread.authorize(cred)
    sh   = gc.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
        df = get_as_dataframe(ws, dtype=str, header=0,
                              evaluate_formulas=False).dropna(how='all')
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME,
                              rows='20000', cols=str(len(COLUMNS)))
        df = pd.DataFrame(columns=COLUMNS[1:])
    for col in COLUMNS[1:]:
        if col not in df.columns:
            df[col] = np.nan if col.startswith("Precio") else ""
    return ws, df

def _write_sheet(ws, df: pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws, df[COLUMNS], include_index=False)

def forecast_group(df_g: pd.DataFrame) -> float:
    """Proyecta t+FORECAST_DAYS usando ajuste lineal sobre media diaria."""
    if len(df_g) < 2:
        return float(df_g['Precio'].iloc[-1])
    x = (df_g.index - df_g.index.min()).days.values
    y = df_g['Precio'].values
    a, b = np.polyfit(x, y, 1)
    return float(a * (x[-1] + FORECAST_DAYS) + b)

def rebuild_projections(full_df: pd.DataFrame) -> pd.DataFrame:
    """Genera una fila PROY por cada Grupo con precio proyectado."""
    full_df['FechaConsulta'] = pd.to_datetime(full_df['FechaConsulta'])
    cutoff = full_df['FechaConsulta'].max() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent = full_df[
        (full_df['TipoDato']=='HIST') &
        (full_df['FechaConsulta'] >= cutoff)
    ]
    daily = (
        recent
        .groupby(['Grupo','FechaConsulta'])['Precio']
        .mean()
        .reset_index()
    )
    daily['FechaConsulta'] = pd.to_datetime(daily['FechaConsulta'])

    proj_rows: List[Dict] = []
    for grp, gdf in daily.groupby('Grupo'):
        # interpolar solo en la serie numérica de 'Precio'
        precio_ts = (
            gdf.sort_values('FechaConsulta')
               .set_index('FechaConsulta')['Precio']
               .asfreq('D')
               .interpolate()
        )
        gdf_clean = precio_ts.to_frame('Precio')
        y_next = forecast_group(gdf_clean)

        proj_rows.append({
            "Supermercado":   "—",
            "Producto":       f"PROYECCIÓN {grp.upper()}",
            "Precio":         np.nan,
            "Unidad":         "",
            "Grupo":          grp,
            "Subgrupo":       "",
            "FechaConsulta":  full_df['FechaConsulta'].max().strftime("%Y-%m-%d %H:%M:%S"),
            "PrecioProj30d":  round(y_next,2),
            "TipoDato":       "PROY"
        })

    return pd.DataFrame(proj_rows, columns=COLUMNS[1:])

def main() -> None:
    all_rows: List[Dict] = []
    for cls in SCRAPERS.values():
        inst = cls()
        rows = inst.scrape()
        inst.save_csv(rows)
        all_rows.extend(rows)

    if not all_rows:
        print("Sin datos nuevos.")
        return

    df_all = pd.DataFrame(all_rows)[COLUMNS[1:]].copy()
    df_all["FechaConsulta"] = pd.to_datetime(df_all["FechaConsulta"], errors="coerce")

    ws, prev_df = _open_sheet()
    prev_df["FechaConsulta"] = pd.to_datetime(prev_df["FechaConsulta"], errors="coerce")

    # Detectar nuevas filas HIST
    idx_prev = prev_df.set_index(KEY_COLS[1:]).index
    idx_all  = df_all .set_index(KEY_COLS[1:]).index
    new_idx  = idx_all.difference(idx_prev)

    if not new_idx.empty:
        new_rows = df_all.set_index(KEY_COLS[1:]).loc[new_idx].reset_index()
        start_id = len(prev_df) + 1
        new_rows.insert(0, "ID", range(start_id, start_id + len(new_rows)))
        new_rows["FechaConsulta"] = new_rows["FechaConsulta"].dt.strftime("%Y-%m-%d %H:%M:%S")
        set_with_dataframe(
            ws,
            new_rows[COLUMNS],
            row=len(prev_df) + 2,
            include_index=False,
            include_column_header=False
        )
        prev_df = pd.concat([prev_df, new_rows], ignore_index=True)
        print(f"+{len(new_rows)} filas HIST agregadas.")
    else:
        print("No hay filas HIST nuevas.")

    # Reconstruir y escribir proyecciones
    full_df = prev_df[prev_df['TipoDato'] != 'PROY']
    proj_df = rebuild_projections(prev_df)

    next_id = len(full_df) + 1
    proj_df.insert(0, "ID", range(next_id, next_id + len(proj_df)))

    final_df = pd.concat([full_df, proj_df], ignore_index=True)
    final_df = final_df.sort_values(['TipoDato','FechaConsulta'])
    _write_sheet(ws, final_df)

    print(f"{len(proj_df)} proyecciones PROY actualizadas.")

if __name__ == "__main__":
    main()
