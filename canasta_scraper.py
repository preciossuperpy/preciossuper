# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – versión corregida campos consistentes
Autor: Diego B. Meza · Revisión: 2025-07-01
"""

import os
import sys
import glob
import re
import json
import unicodedata
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from typing import List, Dict, Callable, Set, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
CREDS_RAW = os.getenv("GOOGLE_CREDS")
OUT_DIR = os.getenv("OUT_DIR", "./csvs")
os.makedirs(OUT_DIR, exist_ok=True)
PATTERN_DAILY = os.path.join(OUT_DIR, "*_canasta_*.csv")
WORKSHEET_NAME = "maestro"
MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]
REQUIRED_COLS = ["Supermercado", "CategoríaURL", "Producto", "Precio", "Categoría", "Subcategoría", "FechaConsulta"]

if not CREDS_RAW:
    raise ValueError("No se proporcionaron credenciales de Google (GOOGLE_CREDS)")

try:
    creds_data = json.loads(CREDS_RAW)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp:
        json.dump(creds_data, temp)
        CREDS_JSON = temp.name
except json.JSONDecodeError as e:
    print(f"Error en formato JSON: {e}")
    sys.exit(1)

# ────────────────── 1. Normalización texto ─────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ────────────────── 2. Exclusión y categorización ─────────────────────
EXCLUDE_PRODUCT_WORDS = [
    "medicamento", "medicina", "pastilla", "comprimido", "cápsula", "jarabe", "supositorio", "inyección",
    "antibiótico", "analgésico", "antiinflamatorio", "antihistamínico", "antidepresivo", "vitamina", "suplemento",
    "prescripción", "receta", "farmacia", "droguería", "fármaco", "genérico", "marca", "aspirina", "diclofenac",
    "omeprazol", "loratadina", "paracetamol", "ibuprofeno", "amoxicilina", "diazepam", "simvastatina", "metformina",
    "pañal", "pañales", "toallita", "toallitas", "algodón", "curita", "curitas", "gasa", "gasas", "jeringa", "jeringas",
    "termómetro", "tampón", "compresa", "protector", "incontinencia", "papel higiénico",
    "shampoo", "champú", "acondicionador", "jabón", "gel", "crema", "loción", "desodorante", "antitranspirante",
    "maquillaje", "labial", "rimel", "delineador", "sombras", "base", "polvo", "corrector", "bronceador", "protector solar",
    "perfume", "colonia", "desmaquillante", "cotonete", "hisopo", "cepillo dental", "pasta dental", "enjuague bucal",
    "hilo dental", "blanqueador dental", "prótesis", "ortodoncia",
    "preservativo", "condón", "lubricante", "píldora", "anticonceptivo", "prueba embarazo", "tensiómetro", "glucómetro",
    "mascarilla", "cubrebocas", "alcohol", "antiséptico", "desinfectante", "vendaje", "tirita", "tiritas",
    "pañuelo", "pañuelos", "toalla", "toallas", "afeitadora", "maquinilla", "hoja de afeitar", "espuma de afeitar",
    "talco", "cuchilla", "rasuradora", "cortaúñas", "lima", "tijera", "termo", "taza", "vaso",
    "copa menstrual", "bolsa térmica", "tinte", "coloración", "alisador", "rizador", "secador", "plancha", "tenacilla",
    "celulitis", "corporal", "estrias", "aciclovir", "fusifar", "dental", "facial", "loción", "corporal"
]
EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_PRODUCT_WORDS}

def is_excluded(name: str) -> bool:
    if not name:
        return False
    tokens = set(tokenize(name))
    if tokens & EXCLUDE_SET:
        return True
    name_lower = name.lower()
    exclusion_patterns = [
        r"\b(crema|gel|loción)\s+(para|de)\s+",
        r"\b(jabón|shampoo)\b",
        r"\bvitamina\b",
        r"\b(medicamento|medicina)\b",
        r"\b(anti\w+|desinfectante)\b"
    ]
    return any(re.search(pat, name_lower) for pat in exclusion_patterns)

CATEGORY_RULES = [
    {
        "name": "Carnes",
        "include": [
            r"\bcarne\b", r"\bvacuno\b", r"\bcerdo\b", r"\bpollo\b", r"\bpescado\b",
            r"\bmarisco\b", r"\bhamburguesa\b", r"\bmortadela\b", r"\bchuleta\b", r"\bpechuga\b",
            r"\bmuslo\b", r"\bmilanesa\b", r"\bsalchicha\b", r"\bchorizo\b", r"\bchurrasco\b",
            r"\bbife\b", r"\blomo\b", r"\basado\b", r"\bnalga\b", r"\bcuadril\b",
            r"\bcarne molida\b", r"\bpicada\b", r"\bpeceto\b"
        ],
        "exclude": [
            r"\bconserva\b", r"\blata\b", r"\benlatado\b", r"\bsalsa\b"
        ],
        "subcategories": [
            {"name": "Pollo", "include": [r"\bpollo\b", r"\bpechuga\b", r"\bmuslo\b"]},
            {"name": "Vacuno", "include": [r"\bvacuno\b", r"\bbife\b", r"\blomo\b"]},
            {"name": "Cerdo", "include": [r"\bcerdo\b", r"\bchuleta\b", r"\bbondiola\b"]},
            {"name": "Pescado", "include": [r"\bpescado\b", r"\bmerluza\b", r"\bsurubí\b"]},
            {"name": "Fiambres", "include": [r"\bmortadela\b", r"\bsalchicha\b", r"\bjamón\b"]},
            {"name": "Otros", "include": []}  # Subcategoría por defecto
        ]
    },
    {
        "name": "Panadería",
        "include": [
            r"\bpan\b", r"\bbaguette\b", r"\bfactura\b", r"\bmedialuna\b", r"\bcroissant\b",
            r"\bbizcocho\b", r"\bbudin\b", r"\btorta\b", r"\bgalleta\b", r"\bpizza\b",
            r"\bchipa\b", r"\btostada\b", r"\broquilla\b", r"\bpanettone\b"
        ],
        "exclude": [r"\bmolde\b", r"\bjuguete\b"],
        "subcategories": [
            {"name": "Pan", "include": [r"\bpan integral\b", r"\bpan de molde\b"]},
            {"name": "Facturas", "include": [r"\bfactura\b", r"\bmedialuna\b", r"\bcroissant\b"]},
            {"name": "Pizzas", "include": [r"\bpizza\b"]},
            {"name": "Postres", "include": [r"\bbizcocho\b", r"\btorta\b"]},
            {"name": "Otros", "include": []}  # Subcategoría por defecto
        ]
    },
    {
        "name": "Huevos",
        "include": [r"\bhuevo\b", r"\bhuevos\b"],
        "exclude": [r"\bpascua\b", r"\bdecorado\b"],
        "require_exact": True,
        "subcategories": [
            {"name": "Blanco", "include": [r"\bhuevo blanco\b"]},
            {"name": "Color", "include": [r"\bhuevo marrón\b"]},
            {"name": "Codorniz", "include": [r"\bhuevo de codorniz\b"]},
            {"name": "Otros", "include": []}  # Subcategoría por defecto
        ]
    },
    {
        "name": "Lácteos",
        "include": [
            r"\bleche\b", r"\byogur\b", r"\bqueso\b", r"\bmanteca\b",
            r"\bcrema\b", r"\bpostre\b", r"\bflan\b", r"\bdulce de leche\b"
        ],
        "exclude": [r"\bfacial\b", r"\bshampoo\b"],
        "subcategories": [
            {"name": "Leche", "include": [r"\bleche entera\b", r"\bleche descremada\b"]},
            {"name": "Quesos", "include": [r"\bqueso\b"]},
            {"name": "Yogures", "include": [r"\byogur\b"]},
            {"name": "Cremas", "include": [r"\bcrema de leche\b"]},
            {"name": "Dulces", "include": [r"\bdulce de leche\b"]},
            {"name": "Otros", "include": []}  # Subcategoría por defecto
        ]
    },
    {
        "name": "Verdulería",
        "include": [
            r"\blechuga\b", r"\btomate\b", r"\bcebolla\b", r"\bzanahoria\b",
            r"\bpapa\b", r"\bbatata\b", r"\blimón\b", r"\bnaranja\b"
        ],
        "exclude": [r"\bconserva\b", r"\benlatado\b"],
        "subcategories": [
            {"name": "Hortalizas", "include": [r"\btomate\b", r"\bzapallo\b"]},
            {"name": "Frutas", "include": [r"\bmanzana\b", r"\bbanana\b"]},
            {"name": "Tubérculos", "include": [r"\bpapa\b", r"\bbatata\b"]},
            {"name": "Otros", "include": []}  # Subcategoría por defecto
        ]
    }
]

def assign_category(name: str) -> Tuple[Optional[str], str]:
    if not name:
        return None, ""
    
    # Normalizar nombre para comparaciones
    normalized_name = strip_accents(name.lower())
    
    for cat in CATEGORY_RULES:
        # Aplicar exclusiones primero
        if any(re.search(excl, normalized_name) for excl in cat.get("exclude", [])):
            continue
            
        # Verificar coincidencias
        if any(re.search(pat, normalized_name) for pat in cat.get("include", [])):
            main_cat = cat["name"]
            sub_cat = "Otros"  # Valor por defecto
            
            # Buscar subcategoría específica
            for sub in cat.get("subcategories", []):
                if sub["name"] == "Otros":  # Saltar la categoría "Otros"
                    continue
                    
                if any(re.search(pat, normalized_name) for pat in sub.get("include", [])):
                    sub_cat = sub["name"]
                    break
                    
            return main_cat, sub_cat
            
    return None, ""

# ────────────────── 3. Auxiliares de scraping ─────────────────────
def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val))
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except:
        return 0.0

def _first_price(node, sels=None) -> float:
    sels = sels or ["span.price ins span.amount", "span.price > span.amount",
                    "span.woocommerce-Price-amount", "span.amount", "bdi", "[data-price]"]
    for sel in sels:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get("data-price", ""))
            if p > 0:
                return p
    return 0.0

def _build_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.2,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET","HEAD"), raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )
    sess.mount("http://", ad)
    sess.mount("https://", ad)
    return sess

# ────────────────── 4. Scrapers unificados ─────────────────────
class HtmlSiteScraper:
    def __init__(self, name, base_url):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.session = _build_session()
        
    def category_urls(self) -> List[str]: 
        raise NotImplementedError
        
    def parse_category(self, url: str) -> List[Dict]: 
        raise NotImplementedError
        
    def scrape(self):
        urls = self.category_urls()
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(self.parse_category, u) for u in urls]
            for fut in as_completed(futures):
                try:
                    for row in fut.result():
                        row.setdefault("FechaConsulta", fecha)
                        # Asegurar todos los campos requeridos
                        for col in REQUIRED_COLS:
                            if col not in row:
                                row[col] = "" if col != "Precio" else 0.0
                        out.append(row)
                except Exception as e:
                    print(f"Error procesando {self.name}: {e}")
        return out
        
    def save_csv(self, rows: List[Dict]):
        if not rows: 
            return
        fn = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        df = pd.DataFrame(rows)
        # Asegurar columnas requeridas
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = "" if col != "Precio" else 0.0
        df.to_csv(os.path.join(OUT_DIR, fn), index=False)

class StockScraper(HtmlSiteScraper):
    def __init__(self): 
        super().__init__("stock", "https://www.stock.com.py")
        
    def category_urls(self):
        r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        patterns = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat.get("include", [])]
        urls = set()
        for a in soup.select('a[href*="/category/"]'):
            href = a["href"].lower()
            if any(k in href for k in patterns):
                urls.add(urljoin(self.base_url, href))
        return list(urls)
        
    def parse_category(self, url):
        r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for p in soup.select("div.product-item"):
            nm = p.select_one("h2.product-title")
            if not nm: 
                continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): 
                continue
            precio = _first_price(p)
            cat, sub = assign_category(nombre)
            if not cat: 
                continue
            rows.append({
                "Supermercado": "Stock",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Categoría": cat,
                "Subcategoría": sub
            })
        return rows

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): 
        super().__init__("superseis", "https://www.superseis.com.py")
        
    def category_urls(self):
        r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        patterns = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat.get("include", [])]
        urls = set()
        for a in soup.select('a.collapsed[href*="/category/"]'):
            href = a["href"].lower()
            if any(k in href for k in patterns):
                urls.add(urljoin(self.base_url, href))
        return list(urls)
        
    def parse_category(self, url):
        r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for a in soup.select("a.product-title-link"):
            nombre = a.get_text(" ", strip=True)
            if is_excluded(nombre): 
                continue
            precio = _first_price(a.find_parent("div", class_="product-item") or a)
            cat, sub = assign_category(nombre)
            if not cat: 
                continue
            rows.append({
                "Supermercado": "Superseis",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Categoría": cat,
                "Subcategoría": sub
            })
        return rows

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): 
        super().__init__("salemma", "https://www.salemmaonline.com.py")
        
    def category_urls(self):
        r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        patterns = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat.get("include", [])]
        urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(k in href for k in patterns): 
                urls.add(urljoin(self.base_url, href))
        return list(urls)
        
    def parse_category(self, url):
        r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for f in soup.select("form.productsListForm"):
            nombre = f.find("input", {"name":"name"}).get("value", "")
            if is_excluded(nombre): 
                continue
            precio = norm_price(f.find("input", {"name":"price"}).get("value", ""))
            cat, sub = assign_category(nombre)
            if not cat: 
                continue
            rows.append({
                "Supermercado": "Salemma",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Categoría": cat,
                "Subcategoría": sub
            })
        return rows

class AreteScraper(HtmlSiteScraper):
    def __init__(self): 
        super().__init__("arete", "https://www.arete.com.py")
        
    def category_urls(self):
        r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for sel in ("#departments-menu", "#menu-departments-menu-1"):
            for a in soup.select(f"{sel} a[href^=\"catalogo/\"]"):
                href = a["href"].split("?")[0].lower()
                patterns = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat.get("include", [])]
                if any(k in href for k in patterns): 
                    urls.add(urljoin(self.base_url+"/", href))
        return list(urls)
        
    def parse_category(self, url):
        r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        for p in soup.select("div.product"):
            nm = p.select_one("h2.ecommercepro-loop-product__title")
            if not nm: 
                continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): 
                continue
            precio = _first_price(p)
            cat, sub = assign_category(nombre)
            if not cat: 
                continue
            rows.append({
                "Supermercado": "Arete",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Categoría": cat,
                "Subcategoría": sub
            })
        return rows

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = "losjardines"
        self.base_url = "https://losjardinesonline.com.py"

class BiggieScraper:
    name, API, TAKE = "biggie", "https://api.app.biggie.com.py/api/articles", 100
    session = _build_session()
    GROUPS = ["carniceria", "panaderia", "huevos", "lacteos"]

    def fetch_group(self, grp):
        rows = []
        skip = 0
        while True:
            try:
                js = self.session.get(self.API, params={"take": self.TAKE, "skip": skip, "classificationName": grp}, timeout=REQ_TIMEOUT).json()
            except:
                break
            items = js.get("items", [])
            if not items:
                break
            for it in items:
                nombre = it.get("name", "")
                if is_excluded(nombre): 
                    continue
                precio = norm_price(it.get("price", 0))
                cat, sub = assign_category(nombre)
                if not cat: 
                    continue
                rows.append({
                    "Supermercado": "Biggie",
                    "CategoríaURL": grp,
                    "Producto": nombre.upper(),
                    "Precio": precio,
                    "Categoría": cat,
                    "Subcategoría": sub
                })
            skip += self.TAKE
            if skip >= js.get("count", 0): 
                break
        return rows

    def scrape(self):
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for g in self.GROUPS:
            for item in self.fetch_group(g):
                item["FechaConsulta"] = fecha
                # Asegurar todos los campos requeridos
                for col in REQUIRED_COLS:
                    if col not in item:
                        item[col] = "" if col != "Precio" else 0.0
                rows.append(item)
        return rows

    def save_csv(self, rows):
        if not rows: 
            return
        fn = f"biggie_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        df = pd.DataFrame(rows)
        # Asegurar columnas requeridas
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = "" if col != "Precio" else 0.0
        df.to_csv(os.path.join(OUT_DIR, fn), index=False)

SCRAPERS: Dict[str, Callable] = {
    "stock": StockScraper,
    "superseis": SuperseisScraper,
    "salemma": SalemmaScraper,
    "arete": AreteScraper,
    "losjardines": JardinesScraper,
    "biggie": BiggieScraper
}

# ────────────────── 6. Google Sheets ───────────────────────────
def _open_sheet():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    
    try:
        cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
        gc = gspread.authorize(cred)
        sh = gc.open_by_url(SPREADSHEET_URL)
        
        try:
            ws = sh.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="40")
            
        df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False)
        df.dropna(how="all", inplace=True)
        
        # Asegurar columnas requeridas en el histórico
        for col in REQUIRED_COLS:
            if col not in df.columns:
                df[col] = ""
        
        return ws, df
        
    except Exception as e:
        print(f"Error al abrir Google Sheets: {e}")
        raise

def _write_sheet(ws, df):
    try:
        # Ordenar columnas según REQUIRED_COLS + otras
        ordered_cols = [c for c in REQUIRED_COLS if c in df.columns] + \
                      [c for c in df.columns if c not in REQUIRED_COLS]
        df = df[ordered_cols]
        
        ws.clear()
        set_with_dataframe(ws, df, include_index=False)
    except Exception as e:
        print(f"Error al escribir en Google Sheets: {e}")
        raise

# ────────────────── 7. Orquestador principal ───────────────────
def _parse_args(args):
    if not args or "all" in args:
        return list(SCRAPERS.keys())
    return [a for a in args if a in SCRAPERS]

def main(argv=None):
    objetivos = _parse_args(argv if argv is not None else sys.argv[1:])
    registros = []
    
    print("\n" + "="*50)
    print(f"Iniciando scraping de {len(objetivos)} supermercados")
    print("="*50 + "\n")
    
    for key in objetivos:
        try:
            scraper = SCRAPERS[key]()
            filas = scraper.scrape()
            scraper.save_csv(filas)
            registros.extend(filas)
            print(f"• {key:<12}: {len(filas):>5} productos válidos")
        except Exception as e:
            print(f"Error en {key}: {e}")
    
    if not registros:
        print("\nNo se encontraron datos nuevos. Saliendo...")
        return 0

    # Procesar archivos CSV generados
    archivos_csv = glob.glob(PATTERN_DAILY)
    if not archivos_csv:
        print("\nNo se encontraron archivos CSV. Saliendo...")
        return 1
        
    try:
        # Leer y combinar todos los CSVs
        dfs = []
        for f in archivos_csv:
            try:
                df = pd.read_csv(f, dtype=str)
                # Asegurar columnas requeridas
                for col in REQUIRED_COLS:
                    if col not in df.columns:
                        df[col] = "" if col != "Precio" else "0.0"
                dfs.append(df)
                print(f"Leído: {f} ({len(df)} filas)")
            except Exception as e:
                print(f"Error leyendo {f}: {e}")
        
        df_all = pd.concat(dfs, ignore_index=True, sort=False)
        
        # Normalizar datos
        df_all["Categoría"] = df_all["Categoría"].fillna("")
        df_all["Subcategoría"] = df_all["Subcategoría"].fillna("")
        df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce").fillna(0.0)
        
        # Abrir Google Sheets
        ws, df_prev = _open_sheet()
        
        # Combinar con datos históricos
        base = pd.concat([df_prev, df_all], ignore_index=True, sort=False)
        
        # Procesar fechas y eliminar duplicados
        base["FechaConsulta"] = pd.to_datetime(base["FechaConsulta"], errors="coerce")
        base.sort_values("FechaConsulta", inplace=True)
        base["FechaConsulta"] = base["FechaConsulta"].dt.strftime("%Y-%m-%d")
        
        # Eliminar duplicados manteniendo la primera aparición
        base.drop_duplicates(subset=KEY_COLS, keep="first", inplace=True)
        
        # Restablecer IDs
        if "ID" in base.columns:
            base.drop(columns=["ID"], inplace=True)
        base.insert(0, "ID", range(1, len(base) + 1))
        
        # Escribir en Google Sheets
        _write_sheet(ws, base)
        
        print("\n" + "="*50)
        print(f"✅ Hoja actualizada exitosamente")
        print(f"Total de registros: {len(base)}")
        print(f"Registros nuevos: {len(df_all)}")
        print("="*50)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
