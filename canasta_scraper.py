# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – versión GitHub Actions (bugs fix 2025‑07‑01)
Autor: Diego B. Meza

Cambios respecto a la versión anterior
──────────────────────────────────────
1. **FechaConsulta** ahora se conserva con fecha + hora (formato ISO) y ya no se
   trunca al sólo día.
2. Se **elimina** completamente la columna *CategoríaURL* (no se almacena en CSV
   ni en la hoja).
3. Mejoras de detección de precio para **Stock** y **Superseis**:
   • Se añade lectura del atributo `data-price` y más selectores.
4. Se asegura que *todas* las filas tengan exactamente las columnas estándar
   `['Supermercado','Producto','Precio','Categoría','Subcategoría','FechaConsulta']`.
"""

import os, sys, glob, re, json, unicodedata, tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from typing import List, Dict, Callable, Set, Tuple

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
CREDS_RAW       = os.getenv("GOOGLE_CREDS")
OUT_DIR         = os.getenv("OUT_DIR", "./csvs")
PATTERN_DAILY   = os.path.join(OUT_DIR, "*_canasta_*.csv")
WORKSHEET_NAME  = "maestro"
MAX_WORKERS, REQ_TIMEOUT = 8, 10

# Esquema estándar
COLUMNS = ["Supermercado", "Producto", "Precio", "Categoría", "Subcategoría", "FechaConsulta"]
KEY_COLS = ["Supermercado", "Producto", "FechaConsulta"]

if not CREDS_RAW:
    raise ValueError("La variable de entorno GOOGLE_CREDS está vacía")
try:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(json.loads(CREDS_RAW), f)
        CREDS_JSON = f.name
except json.JSONDecodeError as e:
    sys.exit(f"GOOGLE_CREDS no es JSON válido: {e}")

os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Helpers texto ─────────────────────

_defpat = re.compile(r"[a-záéíóúñü]+", re.I)

def strip_accents(tx):
    return "".join(ch for ch in unicodedata.normalize("NFD", tx) if unicodedata.category(ch) != "Mn")

def tokenize(tx):
    return [strip_accents(t.lower()) for t in _defpat.findall(tx)]


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

# ────────────────── 3. Scraping utils ─────────────────────

def norm_price(val):
    raw = str(val)
    raw = re.sub(r"[^\d,\.]", "", raw).replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0

_price_selectors = [
    "[data-price]", "[data-price-final]", "[data-price-amount]",
    "meta[itemprop='price']",
    "span.price ins span.amount", "span.price > span.amount", "span.amount",
    "span.woocommerce-Price-amount", "bdi", "span.price", "span.price-new",
    "span.precio", "div.price", "p.price"
]


def _first_price(node: Tag) -> float:
    # 1) atributos en el nodo y sus descendientes
    for attr in ("data-price", "data-price-final", "data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr]):
            return norm_price(node[attr])
        for el in node.select(f"[{attr}]"):
            if norm_price(el[attr]):
                return norm_price(el[attr])
    # 2) meta price
    meta = node.select_one("meta[itemprop='price']")
    if meta and meta.get("content") and norm_price(meta["content"]):
        return norm_price(meta["content"])
    # 3) clásicos
    for sel in _price_selectors[3:]:
        el = node.select_one(sel)
        if el:
            val = el.get_text() or el.get("content", "")
            p = norm_price(val)
            if p:
                return p
    return 0.0

def _session():
    retry = Retry(total=3, backoff_factor=1.2, status_forcelist=(429,500,502,503,504), allowed_methods=("GET","HEAD"))
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s



# ────────────────── 4. Base scraper ─────────────────────
class HtmlSiteScraper:
    def __init__(self, name, base):
        self.name = name
        self.base_url = base.rstrip("/") if base else ""
        self.session = _session()
    def category_urls(self):
        raise NotImplementedError
    def parse_category(self, url):
        raise NotImplementedError
    def scrape(self):
        urls = self.category_urls()
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futs = [pool.submit(self.parse_category, u) for u in urls]
            for fut in as_completed(futs):
                for r in fut.result():
                    r["FechaConsulta"] = ts
                    # Filas con precio 0 se descartan (fuente fallida)
                    if r.get("Precio", 0) == 0:
                        continue
                    rows.append({c: r.get(c, "" if c != "Precio" else 0.0) for c in COLUMNS})
        return rows
    def save_csv(self, rows):
        if rows:
            fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
            pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR, fn), index=False)
           
# ────────────────── 5. Scrapers específicos ─────────────────────

class StockScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("Stock", "https://www.stock.com.py")
    def category_urls(self):
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text, "html.parser")
        kw = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat["include"]]
        return [urljoin(self.base_url, a["href"]) for a in soup.select('a[href*="/category/"]') if any(k in a["href"].lower() for k in kw)]
    def parse_category(self, url):
        soup = BeautifulSoup(self.session.get(url, timeout=REQ_TIMEOUT).content, "html.parser")
        rows = []
        for card in soup.select("div.product-item"):
            name_el = card.select_one("h2.product-title")
            if not name_el: continue
            name = name_el.get_text(" ", strip=True)
            if is_excluded(name): continue
            price = _first_price(card)
            if price == 0: continue
            cat, sub = assign_category(name)
            if not cat: continue
            rows.append({"Supermercado": "Stock", "Producto": name.upper(), "Precio": price, "Categoría": cat, "Subcategoría": sub})
        return rows

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("Superseis", "https://www.superseis.com.py")
    def category_urls(self):
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text, "html.parser")
        kw = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat["include"]]
        return [urljoin(self.base_url, a["href"]) for a in soup.select('a.collapsed[href*="/category/"]') if any(k in a["href"].lower() for k in kw)]
    def parse_category(self, url):
        soup = BeautifulSoup(self.session.get(url, timeout=REQ_TIMEOUT).content, "html.parser")
        rows = []
        for link in soup.select("a.product-title-link"):
            name = link.get_text(" ", strip=True)
            if is_excluded(name): continue
            cont = link.find_parent("div", class_="product-item") or link
            price = _first_price(cont)
            if price == 0: continue
            cat, sub = assign_category(name)
            if not cat: continue
            rows.append({"Supermercado": "Superseis", "Producto": name.upper(), "Precio": price, "Categoría": cat, "Subcategoría": sub})
        return rows

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("Salemma", "https://www.salemmaonline.com.py")
    def category_urls(self):
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text, "html.parser")
        pats = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat["include"]]
        urls = set()
        for a in soup.find_all("a", href=True):
            if any(k in a["href"].lower() for k in pats):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)
    def parse_category(self, url):
        soup = BeautifulSoup(self.session.get(url, timeout=REQ_TIMEOUT).content, "html.parser")
        rows = []
        for form in soup.select("form.productsListForm"):
            name = form.find("input", {"name": "name"}).get("value", "")
            if is_excluded(name): continue
            price = norm_price(form.find("input", {"name": "price"}).get("value", ""))
            cat, sub = assign_category(name)
            if not cat: continue
            rows.append({"Supermercado": "Salemma", "Producto": name.upper(), "Precio": price, "Categoría": cat, "Subcategoría": sub})
        return rows

class AreteScraper(HtmlSiteScraper):
    def __init__(self):
        super().__init__("Arete", "https://www.arete.com.py")
    def category_urls(self):
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text, "html.parser")
        pats = [pat.strip("\\b") for cat in CATEGORY_RULES for pat in cat["include"]]
        urls = set()
        for sel in ("#departments-menu", "#menu-departments-menu-1"):
            for a in soup.select(f"{sel} a[href^=\"catalogo/\"]"):
                if any(k in a["href"].lower() for k in pats):
                    urls.add(urljoin(self.base_url + "/", a["href"].split("?")[0]))
        return list(urls)
    def parse_category(self, url):
        soup = BeautifulSoup(self.session.get(url, timeout=REQ_TIMEOUT).content, "html.parser")
        rows = []
        for card in soup.select("div.product"):
            name_el = card.select_one("h2.ecommercepro-loop-product__title")
            if not name_el: continue
            name = name_el.get_text(" ", strip=True)
            if is_excluded(name): continue
            price = _first_price(card)
            cat, sub = assign_category(name)
            if not cat: continue
            rows.append({"Supermercado": "Arete", "Producto": name.upper(), "Precio": price, "Categoría": cat, "Subcategoría": sub})
        return rows

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = "Jardines"
        self.base_url = "https://losjardinesonline.com.py"

# -------- Biggie (API) --------
class BiggieScraper(HtmlSiteScraper):
    API = "https://api.app.biggie.com.py/api/articles"
    TAKE = 100
    GROUPS = ["carniceria", "panaderia", "huevos", "lacteos"]
    def __init__(self):
        super().__init__("Biggie", "")
        self.session = _build_session()
    def category_urls(self):
        return self.GROUPS  # se usan como tokens
    def parse_category(self, grp):
        rows = []
        skip = 0
        while True:
            js = self.session.get(self.API, params={"take": self.TAKE, "skip": skip, "classificationName": grp}, timeout=REQ_TIMEOUT).json()
            for it in js.get("items", []):
                name = it.get("name", "")
                if is_excluded(name): continue
                price = norm_price(it.get("price", 0))
                cat, sub = assign_category(name)
                if not cat: continue
                rows.append({"Supermercado": "Biggie", "Producto": name.upper(), "Precio": price, "Categoría": cat, "Subcategoría": sub})
            skip += self.TAKE
            if skip >= js.get("count", 0): break
        return rows

# ────────────────── 6. Registro de scrapers ─────────────────────
SCRAPERS: Dict[str, Callable] = {
    "stock": StockScraper,
    "superseis": SuperseisScraper,
    "salemma": SalemmaScraper,
    "arete": AreteScraper,
    "jardines": JardinesScraper,
    "biggie": BiggieScraper
}

# ────────────────── 7. Google Sheets ─────────────────────

def _sheet_open():
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
    ws = None
    cred = Credentials.from_service_account_file(CREDS_JSON, scopes=scopes)
    gc = gspread.authorize(cred)
    sh = gc.open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
        df_prev = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="50")
        df_prev = pd.DataFrame(columns=COLUMNS)
    # limpiar columna obsoleta
    if "CategoríaURL" in df_prev.columns:
        df_prev.drop(columns=["CategoríaURL"], inplace=True)
    return ws, df_prev.reindex(columns=COLUMNS, fill_value="")


def _sheet_write(ws, df):
    ws.clear()
    set_with_dataframe(ws, df[COLUMNS], include_index=False)

# ────────────────── 8. Orquestador principal ─────────────────────

def main(args=None):
    targets = (args or sys.argv[1:]) or list(SCRAPERS)
    rows: List[Dict] = []
    for key in targets:
        scr = SCRAPERS[key]()
        data = scr.scrape()
        scr.save_csv(data)
        rows.extend(data)
        print(f"• {key:<10}: {len(data):>4} filas válidas")
    if not rows:
        print("Sin datos nuevos"); return 0

    dfs = [pd.read_csv(f, dtype=str)[COLUMNS] for f in glob.glob(PATTERN_DAILY)]
    df_all = pd.concat(dfs, ignore_index=True)
    df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce")

    ws, df_prev = _sheet_open()
    base = pd.concat([df_prev, df_all], ignore_index=True)
    base["FechaConsulta"] = pd.to_datetime(base["FechaConsulta"], errors="coerce")
    base.sort_values("FechaConsulta", inplace=True)
    base["FechaConsulta"] = base["FechaConsulta"].dt.strftime("%Y-%m-%d %H:%M:%S")
    base.drop_duplicates(KEY_COLS, keep="first", inplace=True)
    if "ID" in base.columns:
        base.drop(columns="ID", inplace=True)
    base.insert(0, "ID", range(1, len(base)+1))

    _sheet_write(ws, base)
    print(f"✅ Hoja actualizada ({len(base)} registros)")
    return 0

if __name__ == "__main__":
    sys.exit(main())

