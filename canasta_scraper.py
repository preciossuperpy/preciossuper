# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – versión GitHub Actions mejorada
Autor: Diego B. Meza · Revisión: 2025-06-27
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
from typing import List, Dict, Sequence, Callable, Set, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
# Obtener variables de entorno
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
CREDS_RAW = os.getenv("GOOGLE_CREDS")  # JSON completo

# Directorio para archivos CSV
OUT_DIR = os.getenv("OUT_DIR", "./csvs")
os.makedirs(OUT_DIR, exist_ok=True)
PATTERN_DAILY = os.path.join(OUT_DIR, "*_canasta_*.csv")
WORKSHEET_NAME = "maestro"

MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]

# Validar y escribir las credenciales
if not CREDS_RAW:
    raise ValueError("No se proporcionaron credenciales de Google (GOOGLE_CREDS)")

try:
    # Validar que es JSON válido
    creds_data = json.loads(CREDS_RAW)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp:
        json.dump(creds_data, temp)
        CREDS_JSON = temp.name
except json.JSONDecodeError as e:
    print(f"Error en formato JSON: {e}")
    print("Fragmento de GOOGLE_CREDS:", CREDS_RAW[:200] + "..." if len(CREDS_RAW) > 200 else CREDS_RAW)
    sys.exit(1)

# ────────────────── 1. Normalización texto ─────────────────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ────────────────── 2. Sistema de exclusión y clasificación mejorada ─────────────────────
# Lista ampliada de palabras excluidas (farmacia, medicamentos, cosméticos, etc.)
EXCLUDE_PRODUCT_WORDS = [
    # Medicamentos y farmacia
    "medicamento", "medicina", "pastilla", "comprimido", "cápsula", "jarabe", "supositorio", "inyección",
    "antibiótico", "analgésico", "antiinflamatorio", "antihistamínico", "antidepresivo", "vitamina", "suplemento",
    "prescripción", "receta", "farmacia", "droguería", "fármaco", "genérico", "marca", "aspirina", "diclofenac",
    "omeprazol", "loratadina", "paracetamol", "ibuprofeno", "amoxicilina", "diazepam", "simvastatina", "metformina",
    
    # Higiene y cuidado personal
    "pañal", "pañales", "toallita", "toallitas", "algodón", "curita", "curitas", "gasa", "gasas", "jeringa", "jeringas",
    "termómetro", "tampón", "compresa", "protector", "incontinencia", "papel higiénico",
    "shampoo", "champú", "acondicionador", "jabón", "gel", "crema", "loción", "desodorante", "antitranspirante",
    "maquillaje", "labial", "rimel", "delineador", "sombras", "base", "polvo", "correcto", "bronceador", "protector solar",
    "perfume", "colonia", "desmaquillante", "cotonete", "hisopo", "cepillo dental", "pasta dental", "enjuague bucal",
    "hilo dental", "blanqueador dental", "prótesis", "ortodoncia",
    "preservativo", "condón", "lubricante", "píldora", "anticonceptivo", "prueba embarazo", "tensiómetro", "glucómetro",
    "mascarilla", "cubrebocas", "alcohol", "antiséptico", "desinfectante", "vendaje", "tirita", "tiritas",
    
    # Varios
    "pañuelo", "pañuelos", "toalla", "toallas", "afeitadora", "maquinilla", "hoja de afeitar", "espuma de afeitar",
    "talco", "cuchilla", "rasuradora", "cortaúñas", "lima", "tijera", "termo", "taza", "vaso",
    "copa menstrual", "bolsa térmica", "tinte", "coloración", "alisa", "rizador", "secador", "plancha", "tenacilla",
    "celulitis", "corporal", "estrias", "aciclovir", "fusifar", "dental", "facial", "locion", "corporal"
]

EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_PRODUCT_WORDS}

def is_excluded(name: str) -> bool:
    """Determina si un producto debe ser excluido por ser de farmacia, cosmético, etc."""
    if not name:
        return False
        
    name_lower = name.lower()
    tokens = set(tokenize(name_lower))
    
    # Exclusión por palabras clave
    if tokens & EXCLUDE_SET:
        return True
        
    # Exclusión por patrones específicos
    exclusion_patterns = [
        r"\b(crema|gel|locion)\s+(para|de)\s+",  # Productos tópicos
        r"\b(jabón|shampoo)\s+(medicado|terapéutico)\b",
        r"\bvitamina\s+\w+\b",  # Cualquier vitamina
        r"\b(medicamento|medicina)\b",
        r"\b(anti\w+|desinfectante)\b"
    ]
    
    return any(re.search(pattern, name_lower) for pattern in exclusion_patterns)

# Sistema mejorado de clasificación con reglas contextuales
CATEGORY_RULES = [
    {
        "name": "Carnicería",
        "include": [
            r"\bcarne\b", r"\bvacuno\b", r"\bcerdo\b", r"\bpollo\b", 
            r"\bpescado\b", r"\bmarisco\b", r"\bhamburguesa\b", 
            r"\bmortadela\b", r"\bchuleta\b", r"\bpechuga\b", r"\bmuslo\b",
            r"\bmilanesa\b", r"\bsalchicha\b", r"\bchorizo\b", r"\bchurrasco\b",
            r"\bbife\b", r"\blomo\b", r"\basado\b", r"\bnalga\b", r"\bcuadril\b"
        ],
        "exclude": [
            r"\bconserva\b", r"\blata\b", r"\benlatado\b", r"\bsalsa\b",
            r"\bgalleta\b", r"\bpascua\b", r"\bchocolate\b", r"\bdulce\b",
            r"\bvegetal\b", r"\bsoja\b", r"\btexturizada\b"
        ]
    },
    {
        "name": "Panadería",
        "include": [
            r"\bpan\b", r"\bbaguette\b", r"\bfactura\b", r"\bmedialuna\b", 
            r"\bcroissant\b", r"\bbizcocho\b", r"\bbudin\b", r"\btorta\b", 
            r"\bgalleta\b", r"\bprepizza\b", r"\bpizza\b", r"\bchipa\b", 
            r"\btostada\b", r"\brosquita\b", r"\bpanqueque\b", r"\bmasa\b",
            r"\bhojaldre\b", r"\bpanettone\b", r"\bpanificado\b"
        ],
        "exclude": [
            r"\bmolde\b", r"\bplástico\b", r"\bmetal\b", r"\bjuguete\b",
            r"\bpascua\b", r"\bchocolate\b", r"\bpintura\b", r"\bbandeja\b",
            r"\bde\s+yeso\b", r"\bpara\s+horno\b"
        ]
    },
    {
        "name": "Huevos",
        "include": [r"\bhuevo\b", r"\bhuevos\b"],
        "exclude": [
            r"\bpascua\b", r"\bchocolate\b", r"\bdulce\b", r"\bjuguete\b",
            r"\bsorpresa\b", r"\bdecorado\b", r"\brelleno\b", r"\bkinde?r\b",
            r"\bde\s+plástico\b", r"\bde\s+chocolate\b"
        ],
        "require_exact": True
    },
    {
        "name": "Lácteos",
        "include": [
            r"\bleche\b", r"\byogur\b", r"\bqueso\b", r"\bmanteca\b", 
            r"\bcrema\b", r"\bmuzzarella\b", r"\bdambo\b", r"\bricotta\b",
            r"\bpostre\b", r"\bflan\b", r"\bdulce de leche\b", r"\bcremoso\b",
            r"\bquesillo\b", r"\bprovoleta\b", r"\byogurt\b", r"\bcrema\s+de\s+leche\b"
        ],
        "exclude": [
            r"\bfacial\b", r"\bcorporal\b", r"\bhidratante\b", r"\bjabón\b",
            r"\bshampoo\b", r"\bacondicionador\b", r"\bmaquillaje\b",
            r"\bdesodorante\b", r"\bprotector\b", r"\bpara\s+manos\b", r"\bpara\s+piel\b"
        ]
    },
    {
        "name": "Verdulería",
        "include": [
            r"\blechuga\b", r"\btomate\b", r"\bcebolla\b", r"\bzanahoria\b",
            r"\bpapa\b", r"\bbatata\b", r"\blimón\b", r"\bnaranja\b",
            r"\bmanzana\b", r"\bbanana\b", r"\bpera\b", r"\bdurazno\b",
            r"\bfrutilla\b", r"\buva\b", r"\bpimiento\b", r"\bpalta\b",
            r"\bajo\b", r"\bperejil\b", r"\blemón\b", r"\bmandarina\b",
            r"\bcebollín\b", r"\bapio\b", r"\brúcula\b", r"\bespinaca\b",
            r"\bbrócoli\b", r"\bcoliflor\b", r"\bzapallo\b", r"\bcalabaza\b"
        ],
        "exclude": [r"\bconserva\b", r"\benlatado\b", r"\bcongelado\b", r"\bsalsa\b"]
    }
]

def assign_group(name: str) -> Optional[str]:
    """Clasificación mejorada con palabras clave contextuales"""
    if not name:
        return None
        
    name_lower = name.lower()
    
    for category in CATEGORY_RULES:
        category_name = category["name"]
        
        # Verificar exclusiones primero
        if any(re.search(excl, name_lower) for excl in category.get("exclude", [])):
            continue
            
        # Manejar casos que requieren coincidencia exacta
        if category.get("require_exact", False):
            for pattern in category["include"]:
                if re.search(rf"\b{pattern}\b", name_lower):
                    return category_name
            continue
            
        # Coincidencia normal
        include_patterns = category["include"]
        if any(re.search(pattern, name_lower) for pattern in include_patterns):
            return category_name
            
    return None

# ────────────────── 3. Funciones auxiliares ─────────────────────
def norm_price(val) -> float:
    if isinstance(val, (int, float)):
        return float(val)
    txt = re.sub(r"[^\d,\.]", "", str(val))
    txt = txt.replace(".", "").replace(",", ".")
    try:  return float(txt)
    except ValueError: return 0.0

def _first_price(node: BeautifulSoup, sels: List[str] = None) -> float:
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
        "Chrome/125.0.0.0 Safari/537.36")
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

# ────────────────── 4. Scrapers HTML ───────────────────────────
KEYWORDS_SUPER = set()
for category in CATEGORY_RULES:
    for pattern in category["include"]:
        KEYWORDS_SUPER.add(pattern.strip('\\b'))

class HtmlSiteScraper:
    def __init__(self, name, base):
        self.name = name
        self.base_url = base.rstrip("/")
        self.session = _build_session()

    def category_urls(self):  
        raise NotImplementedError
        
    def parse_category(self, url): 
        raise NotImplementedError

    def scrape(self):
        urls = self.category_urls()
        if not urls: 
            return []
            
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = [pool.submit(self.parse_category, url) for url in urls]
            for future in as_completed(futures):
                try:
                    results = future.result()
                    for row in results:
                        row["FechaConsulta"] = fecha
                        out.append(row)
                except Exception as e:
                    print(f"Error procesando categoría: {e}")
                    
        return out

    def save_csv(self, rows):
        if not rows: 
            return
            
        fn = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

class StockScraper(HtmlSiteScraper):
    def __init__(self): 
        super().__init__("stock", "https://www.stock.com.py")

    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            print(f"Error en categorías de Stock: {e}")
            return []
            
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        
        for a in soup.select('a[href*="/category/"]'):
            href = a["href"].lower()
            if any(k in href for k in KEYWORDS_SUPER):
                full_url = urljoin(self.base_url, a["href"])
                urls.add(full_url)
                
        return list(urls)

    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            print(f"Error parseando categoría {url}: {e}")
            return []
            
        soup = BeautifulSoup(r.content, "html.parser")
        rows = []
        
        for p in soup.select("div.product-item"):
            nm = p.select_one("h2.product-title")
            if not nm: 
                continue
                
            nombre = nm.get_text(" ", strip=True)
            
            # Excluir productos farmacéuticos y no alimenticios
            if is_excluded(nombre):
                continue
                
            grupo = assign_group(nombre)
            if not grupo:
                continue
                
            precio = _first_price(p, ["span.price-label", "span.price"])
            
            rows.append({
                "Supermercado": "Stock",
                "CategoríaURL": url,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Grupo": grupo
            })
            
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
    def __init__(self): 
        super().__init__()
        self.name="losjardines"
        self.base_url="https://losjardinesonline.com.py"

# ────────────────── 3. Biggie (API) ────────────────────────────
class BiggieScraper:
    name, API, TAKE = "biggie", "https://api.app.biggie.com.py/api/articles", 100
    GROUPS = ["carniceria", "panaderia", "huevos", "lacteos"]
    session = _build_session()

    def fetch_group(self, grp):
        rows = []
        skip = 0
        
        while True:
            try:
                js = self.session.get(
                    self.API, 
                    params={"take": self.TAKE, "skip": skip, "classificationName": grp},
                    timeout=REQ_TIMEOUT
                ).json()
            except Exception as e:
                print(f"Error en Biggie ({grp}): {e}")
                break
                
            items = js.get("items", [])
            if not items:
                break
                
            for it in items:
                nombre = it.get("name", "")
                
                # Excluir productos farmacéuticos
                if is_excluded(nombre):
                    continue
                    
                precio = norm_price(it.get("price", 0))
                grupo = assign_group(nombre) or grp.capitalize()
                
                rows.append({
                    "Supermercado": "Biggie",
                    "CategoríaURL": grp,
                    "Producto": nombre.upper(),
                    "Precio": precio,
                    "Grupo": grupo
                })
                
            skip += self.TAKE
            if skip >= js.get("count", 0):
                break
                
        return rows

    def scrape(self):
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        
        for g in self.GROUPS:
            try:
                for item in self.fetch_group(g):
                    item["FechaConsulta"] = fecha
                    rows.append(item)
            except Exception as e:
                print(f"Error en grupo {g} de Biggie: {e}")
                
        return rows

    def save_csv(self, rows):
        if not rows:
            return
            
        fn = f"biggie_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

# ────────────────── 5. Gestor de sitios ────────────────────────
SCRAPERS: Dict[str, Callable] = {
    "stock": StockScraper,
    "superseis": SuperseisScraper,
    "salemma": SalemmaScraper,
    "arete": AreteScraper,
    "losjardines": JardinesScraper,
    "biggie": BiggieScraper
}

def _parse_args(argv=None):
    if argv is None: 
        return list(SCRAPERS)
        
    if any(a in ("-h", "--help") for a in argv):
        print("Uso: python script.py [sitio1 sitio2 …]")
        sys.exit(0)
        
    return [a for a in argv if a in SCRAPERS] or list(SCRAPERS)

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
        
        return ws, df
        
    except Exception as e:
        print(f"Error al abrir Google Sheets: {e}")
        raise

def _write_sheet(ws, df):
    try:
        ws.clear()
        set_with_dataframe(ws, df, include_index=False)
    except Exception as e:
        print(f"Error al escribir en Google Sheets: {e}")
        raise

# ────────────────── 7. Orquestador principal ───────────────────
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
                dfs.append(df)
                print(f"Leído: {f} ({len(df)} filas)")
            except Exception as e:
                print(f"Error leyendo {f}: {e}")
        
        df_all = pd.concat(dfs, ignore_index=True, sort=False)
        
        # Normalizar datos
        df_all["Grupo"] = df_all["Grupo"].map(strip_accents).fillna("")
        df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce")
        
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
