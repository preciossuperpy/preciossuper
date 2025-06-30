# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – versión GitHub Actions (2025‑07‑02)
Autor: Diego B. Meza

Correcciones sobre la versión anterior:
1. FechaConsulta incluye fecha+hora+minuto+segundo en UTC (ISO).
2. Se elimina la columna CategoríaURL en CSV y Sheets.
3. Todas las filas usan columnas estándar:
   ['ID','Supermercado','Producto','Precio','Unidad','Categoría','Subcategoría','FechaConsulta']
4. Detección de precio mejorada para Stock y Superseis.
"""
import os
import sys
import glob
import re
import json
import unicodedata
import tempfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from typing import List, Dict, Set, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ─────────────────── 0. Configuración ────────────────────
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
CREDS_RAW       = os.getenv("GOOGLE_CREDS")
OUT_DIR         = os.getenv("OUT_DIR", "./csvs")
PATTERN_DAILY   = os.path.join(OUT_DIR, "*canasta_*.csv")
WORKSHEET_NAME  = os.getenv("WORKSHEET_NAME", "maestro")
MAX_WORKERS, REQ_TIMEOUT = 8, 10

# Columnas y claves
COLUMNS = ["ID","Supermercado","Producto","Precio","Unidad","Categoría","Subcategoría","FechaConsulta"]
KEY_COLS = ["Supermercado","Producto","FechaConsulta"]

# Validar credenciales Google
if not CREDS_RAW:
    raise ValueError("GOOGLE_CREDS no proporcionado")
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
    json.dump(json.loads(CREDS_RAW), f)
    CREDS_JSON = f.name
os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)

def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt)
                   if unicodedata.category(c) != "Mn")

def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ────────────────── 2. Exclusión productos ─────────────────────
EXCLUDE_WORDS = [
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

EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_WORDS}

def is_excluded(name: str) -> bool:
    if not name: return False
    toks = set(tokenize(name))
    if toks & EXCLUDE_SET: return True
    return False

# ────────────────── 3. Clasificación Grupo/Subgrupo ─────────────────────
BROAD_GROUP = {
    "Panificados":    ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":         ["naranja","manzana","banana","pera","uva","frutilla"],
    "Verduras":       ["tomate","cebolla","papa","zanahoria","lechuga","espinaca"],
    "Huevos":         ["huevo","huevos","codorniz"],
    "Lácteos":        ["leche","yogur","queso","manteca","crema"],
}
BROAD_TOK = {g:{strip_accents(w) for w in ws} for g,ws in BROAD_GROUP.items()}

SUB_GROUP = {
    "Naranja":        ["naranja","naranjas"],
    "Cebolla":        ["cebolla","cebollas"],
    "Leche Entera":   ["entera"],
    "Leche Descremada":["descremada"],
    "Queso Paraguay": ["paraguay"],
    "Huevo Gallina":  ["gallina"],
    "Huevo Codorniz": ["codorniz"],
}
SUB_TOK = {sg:{strip_accents(w) for w in ws} for sg,ws in SUB_GROUP.items()}

def classify(name: str) -> Tuple[str,str]:
    toks = set(tokenize(name))
    grp = next((g for g,ks in BROAD_TOK.items() if toks & ks), "")
    sub = next((s for s,ks in SUB_TOK.items() if toks & ks), "")
    return grp, sub

# ────────────────── 4. Extracción Unidad ─────────────────────
_unit_re = re.compile(r"(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|g|ml|l|lt|u|paq)", re.I)

def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m: return ""
    val = float(m.group('val').replace(',','.'))
    unit = m.group('unit').lower()
    if unit in ('kg',): val*=1000; unit_out='GR'
    elif unit in ('l','lt'): val*=1000; unit_out='CC'
    elif unit=='g': unit_out='GR'
    elif unit=='ml': unit_out='CC'
    else: unit_out=unit.upper()
    val_str = str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip('0').rstrip('.')
    return f"{val_str}{unit_out}"

# ────────────────── 5. Precio ─────────────────────
_price_sel = [
    "[data-price]","[data-price-final]","[data-price-amount]",
    "meta[itemprop='price']","span.price ins span.amount","span.price > span.amount",
    "span.woocommerce-Price-amount","span.amount","bdi","div.price","p.price"
]

def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try: return float(txt)
    except: return 0.0

from bs4.element import Tag

def _first_price(node: Tag) -> float:
    for attr in ("data-price","data-price-final","data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr])>0:
            return norm_price(node[attr])
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get('content',''))>0:
        return norm_price(meta['content'])
    for sel in _price_sel:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get(el.name,''))
            if p>0: return p
    return 0.0

# ────────────────── 6. HTTP Session ─────────────────────
def _session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.2, status_forcelist=(429,500,502,503,504), allowed_methods=("GET","HEAD"))
    s = requests.Session()
    s.headers['User-Agent']='Mozilla/5.0'
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://',adapter); s.mount('https://',adapter)
    return s

# ────────────────── 7. Base Scraper ─────────────────────
class HtmlSiteScraper:
    def __init__(self,name:str,base:str):
        self.name=name; self.base_url=base.rstrip('/'); self.session=_session()
    def category_urls(self) -> List[str]: raise NotImplementedError
    def parse_category(self,url:str) -> List[Dict]: raise NotImplementedError
    def scrape(self) -> List[Dict]:
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        rows=[]
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            for fut in as_completed([pool.submit(self.parse_category,u) for u in self.category_urls()]):
                for r in fut.result():
                    price=r.get('Precio',0)
                    if price<=0: continue
                    r['FechaConsulta']=ts
                    out={col: r.get(col,'') for col in COLUMNS}
                    out['Precio']=price
                    rows.append(out)
        return rows
    def save_csv(self,rows:List[Dict]):
        if not rows: return
        fn=f"{self.name}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR,fn),index=False)

# ────────────────── 8. Scrapers específicos ─────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('Stock','https://www.stock.com.py')
    def category_urls(self):
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for ws in BROAD_GROUP.values() for tok in ws]
        return [urljoin(self.base_url,a['href']) for a in soup.select('a[href*="/category/"]') if any(k in a['href'].lower() for k in kws)]
    def parse_category(self,url):
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for card in soup.select('div.product-item'):
            nm=card.select_one('h2.product-title');
            if not nm: continue
            name=nm.get_text(' ',strip=True)
            if is_excluded(name): continue
            price=_first_price(card)
            grp,sub=classify(name)
            if not grp: continue
            unit=extract_unit(name)
            out.append({'Supermercado':'Stock','Producto':name.upper(),'Precio':price,'Unidad':unit,'Categoría':grp,'Subcategoría':sub})
        return out

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('Superseis','https://www.superseis.com.py')
    def category_urls(self):
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for ws in BROAD_GROUP.values() for tok in ws]
        return [urljoin(self.base_url,a['href']) for a in soup.select('a.collapsed[href*="/category/"]') if any(k in a['href'].lower() for k in kws)]
    def parse_category(self,url):
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for link in soup.select('a.product-title-link'):
            nm=link.get_text(' ',strip=True)
            if is_excluded(nm): continue
            parent=link.find_parent('div.product-item') or link
            price=_first_price(parent)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({'Supermercado':'Superseis','Producto':nm.upper(),'Precio':price,'Unidad':unit,'Categoría':grp,'Subcategoría':sub})
        return out

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('Salemma','https://www.salemmaonline.com.py')
    def category_urls(self):
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for ws in BROAD_GROUP.values() for tok in ws]
        urls=set()
        for a in soup.find_all('a',href=True):
            if any(k in a['href'].lower() for k in kws): urls.add(urljoin(self.base_url,a['href']))
        return list(urls)
    def parse_category(self,url):
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for frm in soup.select('form.productsListForm'):
            name=frm.find('input',{'name':'name'}).get('value','')
            if is_excluded(name): continue
            price=norm_price(frm.find('input',{'name':'price'}).get('value',''))
            grp,sub=classify(name)
            if not grp: continue
            unit=extract_unit(name)
            out.append({'Supermercado':'Salemma','Producto':name.upper(),'Precio':price,'Unidad':unit,'Categoría':grp,'Subcategoría':sub})
        return out

class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__('Arete','https://www.arete.com.py')
    def category_urls(self):
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,'html.parser')
        kws=[tok for ws in BROAD_GROUP.values() for tok in ws]
        urls=set()
        for sel in ('#departments-menu','#menu-departments-menu-1'):
            for a in soup.select(f"{sel} a[href^=\"catalogo/\"]"):
                href=a['href'].split('?')[0].lower()
                if any(k in href for k in kws): urls.add(urljoin(self.base_url+'/',href))
        return list(urls)
    def parse_category(self,url):
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,'html.parser')
        out=[]
        for card in soup.select('div.product'):
            nm=card.select_one('h2.ecommercepro-loop-product__title')
            if not nm: continue
            name=nm.get_text(' ',strip=True)
            if is_excluded(name): continue
            price=_first_price(card)
            grp,sub=classify(name)
            if not grp: continue
            unit=extract_unit(name)
            out.append({'Supermercado':'Arete','Producto':name.upper(),'Precio':price,'Unidad':unit,'Categoría':grp,'Subcategoría':sub})
        return out

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name='Jardines'
        self.base_url='https://losjardinesonline.com.py'

class BiggieScraper(HtmlSiteScraper):
    API='https://api.app.biggie.com.py/api/articles'
    TAKE=100
    GROUPS=['carniceria','panaderia','huevos','lacteos']
    def __init__(self): super().__init__('Biggie','')
    def category_urls(self): return self.GROUPS
    def parse_category(self,grp):
        out=[]; skip=0
        while True:
            js=self.session.get(self.API,params={'take':self.TAKE,'skip':skip,'classificationName':grp},timeout=REQ_TIMEOUT).json()
            for it in js.get('items',[]):
                name=it.get('name','')
                if is_excluded(name): continue
                price=norm_price(it.get('price',0))
                grp2,sub=classify(name)
                cat=grp2 or grp.capitalize()
                if price<=0: continue
                unit=extract_unit(name)
                out.append({'Supermercado':'Biggie','Producto':name.upper(),'Precio':price,'Unidad':unit,'Categoría':cat,'Subcategoría':sub})
            skip+=self.TAKE
            if skip>=js.get('count',0): break
        return out

SCRAPERS: Dict[str,Callable] = {
    'stock': StockScraper,
    'superseis': SuperseisScraper,
    'salemma': SalemmaScraper,
    'arete': AreteScraper,
    'jardines': JardinesScraper,
    'biggie': BiggieScraper
}

# ────────────────── 9. Google Sheets ─────────────────────
def _open_sheet():
    scopes=["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]
    cred=Credentials.from_service_account_file(CREDS_JSON,scopes=scopes)
    gc=gspread.authorize(cred)
    sh=gc.open_by_url(SPREADSHEET_URL)
    try:
        ws=sh.worksheet(WORKSHEET_NAME)
        df=get_as_dataframe(ws,dtype=str,header=0,evaluate_formulas=False).dropna(how='all')
    except gspread.exceptions.WorksheetNotFound:
        ws=sh.add_worksheet(title=WORKSHEET_NAME,rows='10000',cols='20')
        df=pd.DataFrame(columns=COLUMNS)
    return ws,df

def _write_sheet(ws,df:pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws,df[COLUMNS],include_index=False)

# ────────────────── 10. Orquestador ─────────────────────
def main():
    all_rows=[]
    for key,cls in SCRAPERS.items():
        inst=cls()
        recs=inst.scrape()
        inst.save_csv(recs)
        all_rows.extend(recs)
    if not all_rows:
        print('Sin datos nuevos.'); return
    df_all=pd.concat([pd.read_csv(f,dtype=str)[COLUMNS] for f in glob.glob(PATTERN_DAILY)],ignore_index=True)
    df_all['Precio']=pd.to_numeric(df_all['Precio'],errors='coerce').fillna(0.0)
    ws,df_prev=_open_sheet()
    base=pd.concat([df_prev,df_all],ignore_index=True)
    base['FechaConsulta']=pd.to_datetime(base['FechaConsulta'],errors='coerce')
    base['FechaConsulta']=base['FechaConsulta'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    base.drop_duplicates(subset=KEY_COLS,keep='first',inplace=True)
    if 'ID' in base.columns: base.drop(columns=['ID'],inplace=True)
    base.insert(0,'ID',range(1,len(base)+1))
    _write_sheet(ws,base)
    print(f'Hoja actualizada: {len(base)} registros')

if __name__=='__main__':
    main()
