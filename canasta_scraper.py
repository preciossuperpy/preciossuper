# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – GitHub Actions (build 2025-07-02-final)
Autor: Diego B. Meza

Este script realiza:
1) Scraping de múltiples supermercados: Stock, Superseis, Salemma, Arete, Jardines y Biggie.
2) Clasificación en Grupo/Subgrupo.
3) Extracción de unidad de medida.
4) Registro de FechaConsulta con fecha+hora+minutos+segundos (UTC).
5) Consolidación de CSVs y actualización en Google Sheets.
Columnas finales:
    ['ID', 'Supermercado', 'Producto', 'Precio', 'Unidad', 'Grupo', 'Subgrupo', 'FechaConsulta']
"""

import os, sys, glob, re, json, unicodedata, tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from typing import List, Dict, Set

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
PATTERN_DAILY   = os.path.join(OUT_DIR, "*_canasta_*.csv")
WORKSHEET_NAME  = "maestro"
MAX_WORKERS, REQ_TIMEOUT = 8, 10

# Columnas estándar
COLUMNS = ["Supermercado", "Producto", "Precio", "Unidad", "Grupo", "Subgrupo", "FechaConsulta"]
KEY_COLS = ["Supermercado", "Producto", "FechaConsulta"]

# Credenciales de Google
if not CREDS_RAW:
    raise ValueError("GOOGLE_CREDS ausente")
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
    json.dump(json.loads(CREDS_RAW), f)
    CREDS_JSON = f.name
os.makedirs(OUT_DIR, exist_ok=True)

# ────────────────── 1. Utilidades de texto ─────────────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)

def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ─────────────── 2. Exclusión productos no deseados ─────────────────
EXCLUDE_WORDS = [
    "medicamento","medicina","pastilla","comprimido","cápsula","jarabe","supositorio","inyección",
    "antibiótico","analgésico","antiinflamatorio","antihistamínico","antidepresivo","vitamina","suplemento",
    "prescripción","receta","farmacia","droguería","fármaco","genérico","marca","aspirina","diclofenac",
    "omeprazol","loratadina","paracetamol","ibuprofeno","amoxicilina","diazepam","simvastatina","metformina",
    "pañal","pañales","toallita","toallitas","algodón","curita","curitas","gasa","gasas","jeringa","jeringas",
    "termómetro","tampón","compresa","protector","incontinencia","papel higiénico",
    "shampoo","champú","acondicionador","jabón","gel","crema","loción","desodorante","antitranspirante",
    "maquillaje","labial","rimel","delineador","sombras","base","polvo","corrector","bronceador","protector solar",
    "perfume","colonia","desmaquillante","cotonete","hisopo","cepillo dental","pasta dental","enjuague bucal",
    "hilo dental","blanqueador dental","prótesis","ortodoncia",
    "preservativo","condón","lubricante","píldora","anticonceptivo","prueba embarazo","tensiómetro","glucómetro",
    "mascarilla","cubrebocas","alcohol","antiséptico","desinfectante","vendaje","tirita","tiritas",
    "pañuelo","pañuelos","toalla","toallas","afeitadora","maquinilla","hoja de afeitar","espuma de afeitar",
    "talco","cuchilla","rasuradora","cortaúñas","lima","tijera","termo","taza","vaso",
    "copa menstrual","bolsa térmica","tinte","coloración","alisador","rizador","secador","plancha","tenacilla",
    "celulitis","corporal","estrias","aciclovir","fusifar","dental","facial","loción"
]
EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_WORDS}

def is_excluded(name: str) -> bool:
    if not name:
        return False
    toks = set(tokenize(name))
    if toks & EXCLUDE_SET:
        return True
    extra = r"\b(crema|gel|loción)\s+(para|de)|\b(jabón|shampoo)\b|\bvitamina\b|\b(medicamento|medicina)\b|\banti\w+|desinfectante"
    return re.search(extra, name.lower()) is not None

# ─────────────── 3. Clasificación Grupo/Subgrupo ─────────────────
BROAD_GROUP_KEYWORDS = {
    "Panificados": ["pan","baguette","bizcocho","galleta","masa"],
    "Frutas":      ["naranja","manzana","banana","pera","uva","frutilla"],
    "Verduras":    ["tomate","cebolla","papa","zanahoria","lechuga","espinaca"],
    "Huevos":      ["huevo","huevos","codorniz"],
    "Lácteos":     ["leche","yogur","queso","manteca","crema"],
}
BROAD_TOKENS = {g: {strip_accents(w) for w in ws} for g, ws in BROAD_GROUP_KEYWORDS.items()}

SUBGROUP_KEYWORDS = {
    "Naranja": ["naranja","naranjas"],
    "Cebolla": ["cebolla","cebollas"],
    "Leche Entera": ["entera"],
    "Leche Descremada": ["descremada"],
    "Queso Paraguay": ["paraguay"],
    "Huevo Gallina": ["gallina"],
    "Huevo Codorniz": ["codorniz"],
}
SUB_TOKENS = {sg: {strip_accents(w) for w in ws} for sg, ws in SUBGROUP_KEYWORDS.items()}

def classify(name: str):
    toks = set(tokenize(name))
    grp = next((g for g, ks in BROAD_TOKENS.items() if toks & ks), "")
    sub = next((sg for sg, ks in SUB_TOKENS.items() if toks & ks), "")
    return grp, sub

# ─────────────── 4. Detección de unidad ─────────────────────────
_unit_re = re.compile(r"(\d+(?:[.,]\d+)?)\s*(kg|g|gr|ml|l|lt|u|paq)\b", re.I)

def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m:
        return ""
    val = m.group(1).replace(",", ".")
    unit = m.group(2).upper()
    if unit == "LT": unit = "L"
    return f"{val}{unit}"

# ─────────────── 5. Precio helper ───────────────────────────
_price_selectors = [
    "[data-price]", "[data-price-final]", "[data-price-amount]",
    "meta[itemprop='price']", "span.price ins span.amount", "span.price > span.amount",
    "span.woocommerce-Price-amount", "span.amount", "bdi", "div.price", "p.price"
]

def norm_price(val) -> float:
    raw = re.sub(r"[^\d,\.]", "", str(val)).replace(".", "").replace(",", ".")
    try: return float(raw)
    except: return 0.0

def _first_price(node: Tag) -> float:
    for attr in ("data-price","data-price-final","data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr]):
            return norm_price(node[attr])
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get("content","")):
        return norm_price(meta.get("content"))
    for sel in _price_selectors[4:]:
        el = node.select_one(sel)
        if el:
            txt = el.get_text() or el.get(sel, "")
            p = norm_price(txt)
            if p: return p
    return 0.0

# ─────────────── 6. HTTP session ───────────────────────────
def _session():
    retry = Retry(total=3, backoff_factor=1.2,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET","HEAD"))
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# ─────────────── 7. Clase base de scraper ───────────────────────────
class HtmlSiteScraper:
    def __init__(self,name: str, base_url: str):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.session = _session()
    def category_urls(self) -> List[str]: raise NotImplementedError
    def parse_category(self, url: str) -> List[Dict]: raise NotImplementedError
    def scrape(self) -> List[Dict]:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows: List[Dict] = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futures = [pool.submit(self.parse_category,u) for u in self.category_urls()]
            for fut in as_completed(futures):
                for r in fut.result():
                    price = r.get("Precio",0)
                    if price <= 0: continue
                    r["FechaConsulta"] = ts
                    rows.append({c:r.get(c, "" if c!="Precio" else 0.0) for c in COLUMNS})
        return rows
    def save_csv(self,rows: List[Dict]):
        if not rows: return
        fn = f"{self.name}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR,fn),index=False)

# ─────────────── 8. Stock ───────────────────────────────
class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("Stock","https://www.stock.com.py")
    def category_urls(self) -> List[str]:
        soup = BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,"html.parser")
        kws = [tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [urljoin(self.base_url,a["href"]) for a in soup.select('a[href*="/category/"]') if any(k in a["href"].lower() for k in kws)]
    def parse_category(self,url: str) -> List[Dict]:
        soup = BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,"html.parser")
        out=[]
        for card in soup.select("div.product-item"):
            title=card.select_one("h2.product-title")
            if not title: continue
            nm=title.get_text(" ",strip=True)
            if is_excluded(nm): continue
            price=_first_price(card)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({"Supermercado":"Stock","Producto":nm.upper(),"Precio":price,"Unidad":unit,"Grupo":grp,"Subgrupo":sub})
        return out

# ─────────────── 9. Superseis ───────────────────────────────
class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("Superseis","https://www.superseis.com.py")
    def category_urls(self) -> List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,"html.parser")
        kws=[tok for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst]
        return [urljoin(self.base_url,a["href"]) for a in Soup.select('a.collapsed[href*="/category/"]') if any(k in a["href"].lower() for k in kws)]
    def parse_category(self,url):
        soup=BeautifulSoup(self.session.get(url,timeout=REQ_TIMEOUT).content,"html.parser")
        out=[]
        for link in soup.select("a.product-title-link"):
            nm=link.get_text(" ",strip=True)
            if is_excluded(nm): continue
            cont=link.find_parent("div",class_="product-item") or link
            price=_first_price(cont)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({"Supermercado":"Superseis","Producto":nm.upper(),"Precio":price,"Unidad":unit,"Grupo":grp,"Subgrupo":sub})
        return out

# ─────────────── 10. Salemma ─────────────────────────────
class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("Salemma","https://www.salemmaonline.com.py")
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,"html.parser")
        urls=set()
        for a in soup.find_all("a",href=True):
            href=a["href"].lower()
            if any(tok in href for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                urls.add(urljoin(self.base_url,href))
        return list(urls)
    def parse_category(self,url)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQU_TIMEOUT).content,"html.parser")
        out=[]
        for form in soup.select("form.productsListForm"):
            nm=form.find("input",{"name":"name"}).get("value","")
            if is_excluded(nm): continue
            price=norm_price(form.find("input",{"name":"price"}).get("value",""))
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({"Supermercado":"Salemma","Producto":nm.upper(),"Precio":price,"Unidad":unit,"Grupo":grp,"Subgrupo":sub})
        return out

# ─────────────── 11. Arete ─────────────────────────────
class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("Arete","https://www.arete.com.py")
    def category_urls(self)->List[str]:
        soup=BeautifulSoup(self.session.get(self.base_url,timeout=REQ_TIMEOUT).text,"html.parser")
        urls=set()
        for sel in ("#departments-menu","#menu-departments-menu-1"):
            for a in soup.select(f"{sel} a[href^='catalogo/']"):
                href=a["href"].split("?")[0].lower()
                if any(tok in href for lst in BROAD_GROUP_KEYWORDS.values() for tok in lst):
                    urls.add(urljoin(self.base_url+"/",href))
        return list(urls)
    def parse_category(self,url)->List[Dict]:
        soup=BeautifulSoup(self.session.get(url,timeout=REQU_TIMEOUT).content,"html.parser")
        out=[]
        for card in soup.select("div.product"):
            el=card.select_one("h2.ecommercepro-loop-product__title")
            if not el: continue
            nm=el.get_text(" ",strip=True)
            if is_excluded(nm): continue
            price=_first_price(card)
            grp,sub=classify(nm)
            if not grp: continue
            unit=extract_unit(nm)
            out.append({"Supermercado":"Arete","Producto":nm.upper(),"Precio":price,"Unidad":unit,"Grupo":grp,"Subgrupo":sub})
        return out

# ─────────────── 12. Jardines ─────────────────────────────
class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name="Jardines"
        self.base_url="https://losjardinesonline.com.py"

# ─────────────── 13. Biggie (API) ─────────────────────────────
class BiggieScraper:
    name="Biggie"; API="https://api.app.biggie.com.py/api/articles"; TAKE=100
    GROUPS=["carniceria","panaderia","huevos","lacteos"]
    def __init__(self): self.session=_session()
    def category_urls(self): return self.GROUPS
    def parse_category(self,grp:str)->List[Dict]:
        out=[];skip=0
        while True:
            js=self.session.get(self.API,params={"take":self.TAKE,"skip":skip,"classificationName":grp},timeout=REQ_TIMEOUT).json()
            for it in js.get("items",[]):
                nm=it.get("name","")
                if is_excluded(nm): continue
                price=norm_price(it.get("price",0))
                if price<=0: continue
                g,sub=classify(nm)
                out.append({"Supermercado":"Biggie","Producto":nm.upper(),"Precio":price,"Unidad":extract_unit(nm),"Grupo":g or grp.capitalize(),"Subgrupo":sub})
            skip+=self.TAKE
            if skip>=js.get("count",0): break
        return out
    def scrape(self)->List[Dict]:
        ts=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows=[]
        for grp in self.GROUPS:
            for r in self.parse_category(grp): r["FechaConsulta"]=ts; rows.append(r)
        return rows
    def save_csv(self,rows):
        if not rows: return
        fn=f"biggie_{datetime.utcnow():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows)[COLUMNS].to_csv(os.path.join(OUT_DIR,fn),index=False)

# ─────────────── 14. Registro de scrapers ─────────────────────────
SCRAPERS: Dict[str, Callable] = {
    "stock": StockScraper,
    "superseis": SuperseisScraper,
    "salemma": SalemmaScraper,
    "arete": AreteScraper,
    "jardines": JardinesScraper,
    "biggie": BiggieScraper,
}

# ─────────────── Funciones Google Sheets y Orquestador ─────────────────
def _open_sheet():
    scopes=["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]
    cred=Credentials.from_service_account_file(CREDS_JSON,scopes=scopes)
    gc=gspread.authorize(cred)
    sh=gc.open_by_url(SPREADSHEET_URL)
    try:
        ws=sh.worksheet(WORKSHEET_NAME)
        df_prev=get_as_dataframe(ws,dtype=str,header=0,evaluate_formulas=False).dropna(how="all")
    except gspread.exceptions.WorksheetNotFound:
        ws=sh.add_worksheet(title=WORKSHEET_NAME,rows="1000",cols="20")
        df_prev=pd.DataFrame(columns=COLUMNS)
    return ws,df_prev

def _write_sheet(ws,df:pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws,df[COLUMNS],include_index=False)

def main():
    all_rows=[]
    for key,cls in SCRAPERS.items():
        inst=cls()
        recs=inst.scrape()
        inst.save_csv(recs)
        all_rows.extend(recs)
    if not all_rows:
        print("Sin datos nuevos.")
        return
    dfs=[pd.read_csv(f,dtype=str)[COLUMNS] for f in glob.glob(PATTERN_DAILY)]
    df_all=pd.concat(dfs,ignore_index=True)
    df_all["Precio"]=pd.to_numeric(df_all["Precio"],errors="coerce").fillna(0.0)
    df_all["FechaConsulta"]=pd.to_datetime(df_all["FechaConsulta"],errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    ws,df_prev=_open_sheet()
    merged=pd.concat([df_prev,df_all],ignore_index=True)
    merged.drop_duplicates(subset=KEY_COLS,keep="first",inplace=True)
    if "ID" in merged.columns:
        merged.drop(columns=["ID"],inplace=True)
    merged.insert(0,"ID",range(1,len(merged)+1))
    _write_sheet(ws,merged)
    print(f"Hoja actualizada: {len(merged)} registros")

if __name__=="__main__":
    main()
