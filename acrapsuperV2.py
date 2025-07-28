# -*- coding: utf-8 -*-
import os, re, unicodedata, sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────── CONFIGURACIÓN GENERAL ───────────────
OUT_CSV_DIR = r"D:\SUPERSCRAP"
os.makedirs(OUT_CSV_DIR, exist_ok=True)
FECHA_HORA = datetime.now().strftime('%Y%m%d_%H%M%S')
OUT_CSV_FILE = os.path.join(OUT_CSV_DIR, f"superscrap_{FECHA_HORA}.csv")
MAX_WORKERS = 8
REQ_TIMEOUT = 20

COLUMNS = [
    'Supermercado', 'Producto', 'Precio', 'Unidad',
    'Subgrupo', 'FechaConsulta',
    'etiquetaunidad', 'cantidad_unidades', 'precio_por_unidad'
]
KEY_COLS = ['Supermercado', 'Producto', 'FechaConsulta']

# ─────────────── UTILIDADES TEXTO ───────────────
_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

# ─────────────── SUBGRUPOS Y PALABRAS CLAVE ───────────────

SUBGROUP_KEYWORDS = {
    # Frutas
    "Naranja": [
        "naranja", "naranjas", "nranja", "narnaja", "narania", "naranja fresca", "naranja kg",
        "naranja unidad", "naranja dulce", "naranja jugo"
    ],
    "Mandarina": [
        "mandarina", "mandarinas", "mandarina kg", "mandarina unidad", "mandarina fresca",
        "mandarinas frescas", "mandarina dulce", "mandarina jugo", "mandarina chica", "mandarina grande"
    ],
    "Manzana": [
        "manzana", "manzanas", "manzana roja", "manzana verde", "manzana kg", "manzana unidad",
        "manzana fresca", "manzanas frescas", "manzana gala", "manzana granny"
    ],
    "Banana": [
        "banana", "bananas", "plátano", "plátanos", "banano", "bananos", "banana madura",
        "banana fresca", "banana kg", "banana unidad"
    ],
    "Pera": [
        "pera", "peras", "pera kg", "pera unidad", "pera fresca", "peras frescas",
        "pera común", "pera williams", "pera mantecosa", "pera dura"
    ],
    "Uva": [
        "uva", "uvas", "uva negra", "uva blanca", "uva rosada", "uva kg", "uva unidad",
        "uvas frescas", "uva sin semilla", "uva con semilla"
    ],
    "Sandía": [
        "sandía", "sandias", "sandia", "sandias frescas", "sandía fresca", "sandía roja",
        "sandía kg", "sandía unidad", "sandía entera", "sandía chica"
    ],
    "Melón": [
        "melón", "melones", "melon", "melón fresco", "melón kg", "melón unidad", "melón blanco",
        "melón amarillo", "melón chica", "melón grande"
    ],
    "Piña": [
        "piña", "ananá", "piñas", "ananás", "piña fresca", "ananá fresca", "piña kg",
        "piña unidad", "ananá unidad", "piña entera"
    ],
    "Limón": [
        "limón", "limones", "limon", "limones frescos", "limón fresco", "limón kg", "limón unidad",
        "limón sutil", "limón común", "limón tahití"
    ],
    "Frutilla": [
        "frutilla", "frutillas", "fresa", "fresas", "frutilla fresca", "fresa fresca", "frutillas frescas",
        "fresas frescas", "frutilla kg", "frutilla caja"
    ],
    "Ciruela": [
        "ciruela", "ciruelas", "ciruela roja", "ciruela negra", "ciruela fresca", "ciruelas frescas",
        "ciruela unidad", "ciruela kg", "ciruela chica", "ciruela grande"
    ],
    "Durazno": [
        "durazno", "duraznos", "melocotón", "melocotones", "durazno fresco", "duraznos frescos",
        "durazno kg", "durazno unidad", "durazno blanco", "durazno amarillo"
    ],
    "Mango": [
        "mango", "mangos", "mango fresco", "mango maduro", "mango verde", "mango kg", "mango unidad",
        "mango chica", "mango grande", "mango ataulfo"
    ],

    # Verduras y hortalizas
    "Cebolla": [
        "cebolla", "cebollas", "cebolla blanca", "cebolla comun", "cebolla morada", "cebolla kg",
        "cebolla unidad", "cebolla fresca", "cebolla grande", "cebolla chica"
    ],
    "Cebolla de Verdeo": [
        "cebolla de verdeo", "cebolla verde", "verdeo", "cebollita de verdeo", "verdeo fresco",
        "cebolla tierna", "cebolla larga", "cebolla de rama", "verdeo unidad", "verdeo manojo"
    ],
    "Papa": [
        "papa", "papas", "papa blanca", "papa negra", "papa común", "papa kg", "papa unidad",
        "papas frescas", "papa chica", "papa grande"
    ],
    "Batata": [
        "batata", "batatas", "boniato", "boniatos", "batata blanca", "batata colorada", "batata kg",
        "batata unidad", "batata fresca", "batata chica"
    ],
    "Zanahoria": [
        "zanahoria", "zanahorias", "zanahoria fresca", "zanahoria kg", "zanahoria unidad", "zanahoria chica",
        "zanahoria grande", "zanahoria limpia", "zanahoria manojo", "zanahoria entera"
    ],
    "Morrón Rojo": [
        "morrón rojo", "pimiento rojo", "morron rojo", "morrones rojos", "pimiento morrón rojo", "morrón rojo fresco",
        "morrón rojo kg", "morrón rojo unidad", "morrón rojo entero", "morrón rojo chico"
    ],
    "Morrón Verde": [
        "morrón verde", "pimiento verde", "morron verde", "morrones verdes", "pimiento morrón verde",
        "morrón verde fresco", "morrón verde kg", "morrón verde unidad", "morrón verde entero", "morrón verde chico"
    ],
    "Lechuga": [
        "lechuga", "lechugas", "lechuga crespa", "lechuga mantecosa", "lechuga romana", "lechuga americana",
        "lechuga hidropónica", "lechuga fresca", "lechuga kg", "lechuga unidad"
    ],
    "Tomate": [
        "tomate", "tomates", "tomate fresco", "tomate perita", "tomate cherry", "tomate roma", "tomate redondo",
        "tomate kg", "tomate unidad", "tomates frescos"
    ],
    "Zapallo": [
        "zapallo", "zapallos", "calabaza", "calabazas", "zapallo anco", "zapallo cabutia", "zapallo kg",
        "zapallo unidad", "zapallo fresco", "zapallo entero"
    ],
    "Remolacha": [
        "remolacha", "remolachas", "remolacha fresca", "remolacha kg", "remolacha unidad", "remolacha entera",
        "remolacha chica", "remolacha grande", "remolacha manojo", "remolacha limpia"
    ],
    "Pepino": [
        "pepino", "pepinos", "pepino fresco", "pepino kg", "pepino unidad", "pepino chico", "pepino grande",
        "pepino entero", "pepino comun", "pepino saladet"
    ],
    "Rúcula": [
        "rúcula", "rucula", "rúcula fresca", "rucula fresca", "rúcula manojo", "rúcula unidad", "rucula unidad",
        "rúcula hidropónica", "rucula baby", "rúcula premium"
    ],
    "Berro": [
        "berro", "berros", "berro fresco", "berro manojo", "berro unidad", "berro chico", "berro grande",
        "berro hidropónico", "berro premium", "berro entero"
    ],
    "Repollo": [
        "repollo", "repollo blanco", "repollo morado", "repollo verde", "repolhos", "repollo fresco",
        "repollo kg", "repollo unidad", "repollo entero", "repollo chico"
    ],
    "Ajo": [
        "ajo", "ajos", "ajo fresco", "ajo en cabeza", "ajo cabeza", "ajo manojo", "ajo unidad",
        "ajo kg", "ajo morado", "ajo blanco"
    ],
    "Apio": [
        "apio", "apios", "apio fresco", "apio manojo", "apio unidad", "apio verde", "apio chico",
        "apio grande", "apio entero", "apio premium"
    ],
    "Espinaca": [
        "espinaca", "espinacas", "espinaca fresca", "espinaca manojo", "espinaca unidad", "espinaca verde",
        "espinaca baby", "espinaca premium", "espinaca entera", "espinaca limpia"
    ],

    "Leche Entera": [
        "leche entera", "entera", "leche entero", "leche sachet entera", "leche fresca entera",
        "leche vaca entera", "leche enter", "leche entero litro", "leche entera litro", "leche entera bolsa"
    ],
    "Leche Descremada": [
        "leche descremada", "descremada", "leche sachet descremada", "leche fresca descremada", "leche des",
        "leche deslactosada", "leche descremada litro", "leche en sachet descremada", "leche vaca descremada", "leche descremada bolsa"
    ],
    "Leche Parcialmente Descremada": [
        "leche parcialmente descremada", "leche semi", "semi descremada", "leche semidescremada", "leche semidesnatada",
        "leche parcialmente desnatada", "leche light", "leche semi descremada", "leche 2%", "leche baja en grasa"
    ],
    "Leche en Polvo": [
        "leche en polvo", "leche polvo", "leche deshidratada", "leche instantánea", "leche polvo entera",
        "leche en polvo descremada", "leche polvo descremada", "leche en polvo fortificada", "leche polvo sachet", "leche polvo bolsa"
    ],
    "Queso Paraguay": [
        "queso paraguay", "queso paraguayo", "paraguay", "queso py", "queso típico", "queso tipo paraguay",
        "queso paraguay fresco", "queso paraguay entero", "queso pguay", "queso py kg"
    ],
    "Queso Mozzarella": [
        "mozzarella", "muzarella", "queso mozzarella", "queso muzarella", "mozzarela", "muzarela", "queso muzzarella",
        "queso mozarela", "queso muzzarela", "queso muzarela"
    ],
    "Queso Tybo": [
        "tybo", "queso tybo", "queso tipo tybo", "tybo fresco", "queso tybo entero", "queso tybo barra",
        "tybo barra", "tybo kg", "tybo sandwich", "queso tybo feteado"
    ],
    "Queso Port Salut": [
        "port salut", "queso port salut", "queso portsalut", "portsalut", "queso tipo port salut",
        "port salut entero", "port salut barra", "port salut feteado", "queso port salut entero", "port salut kg"
    ],
    "Queso Rallado": [
        "queso rallado", "queso rallado en bolsa", "queso rallado fresco", "queso parmesano rallado", "queso rallado sachet",
        "rallado", "queso para rallar", "queso rallado parmesano", "queso rallado reggianito", "queso rallado sobre"
    ],
    "Yogur Entero": [
        "yogur entero", "yogurt entero", "yogur natural", "yogurt natural", "yogur firme", "yogurt firme",
        "yogur tradicional", "yogur entero litro", "yogur entero sachet", "yogur natural entero"
    ],
    "Yogur Descremado": [
        "yogur descremado", "yogurt descremado", "yogur light", "yogur 0%", "yogur sin grasa", "yogur descremado litro",
        "yogur descremado sachet", "yogurt dietético", "yogur bajo en grasa", "yogur descremado natural"
    ],

    # HUEVOS
    "Huevo Gallina": [
        "huevo de gallina", "huevo gallina", "huevos de gallina", "huevos gallina", "huevos blancos",
        "huevo blanco", "huevos frescos", "huevos docena", "huevo docena", "huevo gallina fresco"
    ],
    "Huevo Codorniz": [
        "huevo de codorniz", "huevo codorniz", "huevos de codorniz", "huevos codorniz", "huevito codorniz",
        "huevitos codorniz", "huevo codornis", "huevos codornis", "huevo codorníz", "huevos codorníz"
    ],

    # PANIFICADOS
    "Pan Lactal": [
        "pan lactal", "pan de molde", "pan molde", "pan lactal blanco", "pan lactal integral", "pan lactal kg",
        "pan lactal rebanado", "pan lactal feteado", "pan lactal grande", "pan lactal chico"
    ],
    "Pan Francés": [
        "pan francés", "pan frances", "pan tipo francés", "pan francés chico", "pan francés grande", "pan francés baguette",
        "pan frances crocante", "pan frances tradicional", "pan frances kilo", "pan frances unidad"
    ],
    "Pan de Hamburguesa": [
        "pan de hamburguesa", "pan hamburguesa", "pan para hamburguesa", "pan hamburguesa chico", "pan hamburguesa grande",
        "pan de hamburguesa integral", "pan hamburguesa sésamo", "pan hamburguesa pack", "pan hamburguesa doble", "pan hamburguesa unidad"
    ],
    "Pan de Hot Dog": [
        "pan de hot dog", "pan pancho", "pan para hot dog", "pan para pancho", "pan hot dog", "pan hotdog",
        "pan de pancho", "pan de hotdog", "pan hot dog unidad", "pan pancho unidad"
    ],
    "Galletitas Dulces": [
        "galletita dulce", "galletitas dulces", "galletitas azucaradas", "galletitas de vainilla", "galletitas de coco",
        "galletitas rellenas", "galletitas chocolate", "galletitas saborizadas", "galletitas golosina", "galletita dulce pack"
    ],
    "Galletitas Saladas": [
        "galletita salada", "galletitas saladas", "galletitas crackers", "galletitas agua", "galletitas de salvado",
        "galletitas sal", "galletitas snack", "galletitas light", "galletitas saladas pack", "galletita salada unidad"
    ],

    # CARNES Y DERIVADOS
    "Carne Vacuna": [
        "carne vacuna", "carne de vaca", "carne de res", "carne vacuna molida", "carne vacuna kg", "carne de novillo",
        "carne de ternera", "carne de toro", "carne vacuna fresca", "carne vacuna pulpa"
    ],
    "Carne de Cerdo": [
        "carne de cerdo", "cerdo", "carne porcina", "carne cerdo kg", "cerdo fresco", "cerdo deshuesado",
        "cerdo trozado", "carne de cerdo fresca", "carne cerdo trozo", "cerdo kilo"
    ],
    "Carne de Pollo": [
        "carne de pollo", "pollo", "pollo fresco", "carne pollo kg", "pollo trozado", "pollo entero",
        "pechuga de pollo", "muslo de pollo", "ala de pollo", "pollo deshuesado"
    ],
    "Carne Picada": [
        "carne picada", "picada", "picada común", "picada especial", "picada vacuna", "picada mixta",
        "picada carne", "carne picada vacuna", "picada kilo", "carne molida"
    ],
    "Milanesa de Pollo": [
        "milanesa de pollo", "milanesa pollo", "milanesa de suprema", "milanesa pollo rebozada", "milanesa de pollo fresca",
        "milanesa pollo pack", "milanesa pollo congelada", "milanesa pollo kilo", "milanesa pollo unidad", "milanesa suprema"
    ],
    "Milanesa de Carne": [
        "milanesa de carne", "milanesa carne", "milanesa de vaca", "milanesa vacuna", "milanesa de nalga",
        "milanesa de cuadril", "milanesa especial", "milanesa carne pack", "milanesa carne kilo", "milanesa carne unidad"
    ],

    # EMBUTIDOS Y FIAMBRES
    "Salchicha": [
        "salchicha", "salchichas", "salchicha viena", "salchicha parrillera", "salchicha frankfurt",
        "salchicha tipo viena", "salchicha pack", "salchicha ahumada", "salchicha especial", "salchicha unidad"
    ],
    "Mortadela": [
        "mortadela", "mortadela común", "mortadela especial", "mortadela feteada", "mortadela en barra",
        "mortadela pack", "mortadela ahumada", "mortadela kg", "mortadela unidad", "mortadela sandwiche"
    ],
    "Chorizo": [
        "chorizo", "chorizos", "chorizo parrillero", "chorizo colorado", "chorizo especial", "chorizo ahumado",
        "chorizo criollo", "chorizo bombón", "chorizo picado", "chorizo pack"
    ],
    "Jamón Cocido": [
        "jamón cocido", "jamon cocido", "jamón sandwich", "jamón feteado", "jamón cocido feteado",
        "jamón cocido especial", "jamón cocido pack", "jamón cocido kg", "jamón cocido unidad", "jamón cocido coc"
    ],
    "Jamón Crudo": [
        "jamón crudo", "jamon crudo", "jamón serrano", "jamón ahumado", "jamón ibérico", "jamón crudo feteado",
        "jamón crudo pack", "jamón crudo kg", "jamón crudo unidad", "jamón crudo sandwiche"
    ],

    # BEBIDAS
    "Agua Mineral": [
        "agua mineral", "agua mineral sin gas", "agua mineral con gas", "agua mineral botella",
        "agua mineral litro", "agua mineral bidón", "agua mineral natural", "agua mineral fresca",
        "agua mineral pack", "agua mineral unidad"
    ],
    "Agua Saborizada": [
        "agua saborizada", "agua con sabor", "agua saborizada botella", "agua saborizada litro",
        "agua saborizada sin gas", "agua saborizada con gas", "agua saborizada light", "agua saborizada zero",
        "agua saborizada fresca", "agua saborizada unidad"
    ],
    "Gaseosa": [
        "gaseosa", "refresco", "soda", "bebida gaseosa", "gaseosa cola", "gaseosa botella",
        "gaseosa lata", "gaseosa litro", "gaseosa saborizada", "gaseosa light"
    ],
    "Jugo": [
        "jugo", "jugo de fruta", "zumo", "jugo natural", "jugo exprimido", "jugo en polvo", "jugo concentrado",
        "jugo fresco", "jugo litro", "jugo botella"
    ],

    # OTROS
    "Aceite de Girasol": [
        "aceite de girasol", "aceite girasol", "aceite girasol litro", "aceite girasol botella", "aceite de girasol refinado",
        "aceite de girasol natural", "aceite girasol pack", "aceite girasol kg", "aceite girasol unidad", "aceite de girasol extra"
    ],
    "Aceite de Oliva": [
        "aceite de oliva", "aceite oliva", "aceite oliva virgen", "aceite oliva extra", "aceite de oliva virgen",
        "aceite de oliva extra virgen", "aceite oliva botella", "aceite oliva pack", "aceite oliva unidad", "aceite oliva litro"
    ],
    "Azúcar": [
        "azúcar", "azucar", "azúcar blanca", "azúcar común", "azúcar refinada", "azúcar kg",
        "azúcar paquete", "azúcar unidad", "azúcar morena", "azúcar rubia"
    ],
    "Sal Fina": [
        "sal fina", "sal comun", "sal común", "sal blanca", "sal de mesa", "sal fina yodada",
        "sal fina kg", "sal fina unidad", "sal fina paquete", "sal fina salero"
    ],
    "Sal Gruesa": [
        "sal gruesa", "sal para parrilla", "sal gruesa kg", "sal gruesa unidad", "sal gruesa paquete",
        "sal gruesa marina", "sal gruesa parrillera", "sal gruesa blanca", "sal gruesa natural", "sal gruesa salero"
    ],
    "Arroz": [
        "arroz", "arroz blanco", "arroz largo", "arroz doble", "arroz integral", "arroz parboil", "arroz yamaní",
        "arroz pulido", "arroz kg", "arroz paquete"
    ],
    "Fideo": [
        "fideo", "fideos", "pasta", "fideos secos", "fideos cortos", "fideos largos", "fideos moñito",
        "fideos espagueti", "fideo tallarín", "fideos codito"
    ],
    "Polenta": [
        "polenta", "harina de maíz", "polenta instantánea", "polenta rápida", "polenta lista", "polenta gruesa",
        "polenta fina", "polenta paquete", "polenta kg", "polenta unidad"
    ],
    "Harina de Trigo": [
        "harina de trigo", "harina trigo", "harina trigo 000", "harina trigo 0000", "harina blanca",
        "harina para pan", "harina de trigo común", "harina de trigo leudante", "harina trigo kg", "harina trigo paquete"
    ],
    "Maíz": [
        "maíz", "maiz", "maíz amarillo", "maíz blanco", "maíz fresco", "maíz en grano", "maíz para palomitas",
        "maíz entero", "maíz dulce", "maíz choclo"
    ],
    "Poroto": [
        "poroto", "porotos", "frijol", "frijoles", "poroto negro", "poroto colorado", "poroto blanco",
        "poroto fresco", "poroto en bolsa", "poroto kg"
    ],
    "Lenteja": [
        "lenteja", "lentejas", "lenteja roja", "lenteja verde", "lenteja seca", "lenteja paquete",
        "lenteja kg", "lenteja unidad", "lenteja natural", "lenteja común"
    ],
    "Garbanzos": [
        "garbanzo", "garbanzos", "garbanzo seco", "garbanzo en bolsa", "garbanzo paquete", "garbanzo kg",
        "garbanzo unidad", "garbanzo natural", "garbanzo fresco", "garbanzo cocido"
    ]

}


SUB_TOKENS = {sg: {strip_accents(w) for w in ws} for sg, ws in SUBGROUP_KEYWORDS.items()}
def classify_sub(name: str) -> str:
    toks = set(tokenize(name))
    return next((s for s, ks in SUB_TOKENS.items() if toks & ks), "")

# ─────────────── EXCLUSIONES AVANZADAS ───────────────
CATEGORY_EXCLUDES = {
    "tomate": r"arroz\s+con\s+tomate|en\s+tomate|salsa\s+de\s+tomate|con\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extra",
    "morron": r"salsa|mole|pasta|conserva|encurtid[oa]|en\s+vinagre|en\s+lata|molid[oa]|deshidratad[oa]|congelad[oa]|pulpa|pat[eé]|crema|polvo",
    "cebolla": r"en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|crema|sopa",
    "papa": r"chips|frit[ao]s?|chuñ[oa]|pur[ée]|congelad[oa]|deshidratad[oa]|harina|sopa|snack",
    "zanahoria": r"jug(?:o",
    "lechuga": r"ensalada\s+procesada|mix\s+de\s+ensaladas|congelad[oa]|deshidratad[oa]",
    "remolacha": r"en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jug(?:o",
    "rucula": r"extracto|jugo|con\s+|sabor|pulpa|pur[ée]|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|mermelada|chips|snack|polvo|humo",
    "berro": r"extracto|jugo|con\s+|sabor|pulpa|pur[ée]|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|mermelada|chips|snack|polvo|humo",
    "banana": r"harina|polvo|chips|frit[ao]s?|dulce|mermelada|batido|jugo|snack|pur[ée]|congelad[oa]|deshidratad[oa]",
    "citric": r"jug(?:o",
    "zapallo": r"ensalada|pur[ée]|conserva|crema|snack|congelad[oa]|deshidratad[oa]",
    "pepino": r"encurtid[oa]|conserva|en\s+vinagre|congelad[oa]|deshidratad[oa]",
    "batata": r"chips|frit[ao]s?|harina|pur[ée]|mermelada|congelad[oa]|deshidratad[oa]",
    "repollo": r"en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]",
    "leche": r"en\s+polvo|concentrad[oa]|evaporad[oa]|descremad[oa]",
    "queso": r"rallad[oa]|fundid[oa]|crema|procesad[oa]|untar",
    "yogur": r"azucarad[oa]|batid[oa]|gelatinizad[oa]|saborizad[oa]",
    "panificados": r"tostad[oa]|integral|sin\s+gluten|hornead[oa]",
    "galletitas": r"salad[oa]|dulce|rellen[oa]s?|simple|con\s+chocolate",
    "pan de hamburguesa": r"con\s+semilla|bland[oa]|integral|precortad[oa]",
    "miel": r"procesad[oa]|mezclad[oa]|artificial|con\s+sabor",
    "helado": r"crem[oa]|saborizad[oa]|paleta|conos?",
    "condimento / cubo": r"caldo|condimento|concentrad[oa]|polvo",
    "snacks / mascotas": r"alimento\s+para\s+(?:gato",
}
GENERIC_EXCLUDE = [
    # Exclusión general
    r"\bcombo\b", r"\bpack\b", r"\bkit\b", r"\bdisney\b", r"\bset\b", r"\bpromo\b", r"\bregalo\b", r"\bedición\s+especial\b", r"\bcon\s+regalo\b",
    r"\bvarios\b", r"\bvariedad\b", r"\bmodelos\b", r"\btamaño\b", r"\bfigura\b", r"\bcolección\b", r"\bcumpleaños\b",
    # Bebidas alcohólicas y energéticas
    r"\bbebida\s+alcoh[oó]lica\b", r"\balcohol\b", r"\bcerveza\b", r"\bwhisky\b", r"\bvodka\b", r"\br[oó]n\b", r"\bginebra\b", r"\bvino\b",
    r"\bchampagne\b", r"\bcognac\b", r"\btequila\b", r"\baperitivo\b", r"\bvermouth\b", r"\blicor\b",
    r"\benerg[eé]tica\b", r"\bbebida\s+energ[eé]tica\b", r"\bredbull\b", r"\bmonster\b",
    # Farmacia y cuidado personal
    r"\bfarmacia\b", r"\bmedicamento\b", r"\bmedicamentos\b", r"\bparacetamol\b", r"\bibuprofeno\b", r"\bdoliprane\b", r"\baspirina\b",
    r"\bantigripal\b", r"\bantialérgico\b", r"\bcurita\b", r"\bvenda\b", r"\btermómetro\b", r"\bvitamina\b", r"\bshampoo\b", r"\bcrema\b", r"\bprotector\b", r"\bdesodorante\b", r"\bmaquillaje\b", r"\bperfume\b", r"\bcolonia\b", r"\bpañal\b", r"\bpañales\b", r"\btoallita\b", r"\btoallitas\b", r"\babsorbente\b", r"\balgod[oó]n\b", r"\benjuague\b", r"\benjuague\s+bucal\b",
    # Juguetería
    r"\bjuguete\b", r"\bjuguetes\b", r"\bpeluche\b", r"\bdoll\b", r"\bmuñeca\b", r"\bmuñeco\b", r"\bauto\s+de\s+juguete\b", r"\brompecabezas\b", r"\bpuzzle\b", r"\blego\b", r"\bblock\b", r"\bdidáctico\b", r"\bpatrulla\b", r"\bplaymobil\b", r"\bhot\s+wheels\b", r"\bbarbie\b",
    # Ferretería y herramientas
    r"\bmartillo\b", r"\bclavo\b", r"\bclavos\b", r"\btornillo\b", r"\btornillos\b", r"\bdestornillador\b", r"\bdestornilladores\b", r"\balicate\b", r"\balicates\b", r"\bllave\s+inglesa\b", r"\bcinta\s+métrica\b", r"\bmetro\b", r"\bescuadra\b", r"\btenaza\b", r"\bsierra\b", r"\bserrucho\b", r"\bbroca\b", r"\bbrocas\b", r"\bpinza\b", r"\bpinzas\b", r"\btuerca\b", r"\btuercas\b", r"\bferretería\b", r"\bherramienta\b", r"\bherramientas\b", r"\bmasilla\b", r"\bcemento\b", r"\badhesivo\b",
    # Ejercicio y deporte
    r"\bpelota\b", r"\bpelotas\b", r"\bmanopla\b", r"\bguante\b", r"\bguantes\b", r"\bbicicleta\b", r"\bbicicletas\b", r"\bcasco\b", r"\bpesas\b", r"\bpesa\b", r"\bgimnasia\b", r"\bfitness\b", r"\bcolchoneta\b", r"\bcolchonetas\b", r"\brueda\s+abdominal\b", r"\belástica\b", r"\belastico\b", r"\bconos\b", r"\bconito\b", r"\bconitos\b", r"\bbotín\b", r"\bbotines\b", r"\bskate\b", r"\bpatín\b", r"\bpatines\b", r"\bpatineta\b", r"\braqueta\b", r"\braquetas\b", r"\bpaleta\b", r"\bpaletas\b", r"\bpalo\b", r"\bpalos\b",
    # Regalos y fiestas
    r"\bmoño\b", r"\bmoños\b", r"\bcinta\s+decorativa\b", r"\bglobo\b", r"\bglobos\b", r"\bvela\b", r"\bvelas\b", r"\badornos\b", r"\badorno\b", r"\bcentro\s+de\s+mesa\b", r"\bsouvenir\b", r"\bsouvenirs\b", r"\bconfite\b", r"\bconfites\b", r"\bcaramelera\b", r"\bcarameleras\b", r"\btarjeta\b", r"\btarjetas\b", r"\binvitación\b", r"\binvitaciones\b", r"\bpiñata\b", r"\bpiñatas\b", r"\bbolsa\s+de\s+regalo\b", r"\bbolsas\s+de\s+regalo\b",
    # Cubiertos y utensilios
    r"\btenedor\b", r"\btenedores\b", r"\bcuchara\b", r"\bcucharas\b", r"\bcuchillo\b", r"\bcuchillos\b", r"\bplato\b", r"\bplatos\b", r"\btaza\b", r"\btazas\b", r"\bvaso\b", r"\bvasos\b", r"\bcopa\b", r"\bcopas\b", r"\bfuente\b", r"\bollas?\b", r"\bsarten\b", r"\bsartenes\b", r"\bcacerola\b", r"\bcacerolas\b", r"\bjarra\b", r"\bjarrras\b", r"\btabla\b", r"\btablas\b", r"\bpalita\b", r"\bpalitas\b", r"\bcucharón\b", r"\bcucharones\b", r"\bbandeja\b", r"\bbandejas\b",
    # Librería y papelería
    r"\blapicera\b", r"\blapiceras\b", r"\blápiz\b", r"\blápices\b", r"\bresaltador\b", r"\bresaltadores\b", r"\bmarcador\b", r"\bmarcadores\b", r"\bcuaderno\b", r"\bcuadernos\b", r"\bblock\b", r"\bblocks\b", r"\bfolio\b", r"\bfolios\b", r"\bhoja\b", r"\bhojas\b", r"\bcarpeta\b", r"\bcarpetas\b", r"\bbolígrafo\b", r"\bbolígrafos\b", r"\blibreta\b", r"\blibretas\b", r"\bgoma\b", r"\bgomas\b", r"\bregla\b", r"\bregrlas\b", r"\bcompás\b", r"\bcompases\b", r"\bcorrector\b", r"\bcorrectores\b", r"\bpapel\b", r"\bpapeles\b", r"\bclip\b", r"\bclips\b",
    # Mueblería
    r"\bsilla\b", r"\bsillas\b", r"\bmesa\b", r"\bmesas\b", r"\bsofá\b", r"\bsofa\b", r"\bsofás\b", r"\bsillón\b", r"\bsillones\b", r"\bestante\b", r"\bestantes\b", r"\bplacard\b", r"\bplacares\b", r"\bbiblioteca\b", r"\bbibliotecas\b", r"\blibrero\b", r"\blibreros\b", r"\bcómoda\b", r"\bcomodas\b", r"\bmodular\b", r"\bmodulares\b", r"\bmesa\s+ratona\b", r"\bbanqueta\b", r"\bbanquetas\b", r"\bcama\b", r"\bcamas\b",
    # Ropería/ropa
    r"\bcalza\b", r"\bcalzas\b", r"\brem[ea]\b", r"\brem[ea]s\b", r"\bcamiseta\b", r"\bcamisetas\b", r"\bpantal[óo]n\b", r"\bpantal[óo]nes\b", r"\bbermuda\b", r"\bbermudas\b", r"\bbuzo\b", r"\bbuzos\b", r"\bropa\b", r"\bconjunto\b", r"\bconjuntos\b", r"\bbabucha\b", r"\bbabuchas\b", r"\bchomba\b", r"\bchombas\b", r"\bcorbata\b", r"\bcorbatas\b", r"\bvestido\b", r"\bvestidos\b", r"\bfalda\b", r"\bfaldas\b", r"\bcamisa\b", r"\bcamisas\b", r"\bchaleco\b", r"\bchalecos\b", r"\bmedias\b", r"\bmedia\b", r"\bropa\s+interior\b", r"\bropa\s+deportiva\b", r"\bzapatilla\b", r"\bzapatillas\b", r"\bzapato\b", r"\bzapatos\b", r"\bsandalia\b", r"\bsandalias\b",
    # Tecnología
    r"\btv\b", r"\btelevisor\b", r"\btelevisores\b", r"\btablet\b", r"\btableta\b", r"\btabletas\b", r"\bnotebook\b", r"\bnotebooks\b", r"\blaptop\b", r"\blaptops\b", r"\bcomputadora\b", r"\bcomputadoras\b", r"\bmonitor\b", r"\bmonitores\b", r"\bcelular\b", r"\bcelulares\b", r"\bsmartphone\b", r"\bsmartphones\b", r"\bcargador\b", r"\bcargadores\b", r"\bmouse\b", r"\bteclado\b", r"\bteclados\b", r"\brouter\b", r"\bmodem\b", r"\bmodems\b", r"\bimpresora\b", r"\bimpresoras\b", r"\bscanner\b", r"\bparlante\b", r"\bparlantes\b", r"\baltavoz\b", r"\baltavoces\b", r"\bwebcam\b", r"\bauricular\b", r"\bauriculares\b", r"\bdisco\b", r"\bdiscos\b", r"\bssd\b", r"\bhdd\b", r"\bmemoria\b", r"\bmemorias\b", r"\btarjeta\s+sd\b", r"\btarjetas\s+sd\b", r"\bpendrive\b", r"\bpendrives\b", r"\bbluetooth\b",
    # Ferretería extra
    r"\bclavo\b", r"\bclavos\b", r"\btornillo\b", r"\btornillos\b", r"\bdestornillador\b", r"\bdestornilladores\b", r"\balicate\b", r"\balicates\b", r"\bllave\b", r"\bllaves\b",
    # Otros (varios rubros minoristas no alimentarios)
    r"\bmascota\b", r"\bmascotas\b", r"\balimento\s+para\s+mascota\b", r"\bpienso\b", r"\bjaula\b", r"\bcollar\b", r"\bcorrea\b", r"\bcorreas\b", r"\bpecera\b", r"\bpeceras\b", r"\barenero\b", r"\btransportadora\b", r"\btransportadoras\b", r"\bcomederos\b", r"\bcomedero\b"
]

def build_exclude_regex(name: str) -> re.Pattern:
    patterns = GENERIC_EXCLUDE[:]
    for cat, pat in CATEGORY_EXCLUDES.items():
        if re.search(cat, name, re.I):
            patterns.append(pat)
    return re.compile("|".join(patterns), re.I)
def is_excluded(name: str) -> bool:
    _re = build_exclude_regex(name)
    return bool(_re.search(name))

# ─────────────── EXTRACCIÓN DE UNIDAD Y CAMPOS DERIVADOS ───────────────
_unit_re = re.compile(
    r"""
    (?:
        # Formatos compuestos: 4 x 500 ml, 3 por 250g, 2x1L, etc.
        (?P<qty>\d+)\s*(?:x|por|×)\s*
    )?
    (?P<val>\d+(?:[.,]\d+)?)
    \s*
    (?P<unit>
        kg|kilos?|kilogramos?        # kilogramos
        |g|gr|gramos?                # gramos
        |l|lt|litros?                # litros
        |ml|mililitros?|cc|cm3       # mililitros, centímetros cúbicos
        |unid(?:ad)?s?|u|und|uds?    # unidades
        |paq|paquetes?|pk|packs?|stk # paquetes
    )
    (?=[\s\.\,\-_/)]|$)
    """,
    re.IGNORECASE | re.VERBOSE
)

def extract_unit(name: str) -> str:
    m = _unit_re.search(name)
    if not m:
        return ""
    val = float(m.group('val').replace(',', '.'))
    unit = m.group('unit').lower().rstrip('s')
    if unit in ('kg', 'kilo', 'kilogramo'):
        val *= 1000; unit_out = 'GR'
    elif unit in ('l', 'lt', 'litro'):
        val *= 1000; unit_out = 'ML'
    elif unit in ('g', 'gr', 'gramo'):
        unit_out = 'GR'
    elif unit in ('ml', 'cc', 'mililitro', 'cm3'):
        unit_out = 'ML'
    elif unit in ('u', 'und', 'uds', 'uni', 'unidad'):
        unit_out = 'U'
    elif unit in ('paq', 'paquete', 'pk', 'pack', 'packs'):
        unit_out = 'PAQ'
    else:
        unit_out = unit.upper()
    val_str = str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip('0').rstrip('.')
    return f"{val_str}{unit_out}"

def extract_etiqueta_unidad(unidad: str) -> str:
    if not unidad:
        return ""
    # Limpia y lleva a mayúsculas
    t = unidad.upper().replace(".", "").replace("-", "").replace("_", "").strip()
    # Busca la secuencia de letras finales (etiqueta)
    val_alpha = re.search(r"([A-ZÁÉÍÓÚÜÑ]+[A-ZÁÉÍÓÚÜÑ0-9]*)\s*$", t)
    if not val_alpha:
        return ""
    etiqueta = val_alpha.group(1)
    # Normaliza etiquetas
    etiqueta = re.sub(r"^(UNIDAD(?:ES)?|UNID|UND|UDS?|UNI|U)$", "U", etiqueta)
    etiqueta = re.sub(r"^(PAQUETES?|PAQ|PACKS?|PACK|PK)$", "PAQ", etiqueta)
    etiqueta = re.sub(r"^(KILOS?|KILOGRAMOS?|KG)$", "GR", etiqueta)
    etiqueta = re.sub(r"^(GRAMOS?|G|GR)$", "GR", etiqueta)
    etiqueta = re.sub(r"^(LITROS?|L|LT)$", "L", etiqueta)
    etiqueta = re.sub(r"^(MILILITROS?|ML|CC|CM3)$", "ML", etiqueta)
    etiqueta = re.sub(r"^(STK)$", "U", etiqueta)
    return etiqueta


def extract_cantidad_unidades(unidad: str) -> float:
    if not unidad:
        return 1.0
    # Busca cantidad como prefijo en patrones tipo "4 x", "3 por", "2x", "5×", "4*"
    m = re.match(r"^\s*(\d+)\s*(?:x|por|×|\*)", unidad, re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Alternativa: cantidad explícita antes de unidad (ej: "10 unidades", "3 PACK", etc.)
    m2 = re.match(r"^\s*(\d+)\s*[A-ZÁÉÍÓÚÜÑ]{1,10}\b", unidad, re.IGNORECASE)
    if m2:
        return float(m2.group(1))
    # Alternativa: "unidad" tipo "500ml" (sin cantidad → 1)
    m3 = re.match(r"^\s*(-?\d+(?:[.,]\d+)?)", unidad)
    if m3:
        val = m3.group(1).replace(',', '.')
        try:
            return float(val)
        except Exception:
            return 1.0
    return 1.0


def calcular_precio_por_unidad(precio: float, cantidad: float) -> float:
    if cantidad <= 0:
        return 0.0
    return precio / cantidad

def norm_price(val) -> float:
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace('.', '').replace(',', '.')
    try:
        return float(txt)
    except:
        return 0.0

def _first_price(node: Tag) -> float:
    for attr in ("data-price","data-price-final","data-price-amount"):
        if node.has_attr(attr) and norm_price(node[attr])>0:
            return norm_price(node[attr])
    meta = node.select_one("meta[itemprop='price']")
    if meta and norm_price(meta.get('content',''))>0:
        return norm_price(meta.get('content',''))
    for sel in ["span.price ins span.amount","span.price > span.amount"]:
        el = node.select_one(sel)
        if el:
            p = norm_price(el.get_text() or el.get(sel,''))
            if p>0:
                return p
    return 0.0

def get_fecha_ahora():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# ─────────────── CLASES SCRAPER ───────────────
class StockScraper:
    def __init__(self):
        self.name = 'stock'
        self.base_url = 'https://www.stock.com.py'
        self.session = self._build_session()

    def _build_session(self):
        retry = Retry(
            total=4, backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD")
        )
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "es-ES,es;q=0.9"})
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    def category_urls(self) -> List[str]:
        soup = BeautifulSoup(self.session.get(self.base_url, timeout=REQ_TIMEOUT).text, 'html.parser')
        return [
            urljoin(self.base_url, a['href'])
            for a in soup.select('a[href*="/category/"]')
        ]

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for card in soup.select('div.product-item'):
            el = card.select_one('h2.product-title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            if price <= 0:
                continue
            sub = classify_sub(nm)
            unit = extract_unit(nm)
            etiqueta = extract_etiqueta_unidad(unit)
            cantidad = extract_cantidad_unidades(unit)
            precio_unit = calcular_precio_por_unidad(price, cantidad)
            out.append({
                'Supermercado': self.name,
                'Producto': nm.upper(),
                'Precio': price,
                'Unidad': unit,
                'Subgrupo': sub,
                'FechaConsulta': get_fecha_ahora(),
                'etiquetaunidad': etiqueta,
                'cantidad_unidades': cantidad,
                'precio_por_unidad': round(precio_unit, 3)
            })
        return out

class SuperseisScraper:
    def __init__(self):
        self.name = 'superseis'
        self.base_url = 'https://www.superseis.com.py'
        self.session = StockScraper()._build_session()

    def category_urls(self) -> List[str]:
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        return list({
            urljoin(self.base_url, a["href"])
            for a in soup.find_all("a", href=True, class_="collapsed")
            if "/category/" in a["href"]
        })

    def parse_category(self, url: str) -> List[dict]:
        regs: List[dict] = []
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
        except Exception:
            return regs

        soup = BeautifulSoup(r.content, "html.parser")
        for a in soup.find_all("a", class_="product-title-link"):
            nombre = a.get_text(strip=True)
            if is_excluded(nombre):
                continue
            cont = a.find_parent("div", class_="product-item")
            tag = (cont and cont.find("span", class_="price-label")) or a.find_next("span", class_="price-label")
            precio = norm_price(tag.get_text()) if tag else 0.0
            if precio <= 0:
                continue
            sub = classify_sub(nombre)
            unidad = extract_unit(nombre)
            etiqueta = extract_etiqueta_unidad(unidad)
            cantidad = extract_cantidad_unidades(unidad)
            precio_unit = calcular_precio_por_unidad(precio, cantidad)
            regs.append({
                "Supermercado": self.name,
                "Producto": nombre.upper(),
                "Precio": precio,
                "Unidad": unidad,
                "Subgrupo": sub,
                "FechaConsulta": get_fecha_ahora(),
                "etiquetaunidad": etiqueta,
                "cantidad_unidades": cantidad,
                "precio_por_unidad": round(precio_unit, 3)
            })
        return regs

class SalemmaScraper:
    def __init__(self):
        self.name = 'salemma'
        self.base_url = 'https://www.salemmaonline.com.py'
        self.session = StockScraper()._build_session()

    def category_urls(self) -> List[str]:
        urls = set()
        try:
            resp = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        for a in BeautifulSoup(resp.content, 'html.parser').find_all('a', href=True):
            h = a['href'].lower()
            urls.add(urljoin(self.base_url, h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for f in soup.select('form.productsListForm'):
            nm = f.find('input', {'name': 'name'}).get('value', '')
            if is_excluded(nm):
                continue
            price = norm_price(f.find('input', {'name': 'price'}).get('value', ''))
            sub = classify_sub(nm)
            unit = extract_unit(nm)
            etiqueta = extract_etiqueta_unidad(unit)
            cantidad = extract_cantidad_unidades(unit)
            precio_unit = calcular_precio_por_unidad(price, cantidad)
            out.append({
                'Supermercado': self.name,
                'Producto': nm.upper(),
                'Precio': price,
                'Unidad': unit,
                'Subgrupo': sub,
                'FechaConsulta': get_fecha_ahora(),
                'etiquetaunidad': etiqueta,
                'cantidad_unidades': cantidad,
                'precio_por_unidad': round(precio_unit, 3)
            })
        return out

class AreteScraper:
    def __init__(self):
        self.name = 'arete'
        self.base_url = 'https://www.arete.com.py'
        self.session = StockScraper()._build_session()

    def category_urls(self) -> List[str]:
        urls = set()
        try:
            resp = self.session.get(self.base_url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        for sel in ('#departments-menu', '#menu-departments-menu-1'):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                h = a['href'].split('?')[0].lower()
                urls.add(urljoin(self.base_url + '/', h))
        return list(urls)

    def parse_category(self, url: str) -> List[Dict]:
        try:
            resp = self.session.get(url, timeout=REQ_TIMEOUT)
            resp.raise_for_status()
        except Exception:
            return []
        soup = BeautifulSoup(resp.content, 'html.parser')
        out = []
        for card in soup.select('div.product'):
            el = card.select_one('h2.ecommercepro-loop-product__title')
            if not el:
                continue
            nm = el.get_text(' ', strip=True)
            if is_excluded(nm):
                continue
            price = _first_price(card)
            if price <= 0:
                continue
            sub = classify_sub(nm)
            unit = extract_unit(nm)
            etiqueta = extract_etiqueta_unidad(unit)
            cantidad = extract_cantidad_unidades(unit)
            precio_unit = calcular_precio_por_unidad(price, cantidad)
            out.append({
                'Supermercado': self.name,
                'Producto': nm.upper(),
                'Precio': price,
                'Unidad': unit,
                'Subgrupo': sub,
                'FechaConsulta': get_fecha_ahora(),
                'etiquetaunidad': etiqueta,
                'cantidad_unidades': cantidad,
                'precio_por_unidad': round(precio_unit, 3)
            })
        return out

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = 'losjardines'
        self.base_url = 'https://losjardinesonline.com.py'

class BiggieScraper:
    name = 'biggie'
    API = 'https://api.app.biggie.com.py/api/articles'
    TAKE = 100
    GROUPS = ['huevos', 'lacteos', 'frutas', 'verduras', 'cereales', 'panificados']

    def __init__(self):
        self.session = StockScraper()._build_session()

    def category_urls(self) -> List[str]:
        return self.GROUPS

    def parse_category(self, grp: str) -> List[Dict]:
        out = []
        skip = 0
        while True:
            try:
                js = self.session.get(
                    self.API,
                    params={'take': self.TAKE, 'skip': skip, 'classificationName': grp},
                    timeout=REQ_TIMEOUT
                ).json()
            except Exception:
                break
            for it in js.get('items', []):
                nm = it.get('name', '')
                price = norm_price(it.get('price', 0))
                if price <= 0 or is_excluded(nm):
                    continue
                sub = classify_sub(nm)
                unit = extract_unit(nm)
                etiqueta = extract_etiqueta_unidad(unit)
                cantidad = extract_cantidad_unidades(unit)
                precio_unit = calcular_precio_por_unidad(price, cantidad)
                out.append({
                    'Supermercado': 'biggie',
                    'Producto': nm.upper(),
                    'Precio': price,
                    'Unidad': unit,
                    'Subgrupo': sub,
                    'FechaConsulta': get_fecha_ahora(),
                    'etiquetaunidad': etiqueta,
                    'cantidad_unidades': cantidad,
                    'precio_por_unidad': round(precio_unit, 3)
                })
            skip += self.TAKE
            if skip >= js.get('count', 0):
                break
        return out

# ─────────────── MAIN: EJECUTAR Y GUARDAR ───────────────
def main():
    all_rows = []
    scrapers = [
        StockScraper(),
        SuperseisScraper(),
        SalemmaScraper(),
        AreteScraper(),
        JardinesScraper(),
        BiggieScraper(),
    ]
    for scraper in scrapers:
        print(f"Procesando {scraper.__class__.__name__} ...")
        urls = scraper.category_urls()
        # Biggie usa nombres de grupo, el resto usa URLs
        if scraper.__class__.__name__ == 'BiggieScraper':
            for grp in urls:
                all_rows.extend(scraper.parse_category(grp))
        else:
            with ThreadPoolExecutor(MAX_WORKERS) as pool:
                futures = [pool.submit(scraper.parse_category, url) for url in urls]
                for fut in as_completed(futures):
                    all_rows.extend(fut.result())
    if not all_rows:
        print("No se extrajo ningún producto.")
        return
    df_all = pd.DataFrame(all_rows)
    df_all = df_all[COLUMNS].copy()
    df_all.to_csv(OUT_CSV_FILE, index=False, encoding='utf-8-sig')
    print(f"Archivo generado: {OUT_CSV_FILE}")

if __name__ == "__main__":
    main()
