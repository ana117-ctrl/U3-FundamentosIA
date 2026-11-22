#!/usr/bin/env python3
"""
Crawler de PelisPop: construye un dataset (titulo, resena, genero) de 500-1000 películas.

Estrategia:
  1. Parte de la URL de entrada y de los listados (/peliculas, /peliculas/populares).
  2. Recorre en anchura (BFS) las fichas /pelicula/... y los listados /generos/...,
     siguiendo solo enlaces internos del dominio.
  3. Si la cola se agota antes de llegar al mínimo, usa sitemap.xml como respaldo.
  4. Solo se guardan registros con los 3 campos presentes (si falta alguno, se descarta).
"""

import csv
import logging
import random
import re
import time
from collections import Counter, deque
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------- CONFIGURACIÓN ------------------------------
BASE = "https://pelispop.mov"
START_URL = f"{BASE}/pelicula/barbie-in-the-nutcracker-DVgPW9"
SEED_LISTINGS = [f"{BASE}/peliculas", f"{BASE}/generos/animacion"]

MIN_MOVIES = 500          # mínimo deseado (solo se avisa si no se alcanza)
MAX_MOVIES = 700           # el crawler se detiene al llegar aquí
MAX_REQUESTS = 3000         # tope de seguridad de peticiones totales
DELAY_RANGE = (1.0, 2.5)    # pausa aleatoria (segundos) entre peticiones
TIMEOUT = 20
OUTPUT_CSV = "peliculas.csv"
LOG_FILE = "crawler.log"
GENRES_SEPARATOR = ";"      # varios géneros en una sola celda (conserva 3 columnas)
MIN_SYNOPSIS_CHARS = 30     # sinopsis más corta se considera inválida

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (compatible; DatasetCrawler/1.0; "
                   "proyecto academico; contacto: tu-correo@ejemplo.com)"),
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"),
              logging.StreamHandler()],
)
log = logging.getLogger("crawler")


# ------------------------------ UTILIDADES --------------------------------
def make_session() -> requests.Session:
    """Sesión con reintentos automáticos ante errores temporales (429/5xx)."""
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=1.5,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


def normalize_url(url: str) -> str:
    """Quita el fragmento (#...) y la barra final para comparar URLs."""
    url, _ = urldefrag(url)
    return url.rstrip("/")


def same_domain(url: str) -> bool:
    return urlparse(url).netloc.lower().endswith("pelispop.mov")


def is_movie_url(url: str) -> bool:
    """Ficha de película: /pelicula/<slug>-<id> (exactamente 2 segmentos)."""
    parts = [p for p in urlparse(url).path.split("/") if p]
    return len(parts) == 2 and parts[0] == "pelicula"


def is_listing_url(url: str) -> bool:
    """Listados útiles para descubrir películas: catálogo y géneros."""
    path = urlparse(url).path
    return path.startswith("/peliculas") or path.startswith("/generos/")


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


# ------------------------------ CRAWLER -----------------------------------
class Crawler:
    def __init__(self):
        self.session = make_session()
        self.robots = RobotFileParser()
        self.crawl_delay = None
        self.requests_made = 0
        self.queue = deque()
        self.seen = set()           # URLs ya encoladas o visitadas
        self.seen_titles = set()    # títulos normalizados (dedupe por título)
        self.records = []           # [{'titulo','resena','genero','url'}]
        self.discarded = Counter()  # motivo -> cantidad

    # ---- robots.txt: respetamos lo que el sitio indique ----
    def load_robots(self):
        try:
            r = self.session.get(f"{BASE}/robots.txt", timeout=TIMEOUT)
            if r.status_code == 200:
                self.robots.parse(r.text.splitlines())
                self.crawl_delay = self.robots.crawl_delay(HEADERS["User-Agent"])
                log.info("robots.txt cargado (crawl-delay=%s)", self.crawl_delay)
            else:
                self.robots.parse([])
                log.info("robots.txt no disponible (HTTP %s)", r.status_code)
        except requests.RequestException as e:
            self.robots.parse([])
            log.warning("No se pudo leer robots.txt: %s", e)

    def allowed(self, url: str) -> bool:
        return self.robots.can_fetch(HEADERS["User-Agent"], url)

    # ---- descarga con rate limiting y manejo de errores ----
    def fetch(self, url: str):
        """Devuelve BeautifulSoup o None. Nunca lanza excepciones."""
        if not self.allowed(url):
            log.info("Bloqueada por robots.txt: %s", url)
            self.discarded["robots"] += 1
            return None
        if self.requests_made >= MAX_REQUESTS:
            return None

        # Pausa ANTES de cada petición para no sobrecargar el servidor
        time.sleep(max(random.uniform(*DELAY_RANGE), self.crawl_delay or 0))
        self.requests_made += 1
        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                log.warning("HTTP %s en %s", r.status_code, url)
                self.discarded["http_error"] += 1
                return None
            if "html" not in r.headers.get("Content-Type", ""):
                return None
            r.encoding = r.encoding or "utf-8"
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning("Error de red en %s: %s", url, e)
            self.discarded["network_error"] += 1
            return None
        except Exception as e:  # parseo u otro error inesperado
            log.error("Error inesperado en %s: %s", url, e)
            self.discarded["unexpected"] += 1
            return None

    # ---- encolado de enlaces internos ----
    def enqueue(self, url: str):
        url = normalize_url(url)
        if url in self.seen or not same_domain(url):
            return
        if is_movie_url(url) or is_listing_url(url):
            self.seen.add(url)
            self.queue.append(url)

    def extract_links(self, soup: BeautifulSoup, page_url: str):
        """Encola fichas de película, listados de género y paginación."""
        for a in soup.find_all("a", href=True):
            self.enqueue(urljoin(page_url, a["href"]))

    # ---- parseo de una ficha de película ----
    def parse_movie(self, soup: BeautifulSoup, url: str):
        """
        Devuelve dict con titulo/resena/genero, o None si falta algún campo.
        Los 3 campos son obligatorios: si falta uno, se DESCARTA la página
        (no guardamos celdas vacías, para que el dataset sea utilizable).
        """
        h1 = soup.find("h1")
        titulo = clean(h1.get_text()) if h1 else ""

        genres, resena = [], ""
        if h1:
            # Recorremos los elementos que siguen al <h1> hasta el primer <h2>:
            # ahí están los enlaces de género (/generos/<slug>) y luego la sinopsis.
            for el in h1.next_elements:
                name = getattr(el, "name", None)
                if name == "h2":
                    resena = clean(el.get_text())
                    break
                if name == "a" and "/generos/" in (el.get("href") or ""):
                    g = clean(el.get_text())
                    if g and g not in genres:
                        genres.append(g)
                if name in ("h3", "h4"):   # ya salimos de la cabecera de la ficha
                    break

        # Respaldo: la meta description (suele venir truncada)
        if not resena:
            meta = soup.find("meta", attrs={"name": "description"})
            resena = clean(meta["content"]) if meta and meta.get("content") else ""

        genero = GENRES_SEPARATOR.join(genres)

        if not titulo:
            return self._discard("sin_titulo", url)
        if len(resena) < MIN_SYNOPSIS_CHARS:
            return self._discard("sin_resena", url)
        if not genero:
            return self._discard("sin_genero", url)
        return {"titulo": titulo, "resena": resena, "genero": genero, "url": url}

    def _discard(self, reason: str, url: str):
        self.discarded[reason] += 1
        log.warning("Descartada (%s): %s", reason, url)
        return None

    # ---- respaldo: sitemap.xml ----
    def seed_from_sitemap(self):
        log.info("Cola agotada: usando sitemap.xml como respaldo")
        pending, visited, added = [f"{BASE}/sitemap.xml"], set(), 0
        while pending and len(visited) < 20:
            sm = pending.pop(0)
            if sm in visited:
                continue
            visited.add(sm)
            time.sleep(random.uniform(*DELAY_RANGE))
            try:
                r = self.session.get(sm, timeout=TIMEOUT)
                r.raise_for_status()
            except requests.RequestException as e:
                log.warning("Sitemap fallido %s: %s", sm, e)
                continue
            for loc in re.findall(r"<loc>\s*(.*?)\s*</loc>", r.text):
                if loc.endswith(".xml") and same_domain(loc):
                    pending.append(loc)
                elif same_domain(loc) and is_movie_url(loc):
                    before = len(self.queue)
                    self.enqueue(loc)
                    added += len(self.queue) - before
        log.info("Sitemap aportó %d URLs de películas", added)

    # ---- bucle principal ----
    def run(self):
        self.load_robots()
        for u in [START_URL] + SEED_LISTINGS:
            self.enqueue(u)

        used_sitemap = False
        while len(self.records) < MAX_MOVIES and self.requests_made < MAX_REQUESTS:
            if not self.queue:
                if used_sitemap:
                    break
                used_sitemap = True
                self.seed_from_sitemap()
                continue

            url = self.queue.popleft()
            soup = self.fetch(url)
            if soup is None:
                continue

            try:
                if is_movie_url(url):
                    rec = self.parse_movie(soup, url)
                    if rec:
                        key = rec["titulo"].casefold()
                        if key in self.seen_titles:          # dedupe por título
                            self.discarded["titulo_duplicado"] += 1
                        else:
                            self.seen_titles.add(key)
                            self.records.append(rec)
                            if len(self.records) % 25 == 0:
                                log.info("Progreso: %d películas (cola: %d)",
                                         len(self.records), len(self.queue))
                # En fichas y listados seguimos los enlaces internos
                self.extract_links(soup, url)
            except Exception as e:
                log.error("Fallo procesando %s: %s", url, e)
                self.discarded["parse_error"] += 1

    # ---- salida ----
    def save_csv(self):
        # Sin encabezado, UTF-8 sin BOM, 1 registro por línea: titulo,resena,genero
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            for r in self.records:
                w.writerow([r["titulo"], r["resena"], r["genero"]])
        log.info("CSV guardado: %s (%d filas)", OUTPUT_CSV, len(self.records))


def main():
    c = Crawler()
    try:
        c.run()
    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario; se guardará lo recolectado")
    finally:
        c.save_csv()   # siempre guardamos, aunque algo falle

    n = len(c.records)
    if n < MIN_MOVIES:
        log.warning("Solo %d registros (< %d). Revisa crawler.log; ver notas "
                    "sobre paginación/JavaScript.", n, MIN_MOVIES)
    log.info("Descartes: %s", dict(c.discarded))


if __name__ == "__main__":
    main()