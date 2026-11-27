import base64
import calendar
import html
import logging
import os
import random
import re
import signal
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import urllib.parse
from typing import Any, Callable, Literal, Type

import feedparser
import requests
import requests.adapters
import trafilatura
from fast_flights import FlightQuery, Passengers, Query, create_query
from fast_flights.parser import parse as parse_gf_html
from fast_flights.pb import flights_pb2
from google import genai
from google.genai import errors
from pydantic import BaseModel, ConfigDict, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Lade .env falls lokal vorhanden (für lokale Starts außerhalb von Docker)
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.8-flash").strip()
GEMINI_HTTP_TIMEOUT_MS = int(os.environ.get("GEMINI_HTTP_TIMEOUT_MS", "45000"))
GEMINI_CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("GEMINI_CIRCUIT_BREAKER_THRESHOLD", "3"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL_SECONDS", "1800"))
WATCHDOG_TIMEOUT_SECONDS = int(os.environ.get("WATCHDOG_TIMEOUT_SECONDS", "2100"))
DB_PATH = os.environ.get("DB_PATH")
if not DB_PATH:
    DB_PATH = (
        "/app/data/deals.db" if os.path.exists("/app") else str(Path(__file__).resolve().parent / "data" / "deals.db")
    )

HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat")  # nosec B108

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", Path(__file__).resolve().parent / "config.toml"))

STOP_EVENT = threading.Event()


def touch_heartbeat(heartbeat_path: str = HEARTBEAT_FILE) -> None:
    """Aktualisiert die Heartbeat-Datei fehlertolerant für den Docker-Healthcheck."""
    try:
        p = Path(heartbeat_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
    except OSError as exc:
        logging.warning(f"Konnte Heartbeat-Datei {heartbeat_path} nicht aktualisieren: {exc}")


class ActivityWatchdog:
    """
    Hintergrund-Watchdog zur Selbstheilung bei eingefrorenen Netzwerk-/Socket-Operationen.
    Prüft periodisch den Zeitstempel der letzten Aktivität und beendet den Prozess hart per
    os._exit(1), falls die Hauptschleife länger als max_inactivity_seconds blockiert ist,
    damit Docker (restart: unless-stopped) den Container automatisch neu startet.
    """

    def __init__(
        self,
        max_inactivity_seconds: int = WATCHDOG_TIMEOUT_SECONDS,
        check_interval_seconds: float = 60.0,
        heartbeat_file: str = HEARTBEAT_FILE,
        stop_event: threading.Event | None = None,
        exit_fn: Callable[[int], None] = os._exit,
    ) -> None:
        self.max_inactivity_seconds = max(1, int(max_inactivity_seconds))
        self.check_interval_seconds = max(0.05, float(check_interval_seconds))
        self.heartbeat_file = heartbeat_file
        self._stop_event = stop_event or threading.Event()
        self._exit_fn = exit_fn
        self._lock = threading.Lock()
        self._last_activity = time.monotonic()
        self._thread: threading.Thread | None = None

    def beat(self, update_file: bool = True) -> None:
        """Meldet Fortschritt an den Watchdog und aktualisiert optional die Heartbeat-Datei."""
        with self._lock:
            self._last_activity = time.monotonic()
        if update_file and self.heartbeat_file:
            touch_heartbeat(self.heartbeat_file)

    def seconds_since_last_activity(self) -> float:
        with self._lock:
            return time.monotonic() - self._last_activity

    def check_once(self) -> bool:
        """Prüft einmalig auf Überschreitung des Inaktivitätslimits. Gibt True bei Auslösung zurück."""
        idle_seconds = self.seconds_since_last_activity()
        if idle_seconds > self.max_inactivity_seconds:
            logging.critical(
                "WATCHDOG-ALARM: Keine Aktivität seit %.1fs (Limit: %ds). "
                "Erzwinge Prozess-Exit zur automatischen Container-Selbstheilung...",
                idle_seconds,
                self.max_inactivity_seconds,
            )
            self._exit_fn(1)
            return True
        return False

    def _run(self) -> None:
        while not self._stop_event.wait(timeout=self.check_interval_seconds):
            if self.check_once():
                break

    def start(self) -> "ActivityWatchdog":
        self.beat(update_file=True)
        self._thread = threading.Thread(target=self._run, name="activity-watchdog", daemon=True)
        self._thread.start()
        return self


def wait_for_next_cycle(
    total_seconds: int,
    stop_event: threading.Event = STOP_EVENT,
    watchdog: ActivityWatchdog | None = None,
    heartbeat_interval: int = 300,
) -> bool:
    """
    Wartet unterbrechbar bis zum nächsten Zyklus und hält während der geplanten Ruhephase
    den Watchdog sowie die Heartbeat-Datei frisch, damit auch lange Intervalle (> 45 Min.)
    keinen falschen Healthcheck-Alarm auslösen. Gibt True zurück, wenn stop_event gesetzt wurde.
    """
    remaining = float(max(0, total_seconds))
    step = float(max(1, min(heartbeat_interval, 60)))
    elapsed_since_beat = 0.0

    while remaining > 0:
        chunk = min(step, remaining)
        if stop_event.wait(timeout=chunk):
            return True
        remaining -= chunk
        elapsed_since_beat += chunk
        if watchdog and elapsed_since_beat >= heartbeat_interval:
            watchdog.beat(update_file=True)
            elapsed_since_beat = 0.0

    return stop_event.is_set()


def create_genai_client(
    api_key: str = GEMINI_API_KEY,
    timeout_ms: int = GEMINI_HTTP_TIMEOUT_MS,
) -> genai.Client:
    """Erstellt einen Gemini-Client mit striktem HTTP-Timeout (in Millisekunden) gegen Socket-Hangs."""
    return genai.Client(api_key=api_key, http_options={"timeout": timeout_ms})


def handle_shutdown_signal(signum: int, frame: Any):
    logging.info(f"Signal {signum} empfangen. Beende Bot geordnet...")
    STOP_EVENT.set()


def load_config() -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "origin": {
            "home_airport": "HAM",
            "preferred_hubs": ["HAM", "FRA", "BER"],
        },
        "destinations": {
            "regions": ["Asien", "Zentralasien", "Südamerika", "Mittelamerika"],
            "sweet_spots": [
                "DXB",
                "DWC",
                "AUH",
                "JED",
                "RUH",
                "MCT",
                "SLL",
                "DOH",
                "AMM",
                "AQJ",
                "TLV",
                "BEY",
                "TBS",
                "BUS",
                "KUT",
            ],
            "priority_destinations": ["MNL", "KUL"],
        },
        "pricing": {
            "max_price_home_hubs": 2600,
            "max_price_ex_eu": 2000,
        },
        "airlines": {
            "ignored_airlines": [
                "Azerbaijan Airlines",
                "AZAL",
                "J2",
                "Turkmenistan Airlines",
                "T5",
            ],
        },
        "system": {
            "initial_seed_mode": "eval_recent",
            "initial_seed_max_deals": 5,
            "max_entries_per_feed": 25,
            "sent_deals_retention_days": 21,
            "url_reeval_after_days": 7,
            "enable_google_flights_live_check": True,
        },
    }
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "rb") as f:
                cfg = tomllib.load(f)
            for sec, sec_val in defaults.items():
                if sec not in cfg:
                    cfg[sec] = sec_val
                elif isinstance(cfg[sec], dict) and isinstance(sec_val, dict):
                    merged = dict(sec_val)
                    merged.update(cfg[sec])
                    cfg[sec] = merged
            return cfg
        except Exception as e:
            logging.warning(f"Konnte config.toml nicht laden ({e}), nutze Standardwerte.")
    return defaults


CONFIG = load_config()
HOME_AIRPORT: str = str(CONFIG["origin"].get("home_airport", "HAM")).strip().upper()
PREFERRED_HUBS: set[str] = {
    str(h).strip().upper() for h in CONFIG["origin"].get("preferred_hubs", ["HAM", "FRA", "BER"])
}
PRIORITY_DESTINATIONS: list[str] = [
    str(d).strip().upper()
    for d in CONFIG.get("destinations", {}).get("priority_destinations", ["MNL", "KUL"])
    if str(d).strip()
]
IGNORED_AIRLINES: list[str] = [
    str(a).strip() for a in CONFIG.get("airlines", {}).get("ignored_airlines", []) if str(a).strip()
]
MAX_ENTRIES_PER_FEED: int = int(
    os.environ.get("MAX_ENTRIES_PER_FEED", CONFIG.get("system", {}).get("max_entries_per_feed", 25))
)
SENT_DEALS_RETENTION_DAYS: int = max(
    1,
    int(os.environ.get("SENT_DEALS_RETENTION_DAYS", CONFIG.get("system", {}).get("sent_deals_retention_days", 21))),
)
URL_REEVAL_AFTER_DAYS: int = max(
    1,
    int(os.environ.get("URL_REEVAL_AFTER_DAYS", CONFIG.get("system", {}).get("url_reeval_after_days", 7))),
)
_gf_live_check_env = os.environ.get("ENABLE_GF_LIVE_CHECK")
ENABLE_GF_LIVE_CHECK: bool = (
    _gf_live_check_env.strip().lower() in ("1", "true", "yes", "on")
    if _gf_live_check_env is not None
    else bool(CONFIG.get("system", {}).get("enable_google_flights_live_check", True))
)
GF_LIVE_CHECK_TIMEOUT_SECONDS: int = max(3, int(os.environ.get("GF_LIVE_CHECK_TIMEOUT_SECONDS", "12")))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def create_http_session() -> requests.Session:
    """Erstellt eine wiederverwendbare requests.Session mit TCP Connection Pooling und Keep-Alive."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=15, pool_maxsize=30, max_retries=1)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


SESSION = create_http_session()

RSS_FEEDS = [
    # 1. Foren (Vielfliegertreff Primärquellen - Forum 53 = First & Biz Tarife, Forum 5 = Mileage Runs)
    "https://www.vielfliegertreff.de/forum/forums/53/index.rss",  # VFT: First & Business Class Tarife
    "https://www.vielfliegertreff.de/forum/forums/5/index.rss",  # VFT: Mileage Runs & Status-Booster
    # 2. Spezialisierte Business-Class & Status-Portale
    "https://premium-flights.com/feed/",  # Premium-Flights: Biz/First & Status-Runs
    "https://travel-dealz.de/kategorie/fluege/business-class/feed/",  # Travel-Dealz: Business Class
    "https://travel-dealz.com/category/flights/business-class/feed/",  # Travel-Dealz COM: Ex-EU Deals
    "https://travel-dealz.de/kategorie/fluege/first-class/feed/",  # Travel-Dealz: First Class
    "https://youhavebeenupgraded.boardingarea.com/feed/",  # You Have Been Upgraded: GDS/Matrix- & Biz-Deals
    # 3. DACH-Meilenblogs, internationale Deal-Portale & Error-Fare-Tracker
    "https://reisetopia.de/feed/",  # Reisetopia: Flug- & Hotel-Angebote
    "https://frankfurtflyer.de/?feed=rss2",  # Frankfurtflyer: Deals & Star Alliance Sales (FRA)
    "https://www.fly4free.com/flights/flight-deals/europe/feed/",  # Fly4Free: Europa-weite Deals & Error Fares
    "https://meilenoptimieren.com/feed/",  # meilenoptimieren: Deals & Punkte-Analysen
    "https://www.secretflying.com/category/flight-deals/europe-deals/feed/",  # Secret Flying: Europe Deals & Error Fares
    "https://www.flynous.com/feed/",  # Flynous: Günstige Langstreckenflüge (inkl. Biz) ab Europa
]

# Dedizierte Premium-Feeds (enthalten praktisch ausschließlich Business- / First-Class Angebote)
DEDICATED_PREMIUM_FEEDS = {
    "https://www.vielfliegertreff.de/forum/forums/53/index.rss",
    "https://premium-flights.com/feed/",
    "https://travel-dealz.de/kategorie/fluege/business-class/feed/",
    "https://travel-dealz.com/category/flights/business-class/feed/",
    "https://travel-dealz.de/kategorie/fluege/first-class/feed/",
    "https://youhavebeenupgraded.boardingarea.com/feed/",
}

# Signalwörter für Business / First Class (Pflicht für gemischte Feeds wie Fly4free, Reisetopia, etc.)
POSITIVE_CLASS_KEYWORDS = [
    "business class",
    "business-class",
    "businessclass",
    "biz class",
    "bizclass",
    "biz-class",
    "first class",
    "first-class",
    "firstclass",
    "qsuite",
    "polaris",
    "club suites",
    "club world",
    "upper class",
    "lie-flat",
    "lie flat",
    "flatbed",
    "flat-bed",
    "mint suite",
]

# Redaktionelle URL-Pfadsegmente ohne konkrete Flug-Tarife (exakter Segment-Match vor Scraping & LLM)
NON_DEAL_URL_SEGMENTS = {
    "news",
    "reviews",
    "review",
    "guides",
    "guide",
    "kolumne",
    "gutscheine",
    "gutschein",
    "stories",
    "blog",
    "tripreport",
    "kreditkarten",
}

# Harte Negativ-Keywords: Führen IMMER zum Ausschluss (Fragen, Meilenkauf, Hotels, Finanzprodukte, Reviews)
HARD_NEGATIVE_KEYWORDS = [
    "frage:",
    "question:",
    "hilfe",
    "help",
    "sammelthread",
    "diskussion",
    "[fare gone]",
    "[expired]",
    "abgelaufen",
    "hotel",
    "kreditkarte",
    "girokonto",
    "festgeld",
    "tagesgeld",
    "altersvorsorgedepot",
    "points kaufen",
    "punkte kaufen",
    "meilen kaufen",
    "meilenabwertung",
    "coffeebreak",
    "tripreport",
    "review:",
]

# Schwache Negativ-Keywords: Schließen Billig-/Economy-Flüge aus,
# dürfen aber KEINE echten Deals blockieren, wenn im Titel explizit Business/First Class steht.
WEAK_NEGATIVE_KEYWORDS = [
    "economy",
    "eco class",
    "y class",
    "wizz",
    "ryanair",
    "easyjet",
    "condor economy",
    "full-service",
    "full service",
    "sitzplatz",
    "gepäck",
    "upgrade voucher",
]


def is_feed_entry_eligible_pre_filter(
    entry_title: str, entry_content: str, feed_url: str = "", entry_url: str = ""
) -> tuple[bool, str]:
    """
    Deterministischer Pre-Filter vor jedem Scraping & KI-Aufruf:
    0. Exakter URL-Pfadsegment-Filter (sortiert /news/, /reviews/, /guides/, /blog/ etc. sofort aus).
    1. Prüfung auf harte Negativ-Keywords im Titel (Fragen, Hotels, Finanzprodukte, Meilenkauf -> sofortiger Ausschluss).
    2. Hat der Titel ein explizites Positiv-Signal ('business class', 'first class', 'qsuite' etc.),
       werden schwache Negativ-Keywords ('economy', 'full-service') im Titel ignoriert (verhindert False Negatives).
    3. Andernfalls führen schwache Negativ-Keywords im Titel zum Ausschluss.
    4. Bei gemischten Feeds (Fly4free, Reisetopia etc.): Zwingende Pflicht zu mindestens einem Positiv-Keyword
       in Titel oder Text.
    """
    if entry_url:
        path_segments = [seg for seg in urllib.parse.urlsplit(entry_url.strip()).path.lower().split("/") if seg]
        for seg in path_segments:
            if seg in NON_DEAL_URL_SEGMENTS:
                return False, f"Redaktioneller URL-Pfad ausgeschlossen: '/{seg}/'"

    title_lower = entry_title.lower()
    content_lower = entry_content.lower()
    combined = f"{title_lower} {content_lower}"

    # 1. Harte Negativ-Keywords im Titel (immer Ausschluss)
    for bad in HARD_NEGATIVE_KEYWORDS:
        if bad in title_lower:
            return False, f"Harter Ausschluss im Titel: '{bad}'"

    # 2. Prüfe, ob im Titel explizit ein Business/First Class Signal steht
    has_title_positive = any(kw in title_lower for kw in POSITIVE_CLASS_KEYWORDS)

    # 3. Schwache Negativ-Keywords im Titel greifen nur, wenn KEIN Positiv-Signal im Titel vorliegt
    if not has_title_positive:
        for bad in WEAK_NEGATIVE_KEYWORDS:
            if bad in title_lower:
                return False, f"Negativ-Keyword im Titel: '{bad}'"

    # 4. Positiv-Gate für gemischte Feeds
    is_dedicated = (
        feed_url in DEDICATED_PREMIUM_FEEDS
        or "business-class" in feed_url
        or "first-class" in feed_url
        or "forums/53" in feed_url
        or "forumids=372" in feed_url
    )
    if not is_dedicated:
        has_positive = any(kw in combined for kw in POSITIVE_CLASS_KEYWORDS)
        if not has_positive:
            return False, "Gemischter Feed ohne Business/First-Signalwort"

    return True, ""


def _make_iata_via_required(schema: dict[str, Any]) -> None:
    """Stellt sicher, dass Gemini Structured Outputs 'iata_via', 'additional_destinations' und 'is_origin_in_europe' immer aktiv befüllt."""
    req = schema.setdefault("required", [])
    if isinstance(req, list):
        for field_name in ("iata_via", "additional_destinations", "is_origin_in_europe"):
            if field_name not in req:
                req.append(field_name)


DealCategory = Literal[
    "FTL-Booster (Points + QP)",
    "Star Alliance (Nur Points)",
    "Top-Bordprodukt (Oneworld)",
    "Top-Bordprodukt (SkyTeam)",
    "Top-Bordprodukt",
    "Irrelevant",
]


class FlightDealEvaluation(BaseModel):
    model_config = ConfigDict(json_schema_extra=_make_iata_via_required)

    detected_cabin_class: Literal[
        "Business Class", "First Class", "Economy / Premium Eco", "Unklar / Nicht genannt"
    ] = Field(
        description=(
            "Die im Text EINDEUTIG und EXPLIZIT genannte Reiseklasse. "
            "Wenn im Text NICHT ausdrücklich 'Business Class', 'First Class' oder 'BIZclass' steht, "
            "MUSS hier 'Unklar / Nicht genannt' oder 'Economy / Premium Eco' gewählt werden. "
            "NIEMALS raten oder vermuten!"
        )
    )
    cabin_class_quote: str = Field(
        description=(
            "Exakter kurzer Textausschnitt (1 Satz), der belegt, dass dieser konkrete Tarif "
            "in Business oder First Class ist. Wenn im Text kein Nachweis existiert: 'KEIN NACHWEIS'."
        )
    )
    is_relevant: bool = Field(
        description=(
            "True, wenn ALLE Kriterien erfüllt sind: "
            "1. detected_cabin_class in ['Business Class', 'First Class'] "
            "2. cabin_class_quote != 'KEIN NACHWEIS' und nicht leer "
            "3. Zielregion & Preislimit in EUR eingehalten."
        )
    )
    deal_category: DealCategory = Field(
        description=(
            "Kategorie des Deals: 'FTL-Booster (Points + QP)' für alle vollintegrierten Miles & More Partner "
            "(LH, VL, LX, OS, SN, EW, 4Y, EN, LO / LOT Polish Airlines, AZ, LG, OU), "
            "'Star Alliance (Nur Points)' für reine Star Alliance Partner ohne QP (TK, SQ, TP, UA etc.), "
            "'Top-Bordprodukt (Oneworld)' für Oneworld-Airlines (QR, AY, BA, CX, JL, WY, RJ, IB, UL, AT, MH, QF, AA), "
            "'Top-Bordprodukt (SkyTeam)' für SkyTeam-Airlines (AF, KL, VS, UX, SK, SV, KE, CI, VN, GA, MU, MF, DL, AM), "
            "'Top-Bordprodukt' ohne Klammerzusatz für allianzlose Airlines (z.B. EY, EK, GF, CZ, DE, KU, JX), "
            "oder 'Irrelevant'."
        )
    )
    airline_names: str = Field(description="Namen und IATA-Codes der Airlines, z.B. 'SWISS (LX)' oder 'Etihad (EY)'")
    primary_airline_iata: str = Field(
        description="2-Letter IATA Code der ausführenden Hauptairline für den automatischen Google-Flights-Filter, z.B. 'LO' (LOT Polish Airlines), 'RJ', 'LX', 'LH', 'OS', 'QR', 'EY', 'TK', 'UX'"
    )
    iata_origin: str = Field(
        description=(
            "3-Letter IATA Code des tatsächlichen Abflughafens. "
            "ACHTUNG: Darf NUR ein Flughafen sein, der im Text EXPLIZIT als buchbarer Abflugort für diesen Tarif genannt ist! "
            "Wenn der Deal nur ab WAW gilt, MUSS iata_origin='WAW' sein. Niemals einen bevorzugten Hub einsetzen, "
            "der im Text gar nicht angeboten wird!"
        )
    )
    is_origin_in_europe: bool = Field(
        default=True,
        description=(
            "True, wenn iata_origin geografisch in Europa liegt (inkl. EU, UK, Schweiz, Norwegen, Island, Balkan, Türkei/IST). "
            "Zwingend False bei Abflug außerhalb Europas (z.B. Kairo CAI, Tunis TUN, Casablanca CMN, Peking PEK, Hongkong HKG, "
            "Singapur SIN, Dubai DXB, New York JFK, Miami MIA, São Paulo GRU)!"
        ),
    )
    iata_via: list[str] = Field(
        default_factory=list,
        description=(
            "Liste der 3-Letter IATA-Codes aller erforderlichen Zwischenstopps / Umsteigeflughäfen auf der Strecke "
            "zwischen iata_origin und iata_destination in Flugrichtung (z.B. ['HEL'] bei Finnair AMS->HEL->BKK, "
            "['DOH'] bei Qatar Airways, ['AUH'] bei Etihad, ['ZRH'] bei SWISS, ['WAW'] bei LOT ab Westeuropa, "
            "['FRA', 'SIN'] bei zwei echten Umstiegen hintereinander, oder ['FRA/MUC'] bei 1 Umstieg wahlweise via FRA oder MUC). "
            "WICHTIG: Auch wenn der Artikel das Drehkreuz nicht explizit ausschreibt, die Airline aber keinen Nonstop-Flug "
            "zwischen iata_origin und iata_destination anbietet (z.B. Finnair ab AMS nach BKK geht zwingend über 'HEL'), "
            "MUSS das erforderliche Drehkreuz angegeben werden! "
            "Nur bei echtem Nonstop-/Direktflug (z.B. HEL->BKK mit Finnair oder FRA->BKK direkt mit Thai/Lufthansa): leere Liste []. "
            "Niemals iata_origin oder iata_destination selbst wiederholen!"
        ),
    )
    iata_destination: str = Field(description="3-Letter IATA Code des Hauptziels (z.B. BKK, SIN, GRU)")
    additional_destinations: list[str] = Field(
        default_factory=list,
        description=(
            "Weitere im selben Sale/Artikel explizit genannte buchbare Ziele in erlaubten Zielregionen "
            "(OHNE iata_destination selbst zu wiederholen), kompakt mit 3-Letter IATA-Code und Preis falls genannt, "
            "z.B. ['KUL (~1.820 €)', 'SIN (~1.850 €)', 'MNL (~1.890 €)']. "
            "Maximal 6 Einträge. Bei Einzelziel-Deals zwingend leere Liste []."
        ),
    )
    is_open_jaw: bool = Field(description="True, falls Gabelflug (Rückflug endet an anderem Flughafen als iata_origin)")
    iata_return_end: str = Field(
        description="3-Letter IATA Code des Rückflug-Endpunkts (identisch mit iata_origin bei normalem Return)"
    )
    region: Literal["Asien", "Zentralasien", "Südamerika", "Mittelamerika", "Nahost", "Sonstige"] = Field(
        description=(
            "Zielregion des Hauptziels: 'Asien' (Ost-, Südost- und Südasien inkl. Japan, Südkorea, Indien, Malediven), "
            "'Zentralasien' (Kasachstan z.B. ALA/NQZ/TSE, Usbekistan z.B. TAS/SKD, Kirgisistan FRU, Tadschikistan DYU), "
            "'Südamerika', 'Mittelamerika' (inkl. Karibik/Mexiko), 'Nahost' oder 'Sonstige' (z.B. Europa, Nordamerika, Afrika, Australien/Ozeanien)."
        )
    )
    sample_outbound_date: str = Field(
        description="Hinflugdatum im Format YYYY-MM-DD aus dem Artikel/Buchungslink oder plausibles Datum im Reisezeitraum"
    )
    sample_return_date: str = Field(description="Rückflugdatum im Format YYYY-MM-DD (ca. 10-14 Tage nach Hinflug)")
    price_eur: int = Field(description="Gesamtpreis umgerechnet in Euro (ganzzahlig)")
    booking_class: str = Field(
        description="Buchungsklasse falls im Text erwähnt (z.B. 'P', 'Z', 'J'), sonst 'Nicht genannt'"
    )
    mm_points: int = Field(description="Berechnete Miles & More Points für den gesamten Return-Flug")
    mm_qualifying_points: int = Field(
        description=(
            "Berechnete Qualifying Points (QP) für den gesamten Return-Flug. "
            "ACHTUNG: Bei vollintegrierten Airlines (LH, LX, OS, SN, EW, 4Y, EN, LO / LOT Polish Airlines, AZ, LG, OU) "
            "ist mm_qualifying_points IMMER IDENTISCH zu mm_points (LOT bringt immer volle QP!). "
            "Reine Star Alliance Partner (SQ, TK, TP, UA etc.) bringen 0 QP."
        )
    )
    cabin_assessment: str = Field(
        description=(
            "Bewertung des Bordprodukts laut Text (z.B. '1-2-1 Lie-Flat A350', 'Qsuite'). "
            "Falls im Text nicht beschrieben, 'Standard Business Class' oder 'Nicht genannt'. "
            "NIEMALS frei erfinden, wenn die Reiseklasse unklar ist!"
        )
    )
    routing_advice: str = Field(
        description=(
            "NUR befüllen bei konkreten, im Text genannten operativen Besonderheiten: "
            "1. Weitere alternative Abflughäfen (z.B. 'Auch ab BUD, ARN, MXP buchbar'). "
            "2. Ungewöhnliche Umstiege (z.B. '> 8h Nacht-Layover in DOH' oder 'Flughafenwechsel LHR->LGW'). "
            "3. Tarifregeln (z.B. 'Mindestaufenthalt 6 Tage', 'Mehrtägiger Stopover in HEL kostenlos/möglich'). "
            "HINWEIS: Reguläre erforderliche Zwischenstopps gehören als IATA-Code in 'iata_via' (nicht als bloßes 'Umstieg in X' wiederholen). "
            "STRENG VERBOTEN: Keine Wiederholung von Abflugort/Route, keine Meilen-/Allianzhinweise, "
            "keine pauschalen Positionierungs-Tipps (24h Puffer) und KEINE generischen Buchungstipps / "
            "Preis-Checks (z.B. OTA vs. Direktbuchung, Momondo/Kayak/Skyscanner, Zahlungsaufschläge, Phantom-Verfügbarkeit). "
            "Wenn keine solchen Besonderheiten im Text stehen: zwingend leerer String ''!"
        )
    )
    rejection_reason: str = Field(
        description="Falls is_relevant=False oder deal_category='Irrelevant': Präziser, kurzer Grund (z.B. 'Nur Economy / Keine Business Class explizit genannt', 'Startflughafen nicht in Europa (SIN)', 'Zielregion Europa', 'Zu teuer: 2.450 €'). Falls relevant: leerer String ''."
    )


def is_airline_ignored_in_text(text: str, ignored_airlines: list[str] | None = None) -> tuple[bool, str]:
    """Prüft, ob ein Text (z.B. RSS-Titel oder Feed-Inhalt) eine ignorierte Airline enthält.

    Unterscheidet zwischen längeren Namen/Akronymen (z.B. 'Azerbaijan Airlines', 'AZAL',
    'Turkmenistan Airlines') und kurzen 2-Letter IATA-Codes (z.B. 'T5', 'J2'), um
    Fehlalarme (z.B. mit London Heathrow Terminal 5 'LHR T5') zuverlässig auszuschließen.
    """
    if not text:
        return False, ""
    airlines = ignored_airlines if ignored_airlines is not None else IGNORED_AIRLINES
    for item in airlines:
        target = item.strip()
        if not target:
            continue
        if len(target) <= 2:
            # Kurzer Code (z.B. T5, J2): Kontextabhängig prüfen (in Klammern, vor/nach Airline/Flug/Biz etc.)
            # Bei T5 explizit London Heathrow Terminal 5 / Transfer / Lounge ausschließen
            if target.upper() == "T5" and re.search(
                r"\b(?:terminal\s*5|lhr\s*t5|heathrow\s*t5|t5\s*(?:transfer|umstieg|galleries|concourse|lounge))\b",
                text,
                re.IGNORECASE,
            ):
                continue

            pattern = rf"(?:\({re.escape(target)}\)|\b{re.escape(target)}[\s\-:]*(?:Airlines?|Airways|Flug|Flüge|Biz|Business)\b|\bmit\s+{re.escape(target)}\s+(?:nach|ab|via|fliegen)\b)"
            if re.search(pattern, text, re.IGNORECASE):
                return True, target
        else:
            # Voller Name oder langes Akronym (z.B. AZAL, Azerbaijan Airlines, Turkmenistan Airlines)
            pattern = rf"\b{re.escape(target)}\b"
            if re.search(pattern, text, re.IGNORECASE):
                return True, target
    return False, ""


def is_deal_ignored_airline(deal: FlightDealEvaluation, ignored_airlines: list[str] | None = None) -> tuple[bool, str]:
    """Prüft, ob ein bewerteter FlightDealEvaluation-Eintrag von einer ignorierten Airline stammt."""
    if deal is None:
        return False, ""
    airlines = ignored_airlines if ignored_airlines is not None else IGNORED_AIRLINES
    primary = (deal.primary_airline_iata or "").strip().upper()
    airline_names = (deal.airline_names or "").strip()

    for item in airlines:
        target = item.strip()
        if not target:
            continue
        # 1. Direkter Abgleich mit primary_airline_iata (z.B. 'T5', 'J2', 'AZAL')
        if primary and target.upper() == primary:
            return True, target
        # 2. Wortgrenzen-Match im Namen-Feld (z.B. 'Turkmenistan Airlines (T5)', 'Azerbaijan Airlines (AZAL)')
        pattern = rf"\b{re.escape(target)}\b"
        if airline_names and re.search(pattern, airline_names, re.IGNORECASE):
            return True, target

    return False, ""


VOLLINTEGRIERTE_MM_AIRLINES = {
    "LH",  # Lufthansa
    "VL",  # Lufthansa City Airlines
    "LX",  # SWISS
    "OS",  # Austrian Airlines
    "SN",  # Brussels Airlines
    "EW",  # Eurowings
    "4Y",  # Discover Airlines
    "EN",  # Air Dolomiti
    "LO",  # LOT Polish Airlines
    "AZ",  # ITA Airways
    "LG",  # Luxair
    "OU",  # Croatia Airlines
}

ONEWORLD_AIRLINES = {
    "AA",  # American Airlines
    "AS",  # Alaska Airlines
    "AT",  # Royal Air Maroc
    "AY",  # Finnair
    "BA",  # British Airways
    "CX",  # Cathay Pacific
    "FJ",  # Fiji Airways
    "HA",  # Hawaiian Airlines
    "IB",  # Iberia
    "JL",  # Japan Airlines
    "MH",  # Malaysia Airlines
    "QF",  # Qantas
    "QR",  # Qatar Airways
    "RJ",  # Royal Jordanian
    "UL",  # SriLankan Airlines
    "WY",  # Oman Air
}

SKYTEAM_AIRLINES = {
    "AF",  # Air France
    "AM",  # Aeroméxico
    "AR",  # Aerolíneas Argentinas
    "CI",  # China Airlines
    "DL",  # Delta Air Lines
    "FM",  # Shanghai Airlines
    "GA",  # Garuda Indonesia
    "KE",  # Korean Air
    "KL",  # KLM
    "KQ",  # Kenya Airways
    "ME",  # Middle East Airlines
    "MF",  # XiamenAir
    "MU",  # China Eastern
    "RO",  # TAROM
    "SK",  # SAS Scandinavian Airlines
    "SV",  # Saudia
    "UX",  # Air Europa
    "VN",  # Vietnam Airlines
    "VS",  # Virgin Atlantic
}

_ONEWORLD_NAME_KEYWORDS = (
    "qatar airways",
    "finnair",
    "british airways",
    "cathay pacific",
    "japan airlines",
    "oman air",
    "royal jordanian",
    "iberia",
    "srilankan",
    "royal air maroc",
    "malaysia airlines",
    "qantas",
    "american airlines",
    "alaska airlines",
    "hawaiian airlines",
    "fiji airways",
)

_SKYTEAM_NAME_KEYWORDS = (
    "air france",
    "klm",
    "virgin atlantic",
    "air europa",
    "scandinavian airlines",
    "saudia",
    "korean air",
    "china airlines",
    "vietnam airlines",
    "garuda indonesia",
    "china eastern",
    "shanghai airlines",
    "xiamenair",
    "xiamen airlines",
    "delta air lines",
    "aeromexico",
    "aeroméxico",
    "aerolineas argentinas",
    "aerolíneas argentinas",
    "kenya airways",
    "middle east airlines",
    "tarom",
)


def resolve_deal_category(deal: FlightDealEvaluation) -> DealCategory:
    """
    Ermittelt die präzise Anzeigekategorie eines Deals:
    - Vollintegrierte Miles & More Partner / Star Alliance behalten ihre Punkte-Kategorie.
    - Bei 'Top-Bordprodukt'-Deals wird ausschließlich die tatsächliche Allianz angezeigt:
      'Top-Bordprodukt (Oneworld)' bzw. 'Top-Bordprodukt (SkyTeam)'.
    - Hat die Airline keine Allianz (z.B. Etihad, Emirates, Gulf Air, China Southern, Condor),
      wird ohne jeden Klammerzusatz nur 'Top-Bordprodukt' ausgegeben.
    """
    cat = deal.deal_category
    if not deal.is_relevant or cat == "Irrelevant":
        return "Irrelevant" if cat == "Irrelevant" else cat

    if not cat.startswith("Top-Bordprodukt"):
        return cat

    code = (deal.primary_airline_iata or "").strip().upper()
    if not code and deal.airline_names:
        m = re.search(r"\(([A-Z0-9]{2})\)", deal.airline_names)
        if m:
            code = m.group(1).upper()

    if code:
        if code in ONEWORLD_AIRLINES:
            return "Top-Bordprodukt (Oneworld)"
        if code in SKYTEAM_AIRLINES:
            return "Top-Bordprodukt (SkyTeam)"
        return "Top-Bordprodukt"

    names_lower = (deal.airline_names or "").strip().lower()
    if names_lower:
        if any(kw in names_lower for kw in _ONEWORLD_NAME_KEYWORDS):
            return "Top-Bordprodukt (Oneworld)"
        if any(kw in names_lower for kw in _SKYTEAM_NAME_KEYWORDS):
            return "Top-Bordprodukt (SkyTeam)"
        return "Top-Bordprodukt"

    if cat in ("Top-Bordprodukt (Oneworld)", "Top-Bordprodukt (SkyTeam)"):
        return cat
    return "Top-Bordprodukt"


def normalize_and_verify_deal_mm_points(deal: FlightDealEvaluation) -> FlightDealEvaluation:
    """
    Normalisiert und verifiziert die Miles & More Punkte sowie Airline-Codes & Allianzen:
    1. Normalisiert 'LOT' (3-Letter Code) zum 2-Letter IATA Code 'LO'.
    2. Erkennt LOT Polish Airlines zuverlässig anhand des Namens.
    3. Stellt sicher, dass vollintegrierte Partner (LH, VL, LX, OS, SN, EW, 4Y, EN, LO, AZ, LG, OU)
       korrekt als 'FTL-Booster (Points + QP)' eingestuft werden und Points == QP gilt.
       (LOT Polish Airlines ist vollintegrierter Partner und vergibt IMMER volle QP).
    4. Normalisiert 'Top-Bordprodukt'-Kategorien anhand der tatsächlichen Allianz (Oneworld, SkyTeam
       oder ohne Klammerzusatz bei allianzlosen Airlines).
    """
    if deal is None:
        return deal

    primary = (deal.primary_airline_iata or "").strip().upper()
    if primary == "LOT":
        primary = "LO"
        deal.primary_airline_iata = "LO"

    names_lower = (deal.airline_names or "").lower()
    is_lot = primary == "LO" or "lot polish" in names_lower or bool(re.search(r"\blot\b", names_lower))

    if is_lot and primary != "LO":
        deal.primary_airline_iata = "LO"
        primary = "LO"

    if primary in VOLLINTEGRIERTE_MM_AIRLINES or is_lot:
        # Bei vollintegrierten Miles & More Partnern sind Points und QP immer identisch (1:1)
        if deal.mm_points > 0 and deal.mm_qualifying_points == 0:
            deal.mm_qualifying_points = deal.mm_points

        if deal.is_relevant and deal.mm_points > 0:
            if deal.deal_category == "Star Alliance (Nur Points)":
                deal.deal_category = "FTL-Booster (Points + QP)"
        return deal

    deal.deal_category = resolve_deal_category(deal)
    return deal


SINGLE_HUB_AIRLINES: dict[str, str] = {
    "AY": "HEL",  # Finnair
    "LX": "ZRH",  # SWISS (Interkontinental nach Asien/Südamerika/Nahost ausschließlich via ZRH)
    "OS": "VIE",  # Austrian Airlines
    "SN": "BRU",  # Brussels Airlines
    "KL": "AMS",  # KLM
    "AF": "CDG",  # Air France
    "LO": "WAW",  # LOT Polish Airlines
    "AZ": "FCO",  # ITA Airways
    "IB": "MAD",  # Iberia
    "UX": "MAD",  # Air Europa
    "TP": "LIS",  # TAP Air Portugal
    "TK": "IST",  # Turkish Airlines
    "QR": "DOH",  # Qatar Airways
    "EY": "AUH",  # Etihad Airways
    "EK": "DXB",  # Emirates
    "WY": "MCT",  # Oman Air
    "GF": "BAH",  # Gulf Air
    "RJ": "AMM",  # Royal Jordanian
    "KU": "KWI",  # Kuwait Airways
    "UL": "CMB",  # SriLankan Airlines
    "MS": "CAI",  # EgyptAir
    "AT": "CMN",  # Royal Air Maroc
}

# Bekannte Nonstop-Sonderstrecken / 5th-Freedom-Flüge außerhalb des Haupt-Hubs
KNOWN_NONSTOP_EXCEPTIONS: set[tuple[str, str, str]] = {
    ("LO", "BUD", "ICN"),
    ("EK", "BCN", "MEX"),
    ("EK", "MXP", "JFK"),
    ("EK", "ATH", "EWR"),
    ("EK", "LCA", "MLA"),
    ("EK", "BKK", "HKG"),
    ("KL", "SIN", "DPS"),
    ("KL", "CGK", "KUL"),
    ("KL", "EZE", "SCL"),
    ("TK", "GRU", "EZE"),
    ("TK", "SIN", "MEL"),
}

_NONSTOP_MARKERS = {
    "none",
    "null",
    "nonstop",
    "non-stop",
    "direct",
    "direkt",
    "direktflug",
    "kein",
    "keine",
    "keiner",
    "kein stopp",
    "-",
}

_STOP_TOKEN_BLACKLIST = {
    "VIA",
    "UND",
    "AND",
    "MIT",
    "VON",
    "BIS",
    "AUF",
    "NUR",
    "HIN",
    "PRO",
    "MAX",
    "MIN",
    "STD",
    "TAG",
    "NON",
    "THE",
    "FOR",
    "ONE",
    "TWO",
    "EUR",
    "USD",
    "CHF",
    "GBP",
}


def normalize_iata_via(raw_via: list[str] | None, origin: str = "", destination: str = "") -> list[str]:
    """Bereinigt und normalisiert Zwischenstopp-Angaben auf saubere 3-Letter IATA-Codes (bzw. 'FRA/MUC')."""
    if not raw_via:
        return []

    orig = (origin or "").strip().upper()
    dest = (destination or "").strip().upper()
    excluded = {c for c in (orig, dest) if c}
    cleaned: list[str] = []

    for entry in raw_via:
        if not isinstance(entry, str):
            continue
        text = entry.strip()
        if not text or text.lower() in _NONSTOP_MARKERS:
            continue

        # Fall 1: Reiner IATA-Code oder Schrägstrich-Alternative (z.B. 'HEL' oder 'FRA/MUC')
        if re.fullmatch(r"[A-Za-z]{3}(?:\s*/\s*[A-Za-z]{3})*", text):
            parts = [p.strip().upper() for p in text.split("/")]
            valid_parts = [p for p in parts if p not in excluded and p not in _STOP_TOKEN_BLACKLIST]
            if valid_parts:
                token = "/".join(dict.fromkeys(valid_parts))
                if token not in cleaned:
                    cleaned.append(token)
            continue

        # Fall 2: Stadtname mit IATA-Code in Klammern, z.B. 'Helsinki (HEL)'
        paren_codes = re.findall(r"\(([A-Za-z]{3})\)", text)
        if paren_codes:
            for code in paren_codes:
                up = code.upper()
                if up not in excluded and up not in _STOP_TOKEN_BLACKLIST and up not in cleaned:
                    cleaned.append(up)
            continue

        # Fall 3: Freitext mit großgeschriebenen 3-Letter-Codes, z.B. 'via HEL' oder 'AMS-HEL-BKK'
        upper_tokens = re.findall(r"\b([A-Z]{3})\b", text)
        for up in upper_tokens:
            if up not in excluded and up not in _STOP_TOKEN_BLACKLIST and up not in cleaned:
                cleaned.append(up)

    return cleaned


def _extract_stops_from_advice(routing_advice: str, origin: str, destination: str) -> list[str]:
    """Extrahiert explizit genannte Umsteige-/Stopover-Flughäfen aus routing_advice (ignoriert 'Auch ab XXX')."""
    if not routing_advice:
        return []
    orig = (origin or "").strip().upper()
    dest = (destination or "").strip().upper()
    excluded = {c for c in (orig, dest) if c} | _STOP_TOKEN_BLACKLIST
    found: list[str] = []

    pattern = re.compile(
        r"\b(?:layover|umstieg|zwischenstopp|stopover|via|über)\s+(?:in\s+)?"
        r"(?:[A-Za-zÄÖÜäöüß\-\s]{1,25}?\(([A-Z]{3})\)|([A-Z]{3})(?:\s*/\s*([A-Z]{3}))?\b)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(routing_advice):
        c_paren, c_direct, c_alt = m.group(1), m.group(2), m.group(3)
        if c_paren:
            code = c_paren.upper()
            if code not in excluded and code not in found:
                found.append(code)
        elif c_direct and c_direct.isupper():
            parts = [c_direct.upper()]
            if c_alt and c_alt.isupper():
                parts.append(c_alt.upper())
            valid = [p for p in parts if p not in excluded]
            if valid:
                token = "/".join(dict.fromkeys(valid))
                if token not in found:
                    found.append(token)
    return found


def resolve_deal_stops(deal: FlightDealEvaluation) -> list[str]:
    """
    Ermittelt die erforderlichen Zwischenstopps einer Route mehrstufig:
    1. Aus 'deal.iata_via' (vom Modell extrahiert und deterministisch bereinigt).
    2. Fallback über explizite Umstiegs-/Stopover-Nennungen in 'deal.routing_advice'.
    3. Deterministisches Airline-Drehkreuz-Fallback (z.B. Finnair AY -> HEL, Qatar QR -> DOH, SWISS LX -> ZRH),
       sofern weder Start noch Ziel bereits das Drehkreuz sind und keine Nonstop-Sonderstrecke vorliegt.
    """
    if deal is None:
        return []

    orig = (deal.iata_origin or "").strip().upper()
    dest = (deal.iata_destination or "").strip().upper()

    # 1. Primärquelle: deal.iata_via
    stops = normalize_iata_via(deal.iata_via, orig, dest)
    if stops:
        return stops

    # Für ungültige Platzhalter-Codes keine Heuristik anwenden
    invalid_placeholders = {"EUR", "ASI", "UNK", "XXX"}
    if (
        not re.fullmatch(r"[A-Z]{3}", orig)
        or not re.fullmatch(r"[A-Z]{3}", dest)
        or orig == dest
        or orig in invalid_placeholders
        or dest in invalid_placeholders
    ):
        return []

    # 2. Sekundärquelle: Expliziter Stopover/Layover/Umstieg-Hinweis in routing_advice
    advice_stops = _extract_stops_from_advice(deal.routing_advice or "", orig, dest)
    if advice_stops:
        return advice_stops

    # 3. Tertiärquelle: Zwingendes Airline-Drehkreuz bei Nicht-Hub-Verbindungen
    primary = (deal.primary_airline_iata or "").strip().upper()
    if not primary and deal.airline_names:
        m = re.search(r"\(([A-Z0-9]{2})\)", deal.airline_names)
        if m:
            primary = m.group(1).upper()

    if not primary:
        return []

    if (primary, orig, dest) in KNOWN_NONSTOP_EXCEPTIONS or (primary, dest, orig) in KNOWN_NONSTOP_EXCEPTIONS:
        return []

    # Spezifische Nebenbasen ausschließen, ab denen Direktflüge existieren
    if primary == "AF" and orig == "ORY":
        return []
    if primary == "IB" and orig == "BCN":
        return []
    if primary == "TP" and orig == "OPO":
        return []
    if primary == "TK" and (orig in {"IST", "SAW", "ESB", "AYT", "ADB"} or dest in {"IST", "SAW", "ESB", "AYT", "ADB"}):
        return []
    if primary == "SQ" and deal.region in ("Asien", "Zentralasien") and orig != "SIN" and dest != "SIN":
        return ["SIN"]
    if primary == "LH" and orig not in {"FRA", "MUC"} and dest not in {"FRA", "MUC"}:
        return ["FRA/MUC"]

    hub = SINGLE_HUB_AIRLINES.get(primary)
    if hub and hub not in (orig, dest):
        return [hub]

    return []


def format_deal_route_plain(deal: FlightDealEvaluation) -> str:
    """Formatiert die vollständige Route inkl. Zwischenstopps als Klartext für Logs/CLI (z.B. 'AMS->HEL->BKK')."""
    stops = resolve_deal_stops(deal)
    nodes = [n for n in [deal.iata_origin, *stops, deal.iata_destination] if n]
    route = "->".join(nodes)
    if deal.is_open_jaw and deal.iata_return_end and deal.iata_return_end != deal.iata_origin:
        route += f"->{deal.iata_return_end} (Gabelflug)"
    return route


# Kurz-/Mittelstrecken-Sweet-Spots (Georgien & Levante), bei denen reguläre Business-Tarife < 750 € kosten können
SHORT_MEDIUM_HAUL_SWEET_SPOTS: set[str] = {"TBS", "BUS", "KUT", "TLV", "BEY", "AMM", "AQJ"}

# Deterministische Positivliste europäischer Abflughäfen (Fast-Path für Europa-Origin-Check)
KNOWN_EUROPE_AIRPORTS: set[str] = {
    "HAM",
    "FRA",
    "BER",
    "MUC",
    "DUS",
    "CGN",
    "STR",
    "HAJ",
    "NUE",
    "BRE",
    "LEJ",
    "DRS",
    "FMO",
    "PAD",
    "FDH",
    "CPH",
    "BLL",
    "AAL",
    "AAR",
    "AMS",
    "EIN",
    "RTM",
    "BRU",
    "CRL",
    "LUX",
    "CDG",
    "ORY",
    "LYS",
    "NCE",
    "MRS",
    "TLS",
    "BOD",
    "NTE",
    "BSL",
    "ZRH",
    "GVA",
    "VIE",
    "SZG",
    "GRZ",
    "INN",
    "LNZ",
    "KLU",
    "WAW",
    "KRK",
    "GDN",
    "WRO",
    "POZ",
    "KTW",
    "PRG",
    "BTS",
    "BUD",
    "OTP",
    "CLJ",
    "TSR",
    "SOF",
    "VAR",
    "ZAG",
    "SPU",
    "DBV",
    "LJU",
    "BEG",
    "SKP",
    "TIA",
    "SJJ",
    "PRN",
    "TGD",
    "ATH",
    "SKG",
    "HER",
    "RHO",
    "CFU",
    "LCA",
    "PFO",
    "MLA",
    "FCO",
    "CIA",
    "MXP",
    "LIN",
    "BGY",
    "VCE",
    "BLQ",
    "NAP",
    "FLR",
    "PSA",
    "TRN",
    "VRN",
    "BRI",
    "CTA",
    "PMO",
    "MAD",
    "BCN",
    "AGP",
    "PMI",
    "IBZ",
    "VLC",
    "BIO",
    "SVQ",
    "ALC",
    "TFN",
    "TFS",
    "LPA",
    "LIS",
    "OPO",
    "FAO",
    "FNC",
    "PDL",
    "LHR",
    "LGW",
    "STN",
    "LTN",
    "LCY",
    "MAN",
    "EDI",
    "BHX",
    "GLA",
    "BRS",
    "NCL",
    "BFS",
    "DUB",
    "ORK",
    "SNN",
    "OSL",
    "BGO",
    "TRD",
    "SVG",
    "TOS",
    "ARN",
    "GOT",
    "MMX",
    "HEL",
    "TLL",
    "RIX",
    "VNO",
    "KEF",
    "IST",
    "SAW",
    "ESB",
    "AYT",
    "ADB",
}

# Bekannte Nicht-Europa-Abflughäfen (harter Sperrfilter gegen Reverse-, Ex-Nordafrika- und Intra-Asien-Deals)
NON_EUROPE_ORIGIN_AIRPORTS: set[str] = {
    "CAI",
    "HRG",
    "SSH",
    "TUN",
    "CMN",
    "RAK",
    "ALG",
    "ADD",
    "NBO",
    "MBA",
    "DAR",
    "JRO",
    "ZNZ",
    "JNB",
    "CPT",
    "DUR",
    "WDH",
    "SEZ",
    "MRU",
    "RUN",
    "TNR",
    "LOS",
    "ACC",
    "DSS",
    "TMS",
    "LAD",
    "TLV",
    "BEY",
    "AMM",
    "AQJ",
    "DAM",
    "BGW",
    "IKA",
    "KWI",
    "BAH",
    "DOH",
    "AUH",
    "DXB",
    "DWC",
    "SHJ",
    "MCT",
    "SLL",
    "RUH",
    "JED",
    "DMM",
    "MED",
    "TBS",
    "BUS",
    "KUT",
    "EVN",
    "GYD",
    "ALA",
    "NQZ",
    "TSE",
    "TAS",
    "SKD",
    "FRU",
    "DYU",
    "DEL",
    "BOM",
    "BLR",
    "HYD",
    "MAA",
    "CCU",
    "COK",
    "CMB",
    "MLE",
    "DAC",
    "KTM",
    "KHI",
    "LHE",
    "ISB",
    "PEK",
    "PKX",
    "PVG",
    "SHA",
    "CAN",
    "SZX",
    "CTU",
    "TFU",
    "CKG",
    "HGH",
    "XMN",
    "HKG",
    "MFM",
    "TPE",
    "KHH",
    "NRT",
    "HND",
    "KIX",
    "NGO",
    "FUK",
    "CTS",
    "ICN",
    "GMP",
    "PUS",
    "ULN",
    "BKK",
    "DMK",
    "HKT",
    "CNX",
    "SIN",
    "KUL",
    "PEN",
    "CGK",
    "DPS",
    "SUB",
    "MNL",
    "CEB",
    "CRK",
    "SGN",
    "HAN",
    "DAD",
    "PNH",
    "RGN",
    "VTE",
    "SYD",
    "MEL",
    "BNE",
    "PER",
    "ADL",
    "AKL",
    "CHC",
    "ZQN",
    "NAN",
    "PPT",
    "GUM",
    "HNL",
    "JFK",
    "EWR",
    "LGA",
    "BOS",
    "PHL",
    "IAD",
    "DCA",
    "BWI",
    "ORD",
    "DTW",
    "MSP",
    "ATL",
    "MIA",
    "FLL",
    "MCO",
    "TPA",
    "DFW",
    "IAH",
    "AUS",
    "DEN",
    "PHX",
    "LAS",
    "LAX",
    "SFO",
    "SAN",
    "SEA",
    "PDX",
    "YYZ",
    "YUL",
    "YVR",
    "YYC",
    "YOW",
    "MEX",
    "CUN",
    "PTY",
    "SJO",
    "SAL",
    "GUA",
    "HAV",
    "PUJ",
    "SDQ",
    "MBJ",
    "NAS",
    "BOG",
    "MDE",
    "LIM",
    "UIO",
    "GYE",
    "CCS",
    "GRU",
    "GIG",
    "VCP",
    "BSB",
    "EZE",
    "AEP",
    "COR",
    "SCL",
    "MVD",
    "ASU",
}

# Häufig vom Modell fälschlich zu 'Asien' gezählte Nachbar-/Drittregionen-Ziele (greift nur, wenn NICHT in sweet_spots)
EXCLUDED_DESTINATION_AIRPORTS: set[str] = {
    "SEZ",
    "MRU",
    "RUN",
    "ZNZ",
    "TNR",
    "CPT",
    "JNB",
    "DUR",
    "NBO",
    "MBA",
    "DAR",
    "JRO",
    "WDH",
    "ADD",
    "TMS",
    "LAD",
    "CAI",
    "HRG",
    "SSH",
    "CMN",
    "RAK",
    "TUN",
    "ALG",
    "LOS",
    "ACC",
    "SYD",
    "MEL",
    "BNE",
    "PER",
    "ADL",
    "AKL",
    "CHC",
    "ZQN",
    "NAN",
    "PPT",
    "GUM",
    "HNL",
    "JFK",
    "EWR",
    "LGA",
    "BOS",
    "IAD",
    "ORD",
    "ATL",
    "MIA",
    "DFW",
    "IAH",
    "LAX",
    "SFO",
    "SEA",
    "YYZ",
    "YUL",
    "YVR",
    "BAH",
    "KWI",
    "DAM",
    "BGW",
    "IKA",
    "EVN",
    "GYD",
}


def validate_deal_safety(deal: FlightDealEvaluation, title: str, full_text: str) -> tuple[bool, str]:
    """
    Deterministische Sicherheitsprüfungen (Airbag) nach der Gemini-Evaluierung:
    1. Kabinenklassen-Beweis: Wurde Business/First explizit nachgewiesen?
    2. Konkreter Flugpreis & Preisplausibilität (< 750 € Langstrecke bzw. < 280 € Kurz-/Mittelstrecken-Sweet-Spots).
    3. Abflughafen-Verifikation (kein Origin-Hijacking) & Europa-Origin-Pflicht.
    4. Verbindliche Zielregions- & Sweet-Spot-Prüfung (verhindert Ausreißer wie SEZ oder DAM).
    5. Verbindliche Preislimit-Prüfung (Home-Hubs vs. Ex-EU aus config.toml).
    """
    if deal is None or not deal.is_relevant or deal.deal_category == "Irrelevant":
        return True, ""

    # 1. Kabinenklassen-Beweis prüfen
    if deal.detected_cabin_class not in ("Business Class", "First Class"):
        return False, f"Reiseklasse nicht Business/First ({deal.detected_cabin_class})"

    quote = (deal.cabin_class_quote or "").strip()
    if not quote or quote.upper() == "KEIN NACHWEIS":
        return False, "Kein Textnachweis für Business/First Class vorhanden"

    # 2. Konkreter Preis & Untergrenzen-Plausibilität
    if deal.price_eur <= 0:
        return False, "Kein konkreter Flugpreis genannt (0 €)"

    dest = normalize_airport_iata(deal.iata_destination)
    combined_lower = f"{title} {full_text}".lower()
    has_error_fare_signal = any(ef in combined_lower for ef in ("error fare", "error-fare", "preisfehler", "errorfare"))

    if dest in SHORT_MEDIUM_HAUL_SWEET_SPOTS:
        if deal.price_eur < 280 and not has_error_fare_signal:
            return (
                False,
                f"Unplausibler Business-Class-Preis für Kurz-/Mittelstrecke ({dest}): {deal.price_eur} € (Verdacht auf Economy)",
            )
    elif deal.region in ("Asien", "Zentralasien", "Südamerika", "Mittelamerika") and deal.price_eur < 750:
        if not has_error_fare_signal:
            return (
                False,
                f"Unplausibler Langstreckenpreis für Business Class: {deal.price_eur} € (Verdacht auf Economy)",
            )

    # 3. Origin-Verifikation (Origin-Hijacking verhindern)
    origin = (deal.iata_origin or "").upper().strip()
    combined_upper = f"{title} {full_text}".upper()
    if origin and origin not in combined_upper:
        airport_aliases = {
            "FRA": ["FRANKFURT"],
            "HAM": ["HAMBURG"],
            "BER": ["BERLIN"],
            "CPH": ["KOPENHAGEN", "COPENHAGEN"],
            "MUC": ["MÜNCHEN", "MUNICH"],
            "ZRH": ["ZÜRICH", "ZURICH"],
            "VIE": ["WIEN", "VIENNA"],
            "WAW": ["WARSCHAU", "WARSAW"],
            "BUD": ["BUDAPEST"],
            "PRG": ["PRAG", "PRAGUE"],
            "OSL": ["OSLO"],
            "ARN": ["STOCKHOLM", "ARLANDA"],
            "DUB": ["DUBLIN"],
            "AMS": ["AMSTERDAM"],
            "CDG": ["PARIS", "CHARLES DE GAULLE"],
            "MXP": ["MAILAND", "MILAN", "MALPENSA"],
            "FCO": ["ROM", "ROME", "FIUMICINO"],
            "MAD": ["MADRID"],
            "BCN": ["BARCELONA"],
            "LIS": ["LISSABON", "LISBON"],
            "LHR": ["LONDON", "HEATHROW"],
            "LGW": ["GATWICK"],
            "BRU": ["BRÜSSEL", "BRUSSELS"],
            "HEL": ["HELSINKI"],
            "ATH": ["ATHEN", "ATHENS"],
            "IST": ["ISTANBUL"],
            "OTP": ["BUKAREST", "BUCHAREST"],
            "KEF": ["REYKJAVIK", "KEFLAVIK"],
        }
        aliases = airport_aliases.get(origin, [])
        if not any(alias in combined_upper for alias in aliases):
            return False, f"Abflughafen {origin} kommt im Deal-Text nicht vor (Origin-Hijacking abgefangen)"

    # 4. Europa-Origin-Pflicht (Hybrid: Negativliste -> Europa-Fast-Path -> Schema-Flag)
    norm_origin = normalize_airport_iata(origin)
    hubs_set = {
        normalize_airport_iata(str(h))
        for h in CONFIG.get("origin", {}).get("preferred_hubs", list(PREFERRED_HUBS))
        if str(h).strip()
    }
    if norm_origin in NON_EUROPE_ORIGIN_AIRPORTS:
        return False, f"Startflughafen {norm_origin} liegt außerhalb Europas"
    if norm_origin and norm_origin not in KNOWN_EUROPE_AIRPORTS and norm_origin not in hubs_set:
        if not getattr(deal, "is_origin_in_europe", True):
            return False, f"Startflughafen {norm_origin} liegt laut Auswertung außerhalb Europas"

    # 5. Zielregions- & Sweet-Spot-Prüfung
    sweet_spots_set = {
        normalize_airport_iata(str(s)) for s in CONFIG.get("destinations", {}).get("sweet_spots", []) if str(s).strip()
    }
    allowed_regions = {
        str(r).strip()
        for r in CONFIG.get("destinations", {}).get("regions", ["Asien", "Zentralasien", "Südamerika", "Mittelamerika"])
    }
    if dest not in sweet_spots_set:
        if deal.region == "Nahost":
            return False, f"Nahost-Ziel {dest} gehört nicht zu den erlaubten Sweet-Spots"
        if dest in EXCLUDED_DESTINATION_AIRPORTS:
            return False, f"Ziel {dest} liegt außerhalb der erlaubten Zielregionen"
        if deal.region not in allowed_regions:
            return False, f"Zielregion '{deal.region}' ({dest}) gehört nicht zu den erlaubten Zielregionen"

    # 6. Verbindliche Preislimit-Prüfung (Option A: Home-Hubs vs. Ex-EU)
    pricing_cfg = CONFIG.get("pricing", {})
    max_home = int(pricing_cfg.get("max_price_home_hubs", 2600))
    max_ex_eu = int(pricing_cfg.get("max_price_ex_eu", 2000))
    if norm_origin in hubs_set:
        if deal.price_eur > max_home:
            return False, f"Preis {deal.price_eur} € überschreitet Limit ab bevorzugten Hubs ({max_home} €)"
    else:
        if deal.price_eur > max_ex_eu:
            return False, f"Preis {deal.price_eur} € überschreitet Ex-EU-Limit ({max_ex_eu} €)"

    return True, ""


def validate_startup():
    missing = [
        name
        for name, val in [
            ("GEMINI_API_KEY", GEMINI_API_KEY),
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        ]
        if not val
    ]
    if missing:
        raise SystemExit(f"KRITISCHER FEHLER: Fehlende Umgebungsvariablen: {', '.join(missing)}")

    # Prüfe Telegram Bot Token beim Start mit Retry
    tg_verified = False
    for attempt in range(1, 4):
        try:
            resp = SESSION.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=8)
            if resp.ok:
                tg_verified = True
                break
            if resp.status_code in (401, 404):
                raise SystemExit(f"KRITISCHER FEHLER: Telegram Bot Token ungültig ({resp.status_code})")
            logging.warning(f"Telegram-Check Versuch {attempt}/3 meldet HTTP {resp.status_code}")
        except requests.RequestException as e:
            logging.warning(f"Telegram-Check Versuch {attempt}/3 fehlgeschlagen: {e}")
        time.sleep(2)

    if tg_verified:
        logging.info("Startup-Check erfolgreich: API-Keys und Telegram-Verbindung verifiziert.")
    else:
        logging.warning(
            "Telegram-Verbindung konnte beim Start nicht geprüft werden. Bot läuft weiter und versucht es im Zyklus erneut."
        )


def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA journal_size_limit=65536;")
    with closing(conn.cursor()) as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS seen_urls (url_clean TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS sent_deals (deal_fingerprint TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

        # Automatische sanfte Schema-Migration für bestehende Datenbanken
        c.execute("PRAGMA table_info(seen_urls)")
        seen_cols = {col[1] for col in c.fetchall()}
        if "last_seen_at" not in seen_cols:
            c.execute("ALTER TABLE seen_urls ADD COLUMN last_seen_at TIMESTAMP")
        if "entry_published_at" not in seen_cols:
            c.execute("ALTER TABLE seen_urls ADD COLUMN entry_published_at TIMESTAMP")

        c.execute("PRAGMA table_info(sent_deals)")
        existing_cols = {col[1] for col in c.fetchall()}
        if "price_eur" not in existing_cols:
            c.execute("ALTER TABLE sent_deals ADD COLUMN price_eur INTEGER DEFAULT 0")
        if "airline_code" not in existing_cols:
            c.execute("ALTER TABLE sent_deals ADD COLUMN airline_code TEXT DEFAULT ''")
        if "updated_at" not in existing_cols:
            c.execute("ALTER TABLE sent_deals ADD COLUMN updated_at TIMESTAMP")

    conn.commit()
    return conn


def checkpoint_db(conn: sqlite3.Connection) -> None:
    """Schreibt alle WAL-Änderungen in die Hauptdatenbank zurück und kürzt die -wal-Datei auf 0 Byte."""
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")


def close_db(conn: sqlite3.Connection | None) -> None:
    """Führt einen vollständigen WAL-Checkpoint aus und schließt die Verbindung sauber (entfernt -wal & -shm)."""
    if conn is None:
        return
    try:
        checkpoint_db(conn)
    except sqlite3.Error as exc:
        logging.warning(f"WAL-Checkpoint vor dem Schließen der DB fehlgeschlagen: {exc}")
    finally:
        conn.close()


def cleanup_db(conn: sqlite3.Connection, sent_deals_retention_days: int | None = None) -> None:
    retention_days = max(
        1,
        int(SENT_DEALS_RETENTION_DAYS if sent_deals_retention_days is None else sent_deals_retention_days),
    )
    cutoff_modifier = f"-{retention_days} days"
    with closing(conn.cursor()) as c:
        c.execute("DELETE FROM seen_urls WHERE COALESCE(last_seen_at, created_at) < datetime('now', '-90 days')")
        deleted_urls = max(0, c.rowcount)
        c.execute(
            "DELETE FROM sent_deals WHERE COALESCE(updated_at, created_at) < datetime('now', ?)",
            (cutoff_modifier,),
        )
        deleted_deals = max(0, c.rowcount)
    conn.commit()
    if (deleted_urls + deleted_deals) > 0:
        conn.execute("VACUUM;")
    checkpoint_db(conn)


def build_deal_fingerprint(deal: FlightDealEvaluation) -> str:
    """
    Erstellt einen eindeutigen Fingerprint pro Route und Airline (z.B. FRA-BKK-RJ oder FRA-BKK-MUC-LH).
    Verhindert, dass Deals unterschiedlicher Airlines auf derselben Verbindung einander blockieren.
    """
    airline_code = (deal.primary_airline_iata or "").strip().upper()
    if not airline_code and deal.airline_names:
        m = re.search(r"\b([A-Z0-9]{2})\b", deal.airline_names)
        if m:
            airline_code = m.group(1).upper()
        else:
            airline_code = re.sub(r"[^A-Z0-9]", "", deal.airline_names.upper())[:6] or "XX"
    if not airline_code:
        airline_code = "XX"

    orig = deal.iata_origin.strip().upper()
    dest = deal.iata_destination.strip().upper()
    if deal.is_open_jaw and deal.iata_return_end and deal.iata_return_end != deal.iata_origin:
        ret = deal.iata_return_end.strip().upper()
        return f"{orig}-{dest}-{ret}-{airline_code}"
    return f"{orig}-{dest}-{airline_code}"


def should_send_deal(
    conn: sqlite3.Connection, deal: FlightDealEvaluation, min_price_drop: int = 75
) -> tuple[bool, str, int]:
    """
    Intelligente Prüfung auf Duplikate & Preisänderungen:
    - Gibt (True, "new_deal", 0) zurück, wenn Route+Airline noch nie gesendet wurden.
    - Gibt (True, "price_drop", prev_price) zurück, wenn der Deal mindestens 'min_price_drop' EUR günstiger ist.
    - Gibt (False, "duplicate", prev_price) zurück, wenn der Deal gleich teuer, teurer oder nur marginal billiger ist.
    """
    fp = build_deal_fingerprint(deal)
    with closing(conn.cursor()) as c:
        c.execute("SELECT price_eur, deal_fingerprint FROM sent_deals WHERE deal_fingerprint = ?", (fp,))
        row = c.fetchone()

        if not row:
            # Fallback-Prüfung für Alt-DB-Einträge mit 100er-Buckets (z.B. "FRA-BKK-1700") während der Übergangsphase
            price_bucket = round(deal.price_eur / 100) * 100
            legacy_fp = f"{deal.iata_origin}-{deal.iata_destination}-{price_bucket}"
            c.execute("SELECT price_eur, deal_fingerprint FROM sent_deals WHERE deal_fingerprint = ?", (legacy_fp,))
            row = c.fetchone()
            if not row:
                return True, "new_deal", 0

    prev_price = row[0] or 0
    # Falls prev_price bei alten Einträgen 0 war, ermittle ihn aus legacy-Fingerprint falls möglich
    if prev_price <= 0 and "-" in row[1]:
        parts = row[1].split("-")
        if parts[-1].isdigit():
            prev_price = int(parts[-1])

    if prev_price > 0:
        if deal.price_eur <= (prev_price - min_price_drop):
            return True, "price_drop", prev_price
        else:
            return False, "duplicate", prev_price

    return False, "duplicate", prev_price


def record_sent_deal(conn: sqlite3.Connection, deal: FlightDealEvaluation):
    """Speichert oder aktualisiert den gesendeten Deal mit aktuellem Preis und Zeitstempel."""
    fp = build_deal_fingerprint(deal)
    airline_code = (deal.primary_airline_iata or "").strip().upper()
    with closing(conn.cursor()) as c:
        c.execute(
            """
            INSERT INTO sent_deals (deal_fingerprint, price_eur, airline_code, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(deal_fingerprint) DO UPDATE SET
                price_eur = excluded.price_eur,
                airline_code = excluded.airline_code,
                updated_at = CURRENT_TIMESTAMP
        """,
            (fp, deal.price_eur, airline_code),
        )
    conn.commit()


_STRIP_QUERY_PREFIXES = ("utm_", "fbclid", "gclid", "mc_", "ref", "__s", "vgo_ee", "_hsenc", "_hsmi", "mkt_tok")


def normalize_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    query_params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    # Behalte Foren-Parameter (z.B. ?t=123), entferne nur Tracking- und Newsletter-Parameter
    clean_params = [(k, v) for k, v in query_params if not k.lower().startswith(_STRIP_QUERY_PREFIXES)]
    clean_query = urllib.parse.urlencode(clean_params)
    return urllib.parse.urlunsplit((scheme, netloc, parsed.path, clean_query, ""))


def extract_entry_published_utc(entry: dict[str, Any]) -> str | None:
    """
    Extrahiert den Veröffentlichungszeitpunkt eines RSS-Eintrags als UTC-String ('YYYY-MM-DD HH:MM:SS').
    Priorisiert 'published_parsed' (<pubDate>) vor 'updated_parsed', damit bloße Tippfehler-Edits
    keine unnötige Re-Evaluierung auslösen.
    """
    st = entry.get("published_parsed") or entry.get("updated_parsed")
    if not st:
        return None
    try:
        ts = calendar.timegm(st)
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def is_forum_url(url: str, feed_url: str = "") -> bool:
    """Erkennt Foren-URLs (z.B. Vielfliegertreff), deren pubDate bei jeder Antwort hochspringt."""
    combined = f"{url} {feed_url}".lower()
    return "vielfliegertreff.de" in combined or "/forum/threads/" in combined or "/forums/" in combined


def _parse_sqlite_utc(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str.strip()[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def should_evaluate_url(
    conn: sqlite3.Connection,
    url_clean: str,
    entry_published_utc: str | None = None,
    feed_url: str = "",
    reeval_after_days: int | None = None,
    max_freshness_hours: int = 72,
    now_utc: datetime | None = None,
) -> bool:
    """
    Prüft, ob eine Feed-URL ausgewertet werden soll:
    - True, wenn die URL noch nie gesehen wurde.
    - True bei redaktionellen Deal-Artikeln (nicht Foren!), deren RSS-Veröffentlichungsdatum
      mindestens 'reeval_after_days' neuer als der gespeicherte Stand UND höchstens 'max_freshness_hours' alt ist.
    - Sonst False.
    """
    with closing(conn.cursor()) as c:
        c.execute("SELECT created_at, entry_published_at FROM seen_urls WHERE url_clean = ?", (url_clean,))
        row = c.fetchone()

    if not row:
        return True

    if is_forum_url(url_clean, feed_url) or not entry_published_utc:
        return False

    entry_dt = _parse_sqlite_utc(entry_published_utc)
    stored_dt = _parse_sqlite_utc(row[1]) or _parse_sqlite_utc(row[0])
    if not entry_dt or not stored_dt:
        return False

    days_threshold = max(
        1,
        int(
            reeval_after_days
            if reeval_after_days is not None
            else CONFIG.get("system", {}).get("url_reeval_after_days", URL_REEVAL_AFTER_DAYS)
        ),
    )
    ref_now = now_utc or datetime.now(timezone.utc)
    if ref_now.tzinfo is None:
        ref_now = ref_now.replace(tzinfo=timezone.utc)

    is_significantly_newer = entry_dt >= (stored_dt + timedelta(days=days_threshold))
    is_fresh = entry_dt >= (ref_now - timedelta(hours=max_freshness_hours))
    return is_significantly_newer and is_fresh


def upsert_seen_url(conn: sqlite3.Connection, url_clean: str, entry_published_utc: str | None = None) -> None:
    """Speichert oder aktualisiert eine verarbeitete URL transaktionssicher ohne UNIQUE-Constraint-Fehler."""
    with closing(conn.cursor()) as c:
        c.execute(
            """
            INSERT INTO seen_urls (url_clean, last_seen_at, entry_published_at)
            VALUES (?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(url_clean) DO UPDATE SET
                last_seen_at = CURRENT_TIMESTAMP,
                entry_published_at = COALESCE(excluded.entry_published_at, seen_urls.entry_published_at)
            """,
            (url_clean, entry_published_utc),
        )
    conn.commit()


def clean_html(raw_html: str) -> str:
    text = re.sub(r"<(script|style).*?>.*?</\1>", " ", raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def extract_deal_context(raw_html: str) -> tuple[str, str]:
    unquoted = urllib.parse.unquote(raw_html)
    dates_info = []

    # 1. Datumsparameter in Buchungs-URLs (z.B. date_out=2027-02-13&date_in=2027-02-23)
    d_out = re.findall(r"(?:date_out|outbound|departure|depDate|out)=([0-9]{4}-[0-9]{2}-[0-9]{2})", unquoted, re.I)
    d_in = re.findall(r"(?:date_in|inbound|return|retDate|in)=([0-9]{4}-[0-9]{2}-[0-9]{2})", unquoted, re.I)
    for o, i in zip(d_out, d_in):
        dates_info.append(f"Gefundener Buchungstermin im Link: Hinflug {o}, Rückflug {i}")

    # 2. Airline-Parameter in Buchungs-URLs
    airline_in_link = re.findall(r"airline=([A-Z0-9]{2})", unquoted, re.I)
    if airline_in_link:
        dates_info.append(f"Gefundene Airline im Link: {airline_in_link[0].upper()}")

    booking_links = re.findall(
        r"href=[\"\'](https?://(?:go2\.travel-dealz\.de|www\.google\.com/travel/flights|google\.com/travel/flights|google\.com/flights|www\.skyscanner|www\.momondo|kayak\.|fave\.co/|awin1\.com|prf\.hn|expedia\.|trip\.com)[^\"\']+)[\"\']",
        unquoted,
        re.I,
    )

    cleaned = clean_html(raw_html)
    extra_context = "\n".join(dates_info)
    sample_booking = html.unescape(booking_links[0]) if booking_links else ""
    if sample_booking:
        extra_context += f"\nBeispiel-Buchungslink: {sample_booking}"

    full_text = f"{extra_context}\n\n{cleaned}" if extra_context else cleaned
    return full_text, sample_booking


def fetch_article_html(url: str, timeout: int = 12, session: requests.Session | None = None) -> str:
    """Lädt den HTML-Quelltext eines Artikels mit den definierten Browser-Headern herunter."""
    s = session or SESSION
    try:
        resp = s.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return resp.text
        logging.warning(f"Artikel-Abruf ({url}) mit unerwartetem Status {resp.status_code}")
    except Exception as e:
        logging.warning(f"Fehler beim Laden des Artikels {url}: {e}")
    return ""


def get_enriched_deal_context(raw_content: str, article_url: str = "") -> tuple[str, str]:
    """
    Kombiniert Feed-Inhalt mit automatischem Trafilatura-Scraping der Zielseite,
    falls der Feed-Inhalt nur ein kurzer Teaser ist (< 1.200 Zeichen) oder noch kein Buchungslink vorliegt.
    """
    feed_text, sample_booking = extract_deal_context(raw_content)

    # Wenn der Feed bereits einen ausführlichen Volltext liefert (>= 1.200 Zeichen)
    # und bereits ein Buchungslink vorhanden ist, ist kein zusätzliches Scraping nötig (z.B. Travel Dealz).
    if len(feed_text) >= 1200 and sample_booking:
        return feed_text, sample_booking

    # Falls Ziel-URL vorhanden ist, Artikel abrufen und per Trafilatura extrahieren
    if article_url and article_url.startswith("http"):
        page_html = fetch_article_html(article_url)
        if page_html:
            # Versuche Buchungslink aus der Zielseite zu extrahieren, falls im Feed noch keiner war
            if not sample_booking:
                _, page_booking = extract_deal_context(page_html)
                if page_booking:
                    sample_booking = page_booking

            try:
                extracted_text = trafilatura.extract(page_html, include_links=True)
                if extracted_text and len(extracted_text) > len(feed_text):
                    extra_info = []
                    if sample_booking:
                        extra_info.append(f"Beispiel-Buchungslink: {sample_booking}")
                    enriched = "\n".join(extra_info) + "\n\n" + extracted_text if extra_info else extracted_text
                    return enriched, sample_booking
            except Exception as e:
                logging.warning(f"Trafilatura-Extraktion fehlgeschlagen für {article_url}: {e}")

    return feed_text, sample_booking


# Stadt-/Metro-IATA-Codes, die Google Flights im Protobuf-Feld 'airport' nicht zuverlässig auflöst
METRO_TO_PRIMARY_AIRPORT: dict[str, str] = {
    "PAR": "CDG",  # Paris -> Charles de Gaulle
    "LON": "LHR",  # London -> Heathrow
    "ROM": "FCO",  # Rom -> Fiumicino
    "MIL": "MXP",  # Mailand -> Malpensa
    "STO": "ARN",  # Stockholm -> Arlanda
    "TYO": "HND",  # Tokio -> Haneda
    "OSA": "KIX",  # Osaka -> Kansai
    "SEL": "ICN",  # Seoul -> Incheon
    "BJS": "PEK",  # Peking -> Capital
    "JKT": "CGK",  # Jakarta -> Soekarno-Hatta
    "NYC": "JFK",  # New York -> JFK
    "WAS": "IAD",  # Washington -> Dulles
    "CHI": "ORD",  # Chicago -> O'Hare
    "YTO": "YYZ",  # Toronto -> Pearson
    "YMQ": "YUL",  # Montreal -> Trudeau
    "SAO": "GRU",  # São Paulo -> Guarulhos
    "RIO": "GIG",  # Rio de Janeiro -> Galeão
    "BUE": "EZE",  # Buenos Aires -> Ezeiza
    "MOW": "SVO",  # Moskau -> Sheremetyevo
    "BAK": "GYD",  # Baku -> Heydar Aliyev
    "REK": "KEF",  # Reykjavik -> Keflavik
    "BUH": "OTP",  # Bukarest -> Otopeni
    "TCI": "TFS",  # Teneriffa -> Süd
}


def normalize_airport_iata(code: str) -> str:
    """Normalisiert Stadt-/Metro-Codes (z.B. PAR -> CDG, ROM -> FCO) auf echte Flughafen-IATA-Codes."""
    cleaned = (code or "").strip().upper()
    return METRO_TO_PRIMARY_AIRPORT.get(cleaned, cleaned)


def ensure_valid_future_dates(
    out_date: str,
    ret_date: str,
    reference_date: date | None = None,
    min_lead_days: int = 2,
    shift_min_lead_days: int = 14,
    default_trip_days: int = 12,
) -> tuple[str, str]:
    """
    Stellt sicher, dass Hin- und Rückflugdatum gültig und in der Zukunft liegen.
    Liegt das Hinflugdatum in der Vergangenheit (oder zu kurzfristig für eine -1-Tag-Positionierung),
    wird es in vollen 7-Tage-Schritten (Wochentag bleibt erhalten!) in die Zukunft verschoben.
    """
    today = reference_date or datetime.now().date()
    parsed_out: date | None = None
    parsed_ret: date | None = None

    try:
        parsed_out = datetime.strptime((out_date or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        parsed_out = None

    try:
        parsed_ret = datetime.strptime((ret_date or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        parsed_ret = None

    trip_days = default_trip_days
    if parsed_out and parsed_ret:
        diff = (parsed_ret - parsed_out).days
        if 1 <= diff <= 90:
            trip_days = diff

    if parsed_out is None:
        parsed_out = today + timedelta(days=21)
        parsed_ret = parsed_out + timedelta(days=trip_days)
    elif parsed_out < today + timedelta(days=min_lead_days):
        target_min = today + timedelta(days=shift_min_lead_days)
        days_needed = (target_min - parsed_out).days
        weeks_to_add = (days_needed + 6) // 7
        parsed_out = parsed_out + timedelta(days=weeks_to_add * 7)
        parsed_ret = parsed_out + timedelta(days=trip_days)
    elif parsed_ret is None or parsed_ret <= parsed_out:
        parsed_ret = parsed_out + timedelta(days=trip_days)

    return parsed_out.strftime("%Y-%m-%d"), parsed_ret.strftime("%Y-%m-%d")


def build_gf_main_url(deal: FlightDealEvaluation, reference_date: date | None = None) -> str:
    origin = normalize_airport_iata(deal.iata_origin)
    dest = normalize_airport_iata(deal.iata_destination)
    return_dest = (
        normalize_airport_iata(deal.iata_return_end) if (deal.is_open_jaw and deal.iata_return_end) else origin
    )
    is_multi = deal.is_open_jaw and return_dest != origin
    out_date, ret_date = ensure_valid_future_dates(
        deal.sample_outbound_date, deal.sample_return_date, reference_date=reference_date
    )

    try:
        info = flights_pb2.Info()

        airline_code = deal.primary_airline_iata.strip().upper() if deal.primary_airline_iata else ""
        if not airline_code and deal.airline_names:
            m = re.search(r"\b([A-Z0-9]{2})\b", deal.airline_names)
            if m:
                airline_code = m.group(1).upper()

        fd1 = info.data.add()
        fd1.date = out_date
        fd1.from_airport.airport = origin
        fd1.to_airport.airport = dest
        if airline_code and len(airline_code) == 2:
            fd1.airlines.append(airline_code)

        fd2 = info.data.add()
        fd2.date = ret_date
        fd2.from_airport.airport = dest
        fd2.to_airport.airport = return_dest
        if airline_code and len(airline_code) == 2:
            fd2.airlines.append(airline_code)

        info.seat = 4 if deal.detected_cabin_class == "First Class" else 3  # 4 = First, 3 = Business
        info.trip = 3 if is_multi else 1  # 3 = Multi-city, 1 = Round-trip
        info.passengers.append(1)

        tfs = urllib.parse.quote(base64.b64encode(info.SerializeToString()).decode("utf-8"), safe="")
        return f"https://www.google.com/travel/flights/search?tfs={tfs}&hl=de&curr=EUR"
    except Exception as e:
        logging.warning(f"Fallback bei Google Flights Hauptflug-Link: {e}")
        airline_str = f" with {deal.primary_airline_iata}" if deal.primary_airline_iata else ""
        cabin_str = "first class" if deal.detected_cabin_class == "First Class" else "business class"
        if is_multi:
            query = f"Multi-city flights from {origin} to {dest} on {out_date} and from {dest} to {return_dest} on {ret_date}{airline_str} {cabin_str}"
        else:
            query = f"Round trip flights from {origin} to {dest} on {out_date} returning {ret_date}{airline_str} {cabin_str}"
        return f"https://www.google.com/travel/flights?hl=en&gl=de&curr=EUR&q={urllib.parse.quote_plus(query)}"


def build_positioning_url(
    origin: str, out_date: str, ret_date: str, home: str | None = None, reference_date: date | None = None
) -> str:
    home_hub = normalize_airport_iata(home or HOME_AIRPORT)
    origin_code = normalize_airport_iata(origin)
    safe_out, safe_ret = ensure_valid_future_dates(out_date, ret_date, reference_date=reference_date)
    try:
        pos_out = (datetime.strptime(safe_out, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        pos_ret = (datetime.strptime(safe_ret, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        q = create_query(
            flights=[
                FlightQuery(date=pos_out, from_airport=home_hub, to_airport=origin_code),
                FlightQuery(date=pos_ret, from_airport=origin_code, to_airport=home_hub),
            ],
            seat="economy",
            trip="round-trip",
            passengers=Passengers(adults=1),
            currency="EUR",
            language="de",
        )
        tfs = urllib.parse.quote(q.to_str(), safe="")
        return f"https://www.google.com/travel/flights/search?tfs={tfs}&hl=de&curr=EUR"
    except Exception as e:
        logging.warning(f"Fallback bei Google Flights Positionierungs-Link: {e}")
        query = f"Round trip flights from {home_hub} to {origin_code} on {safe_out} returning {safe_ret} economy"
        return f"https://www.google.com/travel/flights?hl=en&gl=de&curr=EUR&q={urllib.parse.quote_plus(query)}"


@dataclass
class GoogleFlightsLiveCheckResult:
    checked: bool = False
    main_price_eur: int | None = None
    main_outbound_date: str = ""
    main_return_date: str = ""
    price_matches_deal: bool = False
    positioning_price_eur: int | None = None
    positioning_origin: str = ""
    positioning_destination: str = ""
    error: str = ""


_GF_CONSENT_COOKIES = {
    "SOCS": "CAESHAgBEhJnd3NfMjAyMzA4MTAtMF9SQzIaAmRlIAEaBgiAo_CmBg",
}


def _build_gf_main_query(deal: FlightDealEvaluation, out_date: str, ret_date: str) -> Query:
    origin = normalize_airport_iata(deal.iata_origin)
    dest = normalize_airport_iata(deal.iata_destination)
    return_dest = (
        normalize_airport_iata(deal.iata_return_end) if (deal.is_open_jaw and deal.iata_return_end) else origin
    )
    is_multi = deal.is_open_jaw and return_dest != origin

    airline_code = deal.primary_airline_iata.strip().upper() if deal.primary_airline_iata else ""
    if not airline_code and deal.airline_names:
        m = re.search(r"\b([A-Z0-9]{2})\b", deal.airline_names)
        if m:
            airline_code = m.group(1).upper()
    airlines_filter = [airline_code] if (airline_code and len(airline_code) == 2) else None

    seat_type: Literal["business", "first"] = "first" if deal.detected_cabin_class == "First Class" else "business"
    trip_type: Literal["round-trip", "multi-city"] = "multi-city" if is_multi else "round-trip"

    return create_query(
        flights=[
            FlightQuery(date=out_date, from_airport=origin, to_airport=dest, airlines=airlines_filter),
            FlightQuery(date=ret_date, from_airport=dest, to_airport=return_dest, airlines=airlines_filter),
        ],
        seat=seat_type,
        trip=trip_type,
        passengers=Passengers(adults=1),
        currency="EUR",
        language="de",
    )


def _build_gf_positioning_query(origin: str, out_date: str, ret_date: str, home: str | None = None) -> Query:
    home_hub = normalize_airport_iata(home or HOME_AIRPORT)
    origin_code = normalize_airport_iata(origin)
    pos_out = (datetime.strptime(out_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    pos_ret = (datetime.strptime(ret_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return create_query(
        flights=[
            FlightQuery(date=pos_out, from_airport=home_hub, to_airport=origin_code),
            FlightQuery(date=pos_ret, from_airport=origin_code, to_airport=home_hub),
        ],
        seat="economy",
        trip="round-trip",
        passengers=Passengers(adults=1),
        currency="EUR",
        language="de",
    )


def fetch_gf_lowest_price_eur(query: Query, timeout_seconds: int = GF_LIVE_CHECK_TIMEOUT_SECONDS) -> int | None:
    """Fragt Google Flights mit striktem Timeout und EU-Consent-Cookie ab und gibt den günstigsten Preis in EUR zurück (Fail-Open)."""
    try:
        from curl_cffi import requests as cffi_requests

        resp = cffi_requests.get(
            "https://www.google.com/travel/flights/search",
            params=query.params(),
            impersonate="chrome124",
            timeout=timeout_seconds,
            cookies=_GF_CONSENT_COOKIES,
        )
        if resp.status_code != 200 or not resp.text:
            logging.info("Google Flights Live-Check lieferte HTTP %s", resp.status_code)
            return None

        results = parse_gf_html(resp.text)
        prices = [f.price for f in results if isinstance(getattr(f, "price", None), int) and f.price > 0]
        return min(prices) if prices else None
    except Exception as exc:
        logging.info("Google Flights Live-Check fehlgeschlagen (Fail-Open): %s", exc)
        return None


def run_google_flights_live_check(
    deal: FlightDealEvaluation,
    home: str | None = None,
    reference_date: date | None = None,
    price_fetcher: Callable[[Query], int | None] | None = None,
) -> GoogleFlightsLiveCheckResult:
    """
    Soft Live-Check auf Google Flights (100% Fail-Open):
    1. Prüft bis zu 3 wochentagserhaltende Terminpaare (+0, +14, +28 Tage) für den Hauptflug.
       Wird ein besserer / bestätigter Termin gefunden, werden sample_outbound_date & sample_return_date
       direkt am Deal aktualisiert, damit alle Links auf den bestätigten Termin zeigen.
    2. Prüft bei Ex-EU-Deals zusätzlich den günstigsten Live-Preis für den Positionierungsflug ab HOME_AIRPORT.
    """
    fetcher = price_fetcher or fetch_gf_lowest_price_eur
    home_hub = normalize_airport_iata(home or HOME_AIRPORT)
    origin = normalize_airport_iata(deal.iata_origin)
    base_out, base_ret = ensure_valid_future_dates(
        deal.sample_outbound_date, deal.sample_return_date, reference_date=reference_date
    )

    result = GoogleFlightsLiveCheckResult(
        checked=True,
        main_outbound_date=base_out,
        main_return_date=base_ret,
    )

    # Toleranzschwelle für bestätigten Deal-Preis (+15 % bzw. mind. +150 € wegen OTA- vs. Direktpreis-Differenz)
    max_match_price = max(int(deal.price_eur * 1.15), deal.price_eur + 150) if deal.price_eur > 0 else 0

    base_out_dt = datetime.strptime(base_out, "%Y-%m-%d").date()
    base_ret_dt = datetime.strptime(base_ret, "%Y-%m-%d").date()

    best_price: int | None = None
    best_out = base_out
    best_ret = base_ret

    try:
        for offset_days in (0, 14, 28):
            cand_out = (base_out_dt + timedelta(days=offset_days)).strftime("%Y-%m-%d")
            cand_ret = (base_ret_dt + timedelta(days=offset_days)).strftime("%Y-%m-%d")
            q_main = _build_gf_main_query(deal, cand_out, cand_ret)
            price = fetcher(q_main)
            if price is not None and (best_price is None or price < best_price):
                best_price = price
                best_out = cand_out
                best_ret = cand_ret
            if price is not None and (max_match_price <= 0 or price <= max_match_price):
                break

        if best_price is not None:
            result.main_price_eur = best_price
            result.main_outbound_date = best_out
            result.main_return_date = best_ret
            result.price_matches_deal = max_match_price <= 0 or best_price <= max_match_price
            # Übernehme den günstigsten gefundenen Termin direkt auf den Deal für Haupt- & Positionierungslink
            deal.sample_outbound_date = best_out
            deal.sample_return_date = best_ret
    except Exception as exc:
        result.error = str(exc)
        logging.info("Hauptflug Live-Check abgebrochen (Fail-Open): %s", exc)

    # Optionaler Positionierungsflug (nur bei Ex-EU Abflug außerhalb der bevorzugten Hubs)
    if origin and origin not in PREFERRED_HUBS and origin != home_hub:
        result.positioning_origin = home_hub
        result.positioning_destination = origin
        try:
            q_pos = _build_gf_positioning_query(
                origin, result.main_outbound_date, result.main_return_date, home=home_hub
            )
            result.positioning_price_eur = fetcher(q_pos)
        except Exception as exc:
            logging.info("Positionierungsflug Live-Check abgebrochen (Fail-Open): %s", exc)

    return result


def generate_with_gemini_fallback(
    client: genai.Client,
    prompt: str,
    response_schema: Type[BaseModel] | None = None,
    preferred_model: str = "gemini-3.8-flash",
    enable_thinking: bool = False,
    max_transient_retries: int = 4,
    http_timeout_ms: int = GEMINI_HTTP_TIMEOUT_MS,
) -> Any:
    """Führt Content-Generierung mit automatischer Modell-Kaskade, striktem HTTP-Timeout und Retry-Logik aus."""
    fallback_models = [preferred_model]
    for fb in [
        "gemini-3.8-flash",
        "gemini-flash-latest",
        "gemini-3.5-flash",
        "gemini-3.1-flash-lite",
        "gemini-3.5-flash-lite",
        "gemini-flash-lite-latest",
    ]:
        if fb not in fallback_models:
            fallback_models.append(fb)

    candidate_models = list(fallback_models)
    transient_attempt = 0
    transient_delays = [10, 25, 45, 60]

    while candidate_models and transient_attempt <= max_transient_retries:
        current_model = candidate_models[0]

        # Konfiguration vorbereiten (inkl. explizitem Request-Timeout & warnungsfrei deaktiviertem AFC)
        gen_config: dict[str, Any] = {
            "temperature": 0.1,
            "http_options": {"timeout": http_timeout_ms},
            "automatic_function_calling": {"disable": True, "maximum_remote_calls": None},
        }
        if response_schema:
            gen_config["response_mime_type"] = "application/json"
            gen_config["response_schema"] = response_schema
        if enable_thinking:
            gen_config["thinking_config"] = {"include_thoughts": True}

        try:
            response = client.models.generate_content(
                model=current_model,
                contents=prompt,
                config=gen_config,
            )
            logging.info("Erfolgreich generiert mit Modell %s", current_model)
            return response

        except Exception as exc:
            exc_str = str(exc)

            # Fall A: Modell unterstützt Thinking nicht -> Feature-Degradation
            if "thinking" in exc_str.lower() and "thinking_config" in gen_config:
                logging.warning(
                    "Modell %s unterstützt thinking_config nicht. Retry ohne Thinking...",
                    current_model,
                )
                gen_config.pop("thinking_config", None)
                try:
                    response = client.models.generate_content(
                        model=current_model,
                        contents=prompt,
                        config=gen_config,
                    )
                    logging.info("Erfolgreich generiert ohne Thinking mit Modell %s", current_model)
                    return response
                except Exception as inner_exc:
                    exc = inner_exc
                    exc_str = str(inner_exc)

            # Fall B: Modell existiert nicht / veraltet (404 Not Found) -> sofort nächstes Modell
            is_not_found = (isinstance(exc, errors.APIError) and exc.code == 404) or (
                "404" in exc_str or "NOT_FOUND" in exc_str
            )

            if is_not_found:
                logging.warning(
                    "Modell %s nicht verfügbar (404). Schalte auf nächstes Modell um...",
                    current_model,
                )
                candidate_models.pop(0)
                if not candidate_models:
                    raise
                continue

            # Fall C: Transiente Fehler (429 Quota, 503 Overload, Netzwerk-/Socket-Timeouts) -> Wait & Shift
            is_transient = (
                (isinstance(exc, errors.APIError) and exc.code in (408, 429, 500, 502, 503, 504))
                or isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError, TimeoutError))
                or any(
                    code in exc_str
                    for code in (
                        "503",
                        "429",
                        "UNAVAILABLE",
                        "ResourceExhausted",
                        "RESOURCE_EXHAUSTED",
                        "timeout",
                        "Timeout",
                        "Read timed out",
                        "RemoteDisconnected",
                        "Connection aborted",
                    )
                )
            )

            if is_transient and transient_attempt < max_transient_retries:
                transient_attempt += 1

                # Wartezeit ermitteln
                retry_match = re.search(r"retry.*?(\d+(?:\.\d+)?)\s*s", exc_str, re.IGNORECASE)
                retry_h_match = re.search(r"retry.*?(\d+)\s*h", exc_str, re.IGNORECASE)
                if retry_h_match or (retry_match and float(retry_match.group(1)) > 65):
                    # Tägliches Limit für dieses Modell erschöpft (>65s) -> sofort Modell aus der Liste werfen
                    logging.warning(
                        "Tageskontingent für Modell %s erschöpft. Wechsle sofort auf nächstes Modell...",
                        current_model,
                    )
                    candidate_models.pop(0)
                    if not candidate_models:
                        raise exc
                    continue
                elif retry_match:
                    sleep_time = int(float(retry_match.group(1))) + 2
                elif "429" in exc_str:
                    sleep_time = min(35 * transient_attempt, 60)
                else:
                    base_delay = transient_delays[min(transient_attempt - 1, len(transient_delays) - 1)]
                    sleep_time = int(base_delay + random.uniform(1.0, 4.0))

                # Bei Überlastung nächstes Modell anwählen
                if len(candidate_models) > 1:
                    candidate_models.pop(0)
                next_model = candidate_models[0]

                logging.warning(
                    "Transiente Gemini-Überlastung auf Modell %s (%s). Retry %d/%d mit %s in %ds...",
                    current_model,
                    exc,
                    transient_attempt,
                    max_transient_retries,
                    next_model,
                    sleep_time,
                )
                time.sleep(sleep_time)
            else:
                raise

    raise RuntimeError("Alle Gemini-Modelle und Retries erschöpft.")


def normalize_additional_destinations(
    raw_destinations: list[str] | None,
    primary_dest: str = "",
    max_items: int = 6,
) -> list[str]:
    """Bereinigt weitere Sale-Ziele: entfernt Platzhalter, Duplikate sowie das Hauptziel selbst."""
    if not raw_destinations:
        return []

    primary = normalize_airport_iata(primary_dest)
    placeholder_values = {"none", "null", "kein", "keine", "keiner", "-", "n/a", "nicht genannt"}
    cleaned: list[str] = []
    seen_keys: set[str] = set()

    for entry in raw_destinations:
        if not isinstance(entry, str):
            continue
        text = re.sub(r"\s+", " ", entry).strip().strip(",;")
        if not text or text.lower() in placeholder_values:
            continue

        codes = [normalize_airport_iata(c) for c in re.findall(r"\b([A-Z]{3})\b", text.upper())]
        non_currency_codes = [c for c in codes if c not in {"EUR", "USD", "CHF", "GBP"}]
        if primary and primary in non_currency_codes:
            continue

        dedup_key = non_currency_codes[0] if non_currency_codes else text.lower()
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        cleaned.append(text)
        if len(cleaned) >= max_items:
            break

    return cleaned


def evaluate_with_gemini(client: genai.Client, title: str, clean_text: str) -> FlightDealEvaluation:
    hubs_list = sorted(
        {
            str(h).strip().upper()
            for h in CONFIG.get("origin", {}).get("preferred_hubs", list(PREFERRED_HUBS))
            if str(h).strip()
        }
    )
    hubs_str = ", ".join(hubs_list)
    dest_regions = CONFIG["destinations"].get("regions", ["Asien", "Zentralasien", "Südamerika", "Mittelamerika"])
    regions_str = ", ".join(dest_regions)
    central_asia_hint = ""
    if any(r in dest_regions for r in ("Asien", "Zentralasien")):
        central_asia_hint = (
            "\n   - WICHTIG ZU ASIEN & ZENTRALASIEN: Neben Ost-, Südost- und Südasien (inkl. Japan, Korea, Indien, Malediven) "
            "zählt auch ZENTRALASIEN (Kasachstan mit Almaty ALA & Astana NQZ/TSE, Usbekistan mit Taschkent TAS & Samarkand SKD, "
            "Kirgisistan FRU, Tadschikistan DYU) VOLLUMFÄNGLICH zu den erlaubten Zielregionen! "
            "Lehne Deals nach Kasachstan oder Usbekistan NIEMALS wegen der Zielregion ab!"
            "\n   - AUSGESCHLOSSEN: Australien / Ozeanien (z.B. SYD, MEL, PER, ADL, AKL), Nordamerika, Afrika und Europa gehören NICHT zu Asien -> als region = 'Sonstige' und is_relevant = False ablehnen!"
        )
    sweet_spots = CONFIG["destinations"].get(
        "sweet_spots",
        ["DXB", "DWC", "AUH", "JED", "RUH", "MCT", "SLL", "DOH", "AMM", "AQJ", "TLV", "BEY", "TBS", "BUS", "KUT"],
    )
    sweet_spots_str = ", ".join(sweet_spots)
    priority_dests = [
        str(d).strip().upper() for d in CONFIG["destinations"].get("priority_destinations", []) if str(d).strip()
    ]
    priority_dests_hint = ""
    if priority_dests:
        priority_dests_str = ", ".join(priority_dests)
        priority_dests_hint = (
            f"\n   - WUNSCHZIELE BEI SAMMEL-DEALS: Wenn der Artikel MEHRERE Ziele in erlaubten Zielregionen anbietet "
            f"und eines der Wunschziele ({priority_dests_str}) explizit als buchbar genannt UND preislich im Limit ist, "
            f"wähle ZWINGEND dieses Wunschziel als 'iata_destination' (mit dessen konkretem Preis als 'price_eur')!"
        )
    multi_dest_hint = (
        "\n   - WEITERE ZIELE IM SALE ('additional_destinations'): Wenn der Artikel mehrere buchbare Ziele in erlaubten Zielregionen "
        "auflistet, nenne bis zu 6 weitere Ziele (OHNE 'iata_destination' zu wiederholen) kompakt in 'additional_destinations' "
        "(Format: 'KUL (~1.820 €)' bzw. 'KUL', falls kein Einzelpreis genannt). Bei Einzelziel-Deals: []."
    )
    price_hubs = CONFIG["pricing"].get("max_price_home_hubs", 2600)
    price_ex_eu = CONFIG["pricing"].get("max_price_ex_eu", 2000)
    ignored_airlines = CONFIG.get("airlines", {}).get("ignored_airlines", IGNORED_AIRLINES)
    ignored_airlines_str = ", ".join(ignored_airlines) if ignored_airlines else "Keine"

    prompt = f"""
Analysiere diesen Flug-Deal für einen Reisenden mit Basis {HOME_AIRPORT} (Bevorzugte Hubs: {hubs_str}). Aktuelles Datum: {datetime.now().strftime("%Y-%m-%d")}.

STRIKTE REGELN FÜR 'is_relevant = True':
1. REISEKLASSE & BEWEISPFLICHT (NULL-TOLERANZ):
   - Der Deal ist NUR DANN relevant (is_relevant = True), wenn im Text EXPLIZIT 'Business Class', 'First Class' oder 'BIZclass' genannt ist.
   - Begriffe wie 'Full-service', 'Direktflug', 'Traumreise', 'Billigflug' oder bloße Flugzeugbezeichnungen (z.B. A330neo, 787) bedeuten KEINESFALLS Business Class!
   - Erfinde oder vermute NIEMALS eine Reiseklasse oder ein Bordprodukt. Wenn keine Business Class oder First Class explizit im Text nachweisbar ist:
     * Setze detected_cabin_class = 'Unklar / Nicht genannt' oder 'Economy / Premium Eco'
     * Setze cabin_class_quote = 'KEIN NACHWEIS'
     * Setze is_relevant = False
     * Setze deal_category = 'Irrelevant'
     * Setze rejection_reason = 'Nur Economy / Keine Business Class explizit genannt'
2. Zielregion: {regions_str} (oder erlaubte Sweet-Spot-Airports inkl. Nahost-QP & Georgien: {sweet_spots_str}).{central_asia_hint}{priority_dests_hint}{multi_dest_hint}
3. STARTFLUGHAFEN, ZWISCHENSTOPPS & PRIORISIERUNG (KEIN ORIGIN-HIJACKING):
   - 'iata_origin' MUSS zwingend ein Flughafen sein, der im Artikel oder Buchungslink EXPLIZIT als buchbarer Abflugort für diesen Tarif genannt ist!
   - Wenn ein Deal NUR ab Warschau (WAW) oder Oslo (OSL) angeboten wird, MUSS iata_origin 'WAW' bzw. 'OSL' sein.
   - Es ist STRENGSTENS VERBOTEN, einen bevorzugten Hub ({hubs_str}) als iata_origin einzusetzen, wenn dieser Flughafen nicht im Artikel als buchbarer Abflugort aufgeführt ist!
   - Nur wenn der Deal MEHRERE Abflughäfen anbietet (z.B. 'Ab Frankfurt, München und Amsterdam'), wähle den bevorzugten Hub ({hubs_str}) als iata_origin. Nenne weitere Abflughäfen in 'routing_advice'.
   - ERFORDERLICHE ZWISCHENSTOPPS ('iata_via'): Gib ALLE erforderlichen Umsteigeflughäfen zwischen iata_origin und iata_destination als 3-Letter IATA-Codes in Flugrichtung an (z.B. ['HEL'] bei Finnair AMS -> HEL -> BKK, ['DOH'] bei Qatar Airways, ['AUH'] bei Etihad, ['ZRH'] bei SWISS, ['WAW'] bei LOT ab Westeuropa, ['IST'] bei Turkish Airlines).
   - Auch wenn das Drehkreuz im Text nicht ausdrücklich ausgeschrieben wird, die Hauptairline aber keinen Nonstop-Flug von iata_origin nach iata_destination durchführt, MUSS das erforderliche Drehkreuz der Airline in 'iata_via' stehen!
   - Bei 1-Stopp-Verbindungen mit wahlweisem Hub (z.B. Lufthansa via Frankfurt oder München) schreibe ['FRA/MUC'] als EIN Element (nur bei zwei echten Stopps hintereinander zwei Elemente wie ['FRA', 'SIN']). Bei echten Nonstop-Flügen: [].
4. Preis (Fremdwährungen wie USD, CHF, GBP, PLN, SEK, DKK zwingend in EUR umrechnen!):
   - Ab {hubs_str}: max. {price_hubs} EUR Return (jeder Preis <= {price_hubs} EUR ist preislich erlaubt).
   - Ab allen anderen europäischen Flughäfen / Ex-EU (z.B. CPH, MUC, AMS, WAW, OSL, ARN, PRG, DUB, MXP, BUD): max. {price_ex_eu} EUR Return.
5. Miles & More Punkte-Logik (Stand Oktober 2026 | FTL-Statusziel: 650 Points, davon mind. 325 QP):
   - Vollintegriert (Points UND QP): LH (Lufthansa), VL (City Airlines), LX (SWISS), OS (Austrian Airlines), SN (Brussels Airlines), EW (Eurowings), 4Y (Discover Airlines), EN (Air Dolomiti), LO (LOT Polish Airlines), AZ (ITA Airways), LG (Luxair), OU (Croatia Airlines).
     -> KRITISCH LOT POLISH AIRLINES (LO): LOT ist ein VOLLINTEGRIERTER Miles & More Partner!
     -> Flüge mit LOT Polish Airlines bringen IMMER Points UND Qualifying Points (QP) im Verhältnis 1:1! NIEMALS 0 QP für LOT vergeben!
     -> deal_category für LOT MUSS 'FTL-Booster (Points + QP)' sein (NIEMALS 'Star Alliance (Nur Points)')!
     -> Kontinental Business = 40 Points + 40 QP pro Segment. Interkontinental Business = 200 Points + 200 QP pro Segment (First = 400).
     -> Beispiel LOT Return ab Deutschland/Europa via Warschau (z.B. FRA-WAW-NRT-WAW-FRA): 2x Zubringer (2x40) + 2x Langstrecke (2x200) = 480 Points und 480 QP!
     -> Beispiel LOT Return Direktflug ab Warschau (WAW-NRT-WAW): 2x Langstrecke (2x200) = 400 Points und 400 QP!
   - KRITISCH Zentralasien: Kasachstan (ALA/NQZ/TSE), Usbekistan (TAS/SKD), Kirgisistan (FRU), Tadschikistan (DYU) zählen bei Miles & More als INTERKONTINENTAL (200 Points pro Segment ab Europa/IST; bei LH/LO zusätzlich 200 QP pro Segment!). Kaukasus (TBS/EVN/GYD) zählt hingegen als kontinental (40 Points).
   - KRITISCH Nordafrika: Ägypten (CAI/HRG), Marokko (CMN/RAK), Tunesien (TUN), Algerien (ALG) zählen als KONTINENTAL (nur 40 Points/QP pro Segment, niemals 200!).
   - KRITISCH Edelweiss (WK): Gibt 0 Points/0 QP unter WK-Flugnummer! Nur unter LX-Codeshare (LX8000-8499) gibt es Punkte/QP.
   - KRITISCH Asiana (OZ): Seit 15.10.2026 ausgeschieden -> 0 Points / 0 QP!
   - Andere Star Alliance (SQ, TP, TK, BR, NH, UA, AC, CM, AV, TG, ET, CA, MS, AI, ZH): Bringen NUR Points (40 kontinental / 200 pro Langstrecke), aber 0 QP!
   - Fremd-Allianzen & allianzlose Airlines: 0 Points / 0 QP, aber relevant wenn Preis (<= {price_ex_eu} EUR Ex-EU bzw. <= {price_hubs} EUR ab {hubs_str}) & Hardware (z.B. Qsuite, Etihad Business Studio, Oman Air Apex Suite) stimmen:
     * Oneworld (QR, AY, BA, CX, JL, WY, RJ, IB, UL, AT, MH, QF, AA): deal_category = 'Top-Bordprodukt (Oneworld)'
     * SkyTeam (AF, KL, VS, UX, SK, SV, KE, CI, VN, GA, MU, MF, DL, AM): deal_category = 'Top-Bordprodukt (SkyTeam)'
     * Keine Allianz (z.B. EY, EK, GF, CZ, DE, KU, JX): deal_category = 'Top-Bordprodukt' (OHNE Allianz-Klammerzusatz!)
6. Besonderheiten & Tarifdetails ('routing_advice'):
   - Nenne HIER nur echte operative Zusatzfakten aus dem Text: weitere buchbare Abflughäfen, außergewöhnlich lange Layover (> 8h), Flughafenwechsel oder Tarifregeln (Stopover/Mindestaufenthalt).
   - WICHTIG & STRENG VERBOTEN: Wiederhole NIEMALS Meilen, 0 Points/QP, Allianzen, Positionierungshinweise (24h-Puffer) oder generische Preis-Checks / Buchungstipps (wie 'OTA-Bestpreis vs. Direktbuchung', Momondo/Kayak/Skyscanner, Zahlungsaufschläge, Phantom-Verfügbarkeit)!
   - Wenn keine solchen echten Besonderheiten im Artikel stehen: Setze routing_advice = "".
7. Reisedaten, Flughafen-Codes & Airline-Code:
   - WICHTIG FLUGHAFEN-CODES: Nutze für 'iata_origin', 'iata_destination' und 'iata_return_end' ZWINGEND echte 3-Letter Flughafen-Codes (z.B. 'CDG' statt 'PAR', 'FCO' statt 'ROM', 'LHR' statt 'LON', 'MXP' statt 'MIL', 'ARN' statt 'STO', 'HND'/'NRT' statt 'TYO', 'CGK' statt 'JKT') – NIEMALS Stadt-/Metro-Sammelcodes!
   - WICHTIG REISEDATEN: Nutze die im Text oder Buchungslink genannten Beispieldaten (Hinflug / Rückflug), SOFERN sie in der Zukunft liegen (mindestens 14 Tage nach dem aktuellen Datum!). Liegt das genannte Beispieldatum in der Vergangenheit oder ist keines genannt, wähle ein plausibles ZUKÜNFTIGES Datum im genannten Reisezeitraum (Format YYYY-MM-DD).
   - Setze 'primary_airline_iata' zwingend auf den 2-Letter Code der ausführenden Hauptairline (z.B. 'LO' für LOT Polish Airlines, 'LX', 'OS', 'LH', 'QR', 'EY', 'TK', 'RJ', 'UX') für den automatischen Google-Flights-Filter.
8. Ignorierte Airlines: Flüge von folgenden Airlines sind STRIKT IRRELEVANT (is_relevant = False, deal_category = 'Irrelevant') und dürfen NIEMALS als Angebot angezeigt werden: {ignored_airlines_str}. Setze in diesem Fall 'rejection_reason = Airline ignoriert (Name/Code)'.
9. Ablehnungsgrund ('rejection_reason'):
   - Falls 'is_relevant = False' oder 'deal_category = Irrelevant': Gib zwingend eine prägnante, klare Begründung an (z.B. 'Nur Economy / Keine Business Class explizit genannt', 'Airline ignoriert (AZAL)', 'One-Way Flug (nur Return erlaubt)', 'Startflughafen nicht in Europa (SIN)', 'Zielregion nicht im Suchfokus (Europa)', 'Preis über Limit: 2.450 EUR').
   - Falls relevant: leer lassen ("").

Titel: {title}
Inhalt: {clean_text[:5000]}
"""
    response = generate_with_gemini_fallback(
        client=client,
        prompt=prompt,
        response_schema=FlightDealEvaluation,
        preferred_model=GEMINI_MODEL,
        enable_thinking=False,
    )
    if response.parsed is None:
        raise ValueError("Gemini lieferte eine leere strukturierte Antwort (response.parsed ist None).")

    deal = response.parsed
    deal.iata_origin = normalize_airport_iata(deal.iata_origin)
    deal.iata_destination = normalize_airport_iata(deal.iata_destination)
    deal.iata_return_end = normalize_airport_iata(deal.iata_return_end)
    deal.sample_outbound_date, deal.sample_return_date = ensure_valid_future_dates(
        deal.sample_outbound_date, deal.sample_return_date
    )
    deal = normalize_and_verify_deal_mm_points(deal)
    deal.iata_via = resolve_deal_stops(deal)
    deal.additional_destinations = normalize_additional_destinations(
        deal.additional_destinations, deal.iata_destination
    )
    is_ignored, ignored_air = is_deal_ignored_airline(deal, ignored_airlines)
    if is_ignored:
        deal.is_relevant = False
        deal.deal_category = "Irrelevant"
        if not deal.rejection_reason:
            deal.rejection_reason = f"Airline ignoriert ({ignored_air})"

    return deal


def _format_short_date_range(out_date: str, ret_date: str) -> str:
    try:
        o = datetime.strptime(out_date, "%Y-%m-%d").strftime("%d.%m.")
        r = datetime.strptime(ret_date, "%Y-%m-%d").strftime("%d.%m.")
        return f"{o}–{r}"
    except ValueError:
        return f"{out_date}–{ret_date}"


def build_telegram_message(
    deal: FlightDealEvaluation,
    title: str,
    url: str,
    sample_booking: str = "",
    prev_price: int = 0,
    live_check: GoogleFlightsLiveCheckResult | None = None,
) -> str:
    origin_code = normalize_airport_iata(deal.iata_origin)
    dest_code = normalize_airport_iata(deal.iata_destination)
    gf_main = build_gf_main_url(deal)
    pos_line = ""
    if origin_code not in PREFERRED_HUBS:
        gf_pos = build_positioning_url(
            origin_code, deal.sample_outbound_date, deal.sample_return_date, home=HOME_AIRPORT
        )
        pos_price_suffix = (
            f", ab ~{live_check.positioning_price_eur} €"
            if (live_check and live_check.positioning_price_eur is not None)
            else ""
        )
        pos_line = f'\n🔹 <a href="{gf_pos}">Positionierung: {html.escape(HOME_AIRPORT)}–{html.escape(origin_code)} (+/- 24h Stopover{pos_price_suffix})</a>'

    booking_line = ""
    if sample_booking and sample_booking.startswith("http"):
        booking_line = f'\n🔹 <a href="{html.escape(sample_booking, quote=True)}">Geprüftes Buchungsbeispiel (Skyscanner / OTA)</a>'

    stops = resolve_deal_stops(deal)
    route_nodes = [origin_code, *stops, dest_code]
    route_str = " ➔ ".join(f"<code>{html.escape(node)}</code>" for node in route_nodes if node)
    if deal.is_open_jaw and deal.iata_return_end and deal.iata_return_end != deal.iata_origin:
        route_str += f" ➔ <code>{html.escape(normalize_airport_iata(deal.iata_return_end))}</code> (Gabelflug)"

    ftl_info = ""
    if deal.mm_points > 0:
        pct_p = round((deal.mm_points / 650) * 100)
        pct_qp = round((deal.mm_qualifying_points / 325) * 100)
        ftl_info = f" (FTL-Ziel: ~{pct_p}% Points / ~{pct_qp}% QP)"

    airline_label = f" ({html.escape(deal.primary_airline_iata)})" if deal.primary_airline_iata else ""

    live_check_line = ""
    if live_check and live_check.main_price_eur is not None:
        date_range = _format_short_date_range(
            live_check.main_outbound_date or deal.sample_outbound_date,
            live_check.main_return_date or deal.sample_return_date,
        )
        if live_check.price_matches_deal:
            live_check_line = (
                f"• <b>Google Flights Live-Check:</b> ✅ ab ~{live_check.main_price_eur} € bestätigt ({date_range})\n"
            )
        else:
            live_check_line = (
                f"• <b>Google Flights Live-Check:</b> ⚠️ am Beispieltermin ab ~{live_check.main_price_eur} € "
                f"({date_range} – andere Daten/OTA prüfen)\n"
            )

    # Besonderheiten (routing_advice) konditional & mit Sicherheitsfilter (Meilen, Allianzen & generische Boilerplate) einbinden
    advice_line = ""
    raw_advice = (deal.routing_advice or "").strip()
    forbidden_advice_keywords = [
        "point",
        "qp",
        "miles & more",
        "allianz",
        "star alliance",
        "preis-check",
        "ota-bestpreis",
        "phantom-verfügbarkeit",
        "phantomverfügbarkeit",
        "zahlungsaufschläge",
        "airline-direktbuchung",
        "momondo",
        "kayak",
        "skyscanner",
    ]
    if raw_advice and not any(kw in raw_advice.lower() for kw in forbidden_advice_keywords):
        advice_line = f"• <b>Besonderheiten:</b> {html.escape(raw_advice)}\n"

    # Header & Preisangabe (bei Preissenkungen & Wunschzielen hervorheben)
    priority_dest_set = {
        str(d).strip().upper()
        for d in CONFIG.get("destinations", {}).get("priority_destinations", [])
        if str(d).strip()
    }
    priority_suffix = " ⭐" if dest_code in priority_dest_set else ""
    category_label = resolve_deal_category(deal)
    if prev_price > 0 and prev_price > deal.price_eur:
        header_line = f"📉 <b>PREIS-UPDATE: {html.escape(category_label)}{priority_suffix}</b>"
        diff = prev_price - deal.price_eur
        price_line = f"• <b>Preis:</b> ~{deal.price_eur} € 📉 <i>(-{diff} € gegenüber bisher ~{prev_price} €!)</i> (Buchungsklasse: <code>{html.escape(deal.booking_class)}</code>)"
    else:
        header_line = f"✈️ <b>{html.escape(category_label)}{priority_suffix}</b>"
        price_line = (
            f"• <b>Preis:</b> ~{deal.price_eur} € (Buchungsklasse: <code>{html.escape(deal.booking_class)}</code>)"
        )

    extra_dests = normalize_additional_destinations(deal.additional_destinations, dest_code)
    extra_dests_line = ""
    if extra_dests:
        formatted_dests = ", ".join(html.escape(d) for d in extra_dests)
        extra_dests_line = f"• <b>Weitere Ziele im Sale:</b> {formatted_dests}\n"

    cabin_link_label = "First" if deal.detected_cabin_class == "First Class" else "Business"

    msg = (
        f"{header_line}\n\n"
        f"<b>{html.escape(title)}</b>\n\n"
        f"• <b>Route:</b> {route_str} ({html.escape(deal.airline_names)})\n"
        f"{extra_dests_line}"
        f"{price_line}\n"
        f"{live_check_line}"
        f"• <b>Miles & More:</b> {deal.mm_points} Points | <b>{deal.mm_qualifying_points} QP</b>{ftl_info}\n"
        f"• <b>Bordprodukt:</b> {html.escape(deal.cabin_assessment)}\n"
        f"{advice_line}\n"
        f"🔗 <b>Direktlinks:</b>\n"
        f'🔹 <a href="{html.escape(url, quote=True)}">Deal-Quelle & Tarifdetails</a>\n'
        f'🔹 <a href="{gf_main}">Google Flights: {html.escape(origin_code)}–{html.escape(dest_code)}{airline_label} ({cabin_link_label})</a>'
        f"{pos_line}"
        f"{booking_line}"
    )
    return msg


def send_telegram(
    deal: FlightDealEvaluation,
    title: str,
    url: str,
    sample_booking: str = "",
    session: requests.Session | None = None,
    prev_price: int = 0,
    live_check: GoogleFlightsLiveCheckResult | None = None,
):
    msg = build_telegram_message(deal, title, url, sample_booking, prev_price=prev_price, live_check=live_check)
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    s = session or SESSION
    for attempt in range(3):
        resp = s.post(
            tg_url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        if resp.status_code == 429:
            retry_after = int(resp.json().get("parameters", {}).get("retry_after", 10))
            logging.warning(f"Telegram Rate Limit. Warte {retry_after}s...")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return


def fetch_flynous_synthetic_rss() -> str:
    """Scrapt Flynous HTML und generiert valides RSS 2.0 XML."""
    try:
        from curl_cffi import requests as cffi_requests
        from selectolax.lexbor import LexborHTMLParser

        r = cffi_requests.get("https://www.flynous.com/", impersonate="chrome124", timeout=15)
        if r.status_code != 200:
            logging.warning(f"Flynous HTML-Abruf fehlgeschlagen mit Status {r.status_code}")
            return ""
        tree = LexborHTMLParser(r.text)
        items = []
        for art in tree.css("article"):
            h = art.css_first("h1, h2, h3, .entry-title")
            a = h.css_first("a") if h else art.css_first("a")
            if not a:
                continue
            title = a.text(strip=True)
            link = a.attributes.get("href", "")
            summary_elem = art.css_first(".entry-summary, .entry-content, p")
            summary = summary_elem.text(strip=True) if summary_elem else title
            if title and link:
                items.append(
                    f"<item><title>{html.escape(title)}</title><link>{html.escape(link)}</link><description>{html.escape(summary)}</description></item>"
                )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<rss version="2.0">\n'
            "<channel>\n"
            "  <title>Flynous Flight Deals</title>\n"
            "  <link>https://www.flynous.com/</link>\n"
            "  <description>Flynous Deals ab Europa</description>\n"
            f"  {''.join(items)}\n"
            "</channel>\n"
            "</rss>"
        )
    except Exception as e:
        logging.warning(f"Fehler beim Erstellen des Flynous RSS-Feeds: {e}")
        return ""


def fetch_secretflying_synthetic_rss() -> str:
    """Scrapt Secret Flying (Euro Deals) und generiert valides RSS 2.0 XML."""
    try:
        from curl_cffi import requests as cffi_requests
        from selectolax.lexbor import LexborHTMLParser

        r = cffi_requests.get("https://www.secretflying.com/euro-deals/", impersonate="chrome124", timeout=15)
        if r.status_code != 200:
            logging.warning(f"Secret Flying HTML-Abruf fehlgeschlagen mit Status {r.status_code}")
            return ""
        tree = LexborHTMLParser(r.text)
        items = []
        seen = set()
        for art in tree.css("article, .post"):
            h = art.css_first("h2, h3, .entry-title")
            a = h.css_first("a") if h else art.css_first('a[href*="/posts/"]')
            if not a:
                continue
            link = a.attributes.get("href", "")
            title = a.text(strip=True)
            if not title:
                for sub_a in art.css('a[href*="/posts/"]'):
                    t = sub_a.text(strip=True)
                    if t and len(t) > 10:
                        title = t
                        break
            if not title or not link or link in seen:
                continue
            seen.add(link)
            summary_elem = art.css_first(".entry-summary, .entry-content, p")
            summary = summary_elem.text(strip=True) if summary_elem else title
            items.append(
                f"<item><title>{html.escape(title)}</title><link>{html.escape(link)}</link><description>{html.escape(summary)}</description></item>"
            )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<rss version="2.0">\n'
            "<channel>\n"
            "  <title>Secret Flying Europe Deals</title>\n"
            "  <link>https://www.secretflying.com/euro-deals/</link>\n"
            "  <description>Secret Flying Deals</description>\n"
            f"  {''.join(items)}\n"
            "</channel>\n"
            "</rss>"
        )
    except Exception as e:
        logging.warning(f"Fehler beim Erstellen des Secret Flying RSS-Feeds: {e}")
        return ""


def fetch_feed_content(feed_url: str, session: requests.Session | None = None) -> str:
    if "flynous.com" in feed_url:
        return fetch_flynous_synthetic_rss()
    if "secretflying.com" in feed_url:
        return fetch_secretflying_synthetic_rss()

    s = session or SESSION
    try:
        resp = s.get(feed_url, headers=HEADERS, timeout=15)
        if resp.status_code == 200 and ("<rss" in resp.text or "<feed" in resp.text or "<?xml" in resp.text):
            return resp.text
        logging.warning(f"Feed-Abruf unerwarteter Status/Inhalt für {feed_url}: {resp.status_code}")
    except Exception as e:
        logging.warning(f"Fehler beim Abruf von Feed {feed_url}: {e}")
    return ""


def fetch_all_feeds(
    feed_urls: list[str],
    max_workers: int | None = None,
    session: requests.Session | None = None,
    max_entries_per_feed: int | None = None,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """
    Lädt alle Feeds parallel mit ThreadPoolExecutor herunter und bewahrt die Feed-Reihenfolge.
    Verifiziert jeden Feed auf Erreichbarkeit & Inhalt und dokumentiert den Health-Status im Log.
    """
    s = session or SESSION
    limit = max_entries_per_feed if max_entries_per_feed is not None else MAX_ENTRIES_PER_FEED
    workers = max_workers or len(feed_urls)
    results_map: dict[str, list[dict[str, Any]]] = {}
    feed_issues: list[str] = []

    def _fetch_one(u: str) -> tuple[str, list[dict[str, Any]], str]:
        try:
            raw_xml = fetch_feed_content(u, session=s)
            if not raw_xml:
                return (u, [], "Abruf fehlgeschlagen / leerer Inhalt")
            feed = feedparser.parse(raw_xml)
            entries = feed.entries[:limit] if limit and limit > 0 else feed.entries
            if not entries:
                return (u, [], "0 Einträge im Feed")
            return (u, entries, "ok")
        except Exception as feed_err:
            logging.error(f"Fehler beim Parsen von Feed {u}: {feed_err}")
            return (u, [], str(feed_err))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_one, url): url for url in feed_urls}
        for future in as_completed(futures):
            url, entries, status = future.result()
            results_map[url] = entries
            if status != "ok":
                feed_issues.append(f"{url} [{status}]")

    total_feeds = len(feed_urls)
    active_feeds = len([u for u in feed_urls if results_map.get(u)])
    total_entries = sum(len(entries) for entries in results_map.values())

    if active_feeds == total_feeds:
        logging.info(
            f"Feed-Scan erfolgreich: {active_feeds}/{total_feeds} Feeds aktiv ({total_entries} Einträge geladen)."
        )
    else:
        logging.warning(
            f"Feed-Scan unvollständig: {active_feeds}/{total_feeds} Feeds aktiv. "
            f"Auffällige Feeds ({len(feed_issues)}): {', '.join(feed_issues)}"
        )

    return [(url, results_map.get(url, [])) for url in feed_urls if results_map.get(url)]


def should_trip_circuit_breaker(consecutive_failures: int, threshold: int = GEMINI_CIRCUIT_BREAKER_THRESHOLD) -> bool:
    """Prüft, ob nach zu vielen aufeinanderfolgenden Fehlern der aktuelle Zyklus vorzeitig pausiert werden soll."""
    return threshold > 0 and consecutive_failures >= threshold


def main():
    global SESSION
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    validate_startup()
    client = create_genai_client()

    # Initialer Heartbeat & Start des Hintergrund-Watchdogs zur Selbstheilung bei Blockaden
    watchdog = ActivityWatchdog(
        max_inactivity_seconds=WATCHDOG_TIMEOUT_SECONDS,
        check_interval_seconds=60.0,
        heartbeat_file=HEARTBEAT_FILE,
        stop_event=STOP_EVENT,
    ).start()

    seed_mode = (
        os.environ.get("INITIAL_SEED_MODE", CONFIG.get("system", {}).get("initial_seed_mode", "eval_recent"))
        .strip()
        .lower()
    )
    seed_max_deals = int(
        os.environ.get("INITIAL_SEED_MAX_DEALS", CONFIG.get("system", {}).get("initial_seed_max_deals", 5))
    )

    is_new_db = False
    first_cycle = True

    while not STOP_EVENT.is_set():
        watchdog.beat(update_file=True)
        conn = init_db()
        c = conn.cursor()
        try:
            if first_cycle:
                c.execute("SELECT COUNT(*) FROM seen_urls")
                is_new_db = c.fetchone()[0] == 0
                if is_new_db:
                    logging.info(
                        f"Datenbank ist neu. Initialer Startmodus: '{seed_mode}' "
                        f"(Max. {seed_max_deals} Deals werden bewertet, Rest wird als gesehen markiert)..."
                    )
                first_cycle = False

            cleanup_db(conn)

            # 1. Paralleler Abruf aller Feeds in einer einzigen Welle (max_workers = len(RSS_FEEDS))
            feeds_entries = fetch_all_feeds(RSS_FEEDS, max_workers=len(RSS_FEEDS))
            watchdog.beat(update_file=True)

            # 2. Round-Robin Interleaving: Reihum 1 Eintrag aus jedem Feed
            interleaved_entries = []
            max_entries = max((len(entries) for _, entries in feeds_entries), default=0)
            for idx in range(max_entries):
                for feed_url, entries in feeds_entries:
                    if idx < len(entries):
                        interleaved_entries.append((entries[idx], feed_url))

            evaluated_seed_deals = 0
            consecutive_eval_failures = 0
            circuit_breaker_tripped = False

            # 3. Durchlaufe die ausgewogenen Einträge
            for entry, feed_url in interleaved_entries:
                if STOP_EVENT.is_set():
                    logging.info("Stopp-Signal erhalten. Breche aktuellen Verarbeitungszyklus ab.")
                    break

                watchdog.beat(update_file=True)

                try:
                    raw_link = entry.get("link", "")
                    if not raw_link:
                        continue
                    clean_link = normalize_url(raw_link)
                    entry_pub_utc = extract_entry_published_utc(entry)
                    if not should_evaluate_url(conn, clean_link, entry_published_utc=entry_pub_utc, feed_url=feed_url):
                        continue

                    title = entry.get("title", "")
                    content_list = entry.get("content")
                    raw_content = (
                        (content_list[0].get("value") if content_list else None) or entry.get("summary") or title
                    )

                    # 1. Deterministischer Pre-Filter (URL-Pfadsegmente, Negativ-Keywords & Positiv-Gate für gemischte Feeds)
                    is_eligible, pre_reason = is_feed_entry_eligible_pre_filter(
                        title, raw_content, feed_url, entry_url=clean_link
                    )
                    if not is_eligible:
                        logging.info(f"Eintrag im Pre-Filter verworfen ({pre_reason}): {title}")
                        upsert_seen_url(conn, clean_link, entry_pub_utc)
                        continue

                    # Airline-Ignorier-Filter (Pre-Filter auf Titelebene)
                    is_ignored_title, matched_air = is_airline_ignored_in_text(title, IGNORED_AIRLINES)
                    if is_ignored_title:
                        logging.info(f"Eintrag vorab ignoriert (Airline im Titel: '{matched_air}'): {title}")
                        upsert_seen_url(conn, clean_link, entry_pub_utc)
                        continue

                    # Initial Seeding Logik:
                    if is_new_db:
                        if seed_mode == "silent":
                            # Stummer Modus: Alle Deals still schlucken
                            upsert_seen_url(conn, clean_link, entry_pub_utc)
                            continue
                        elif seed_mode == "eval_recent":
                            # Bewertet nur die ersten seed_max_deals Deals, restliche alte Einträge überspringen
                            if evaluated_seed_deals >= seed_max_deals:
                                upsert_seen_url(conn, clean_link, entry_pub_utc)
                                continue

                    full_text, sample_booking = get_enriched_deal_context(raw_content, raw_link)
                    watchdog.beat(update_file=True)

                    try:
                        deal = evaluate_with_gemini(client, title, full_text)
                        consecutive_eval_failures = 0
                    except Exception as gemini_err:
                        consecutive_eval_failures += 1
                        watchdog.beat(update_file=True)
                        logging.error(
                            "KI-Auswertung fehlgeschlagen (%d/%d in Folge) für %s: %s",
                            consecutive_eval_failures,
                            GEMINI_CIRCUIT_BREAKER_THRESHOLD,
                            entry.get("link", "unbekannt"),
                            gemini_err,
                        )
                        if should_trip_circuit_breaker(consecutive_eval_failures):
                            circuit_breaker_tripped = True
                            logging.warning(
                                "Circuit-Breaker ausgelöst nach %d fehlgeschlagenen KI-Auswertungen in Folge. "
                                "Pausiere restliche Einträge bis zum nächsten Zyklus in %ds...",
                                consecutive_eval_failures,
                                CHECK_INTERVAL,
                            )
                            break
                        continue

                    watchdog.beat(update_file=True)

                    # 2. Deterministischer Post-Validation Airbag
                    is_safe, safety_veto_reason = validate_deal_safety(deal, title, full_text)
                    if not is_safe:
                        logging.warning(
                            f"Sicherheits-Veto für {format_deal_route_plain(deal)} ({deal.price_eur}€): {safety_veto_reason}"
                        )
                        deal.is_relevant = False
                        deal.deal_category = "Irrelevant"
                        deal.rejection_reason = safety_veto_reason

                    if is_new_db and seed_mode == "eval_recent":
                        evaluated_seed_deals += 1

                    if deal.is_relevant and deal.deal_category != "Irrelevant":
                        # Intelligenter Duplikat- und Preissturz-Check
                        should_send, send_reason, prev_price = should_send_deal(conn, deal, min_price_drop=75)
                        fingerprint = build_deal_fingerprint(deal)

                        if should_send:
                            live_check = None
                            if ENABLE_GF_LIVE_CHECK:
                                watchdog.beat(update_file=True)
                                live_check = run_google_flights_live_check(deal, home=HOME_AIRPORT)
                                watchdog.beat(update_file=True)
                            send_telegram(
                                deal,
                                title,
                                raw_link,
                                sample_booking,
                                prev_price=prev_price,
                                live_check=live_check,
                            )
                            record_sent_deal(conn, deal)
                            if send_reason == "price_drop":
                                logging.info(
                                    f"Deal-Preisupdate gepusht: {fingerprint} [{format_deal_route_plain(deal)}] ({deal.price_eur}€ statt bisher {prev_price}€, {deal.airline_names})"
                                )
                            else:
                                logging.info(
                                    f"Neuer Deal gepusht: {fingerprint} [{format_deal_route_plain(deal)}] ({deal.price_eur}€, {deal.airline_names})"
                                )
                            time.sleep(2)
                        else:
                            logging.info(
                                f"Cross-Feed-Duplikat unterdrückt: {fingerprint} ({deal.price_eur}€, bestehend: {prev_price}€)"
                            )
                    else:
                        reason = (
                            deal.rejection_reason
                            if getattr(deal, "rejection_reason", None)
                            else f"Kategorie: {deal.deal_category}"
                        )
                        logging.info(
                            f"Deal verworfen ({format_deal_route_plain(deal)}, {deal.price_eur}€, {deal.airline_names}): {reason}"
                        )

                    # Transaktionssicher: Erst nach erfolgreicher Verarbeitung URL inkl. RSS-Datum speichern/aktualisieren
                    upsert_seen_url(conn, clean_link, entry_pub_utc)
                except Exception as entry_err:
                    logging.error(f"Fehler bei Eintrag {entry.get('link', 'unbekannt')}: {entry_err}")

            if is_new_db and not circuit_breaker_tripped:
                logging.info("Initialer Start-Zyklus erfolgreich abgeschlossen. Live-Überwachung aktiv.")
                is_new_db = False
        finally:
            c.close()
            close_db(conn)

        # Heartbeat nach jedem Zyklus aktualisieren
        watchdog.beat(update_file=True)

        # Idle TCP-Verbindungen im Session-Pool schließen und frischen Pool für den nächsten Zyklus bereitstellen
        SESSION.close()
        SESSION = create_http_session()

        if STOP_EVENT.is_set():
            break

        logging.info(f"Zyklus abgeschlossen. Nächster Durchlauf in {CHECK_INTERVAL}s...")
        if wait_for_next_cycle(CHECK_INTERVAL, stop_event=STOP_EVENT, watchdog=watchdog):
            logging.info("Bot wurde gestoppt. Fahre geordnet herunter.")
            break

    SESSION.close()


if __name__ == "__main__":
    main()
