import base64
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
from datetime import datetime, timedelta
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import urllib.parse
from typing import Any, Literal, Type

import feedparser
import requests
import requests.adapters
import trafilatura
from fast_flights import FlightQuery, Passengers, create_query
from fast_flights.pb import flights_pb2
from google import genai
from google.genai import errors
from pydantic import BaseModel, Field

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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL_SECONDS", "1800"))
DB_PATH = os.environ.get("DB_PATH")
if not DB_PATH:
    DB_PATH = (
        "/app/data/deals.db" if os.path.exists("/app") else str(Path(__file__).resolve().parent / "data" / "deals.db")
    )

HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat")  # nosec B108

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", Path(__file__).resolve().parent / "config.toml"))

STOP_EVENT = threading.Event()


def handle_shutdown_signal(signum: int, frame: Any):
    logging.info(f"Signal {signum} empfangen. Beende Bot geordnet...")
    STOP_EVENT.set()


def load_config() -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "origin": {
            "home_airport": "HAM",
            "preferred_hubs": ["HAM", "FRA", "BER", "CPH"],
        },
        "destinations": {
            "regions": ["Asien", "Südamerika", "Mittelamerika"],
            "sweet_spots": ["DXB", "AUH", "JED", "AMM", "TLV", "BEY"],
        },
        "pricing": {
            "max_price_home_hubs": 2200,
            "max_price_ex_eu": 1750,
            "max_price_top_cabin_ex_eu": 1900,
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
    str(h).strip().upper() for h in CONFIG["origin"].get("preferred_hubs", ["HAM", "FRA", "BER", "CPH"])
}
IGNORED_AIRLINES: list[str] = [
    str(a).strip() for a in CONFIG.get("airlines", {}).get("ignored_airlines", []) if str(a).strip()
]
MAX_ENTRIES_PER_FEED: int = int(
    os.environ.get("MAX_ENTRIES_PER_FEED", CONFIG.get("system", {}).get("max_entries_per_feed", 25))
)

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

# Harte Negativ-Keywords: Führen IMMER zum Ausschluss (Fragen, Meilenkauf, Hotels, etc.)
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
    "points kaufen",
    "punkte kaufen",
    "meilen kaufen",
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

# Abwärtskompatibilität für Imports aus test_suite.py und run_batch_push.py
NEGATIVE_KEYWORDS = HARD_NEGATIVE_KEYWORDS + WEAK_NEGATIVE_KEYWORDS


def is_feed_entry_eligible_pre_filter(entry_title: str, entry_content: str, feed_url: str = "") -> tuple[bool, str]:
    """
    Deterministischer Pre-Filter vor jedem KI-Aufruf:
    1. Prüfung auf harte Negativ-Keywords im Titel (Fragen, Hotels, Meilenkauf -> sofortiger Ausschluss).
    2. Hat der Titel ein explizites Positiv-Signal ('business class', 'first class', 'qsuite' etc.),
       werden schwache Negativ-Keywords ('economy', 'full-service') im Titel ignoriert (verhindert False Negatives).
    3. Andernfalls führen schwache Negativ-Keywords im Titel zum Ausschluss.
    4. Bei gemischten Feeds (Fly4free, Reisetopia etc.): Zwingende Pflicht zu mindestens einem Positiv-Keyword
       in Titel oder Text.
    """
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


class FlightDealEvaluation(BaseModel):
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
    deal_category: Literal[
        "FTL-Booster (Points + QP)",
        "Star Alliance (Nur Points)",
        "Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)",
        "Irrelevant",
    ] = Field(
        description=(
            "Kategorie des Deals: 'FTL-Booster (Points + QP)' für alle vollintegrierten Miles & More Partner "
            "(LH, LX, OS, SN, EW, 4Y, EN, LO / LOT Polish Airlines, AZ, LG, OU), "
            "'Star Alliance (Nur Points)' für reine Star Alliance Partner ohne QP (TK, SQ, TP, UA etc.), "
            "'Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)', oder 'Irrelevant'."
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
    iata_destination: str = Field(description="3-Letter IATA Code des Hauptziels (z.B. BKK, SIN, GRU)")
    is_open_jaw: bool = Field(description="True, falls Gabelflug (Rückflug endet an anderem Flughafen als iata_origin)")
    iata_return_end: str = Field(
        description="3-Letter IATA Code des Rückflug-Endpunkts (identisch mit iata_origin bei normalem Return)"
    )
    region: Literal["Asien", "Südamerika", "Mittelamerika", "Nahost", "Sonstige"]
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
            "2. Ungewöhnliche Umstiege (z.B. '> 8h Nacht-Layover in DOH' oder 'Flughafenwechsel NRT->HND'). "
            "3. Tarifregeln (z.B. 'Mindestaufenthalt 6 Tage', 'Stopover kostenlos'). "
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


def normalize_and_verify_deal_mm_points(deal: FlightDealEvaluation) -> FlightDealEvaluation:
    """
    Normalisiert und verifiziert die Miles & More Punkte sowie Airline-Codes:
    1. Normalisiert 'LOT' (3-Letter Code) zum 2-Letter IATA Code 'LO'.
    2. Erkennt LOT Polish Airlines zuverlässig anhand des Namens.
    3. Stellt sicher, dass vollintegrierte Partner (LH, LX, OS, SN, EW, 4Y, EN, LO, AZ, LG, OU)
       korrekt als 'FTL-Booster (Points + QP)' eingestuft werden und Points == QP gilt.
       (LOT Polish Airlines ist vollintegrierter Partner und vergibt IMMER volle QP).
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


def validate_deal_safety(deal: FlightDealEvaluation, title: str, full_text: str) -> tuple[bool, str]:
    """
    Deterministische Sicherheitsprüfungen (Airbag) nach der Gemini-Evaluierung:
    1. Kabinenklassen-Beweis: Wurde Business/First explizit nachgewiesen?
    2. Langstrecken-Preisplausibilität: Interkontinental Return < 750 € ist ohne expliziten Error-Fare-Nachweis unplausibel.
    3. Abflughafen-Verifikation: Kommt iata_origin im Text oder Titel vor?
    """
    if deal is None or not deal.is_relevant or deal.deal_category == "Irrelevant":
        return True, ""

    # 1. Kabinenklassen-Beweis prüfen
    if deal.detected_cabin_class not in ("Business Class", "First Class"):
        return False, f"Reiseklasse nicht Business/First ({deal.detected_cabin_class})"

    quote = (deal.cabin_class_quote or "").strip()
    if not quote or quote.upper() == "KEIN NACHWEIS":
        return False, "Kein Textnachweis für Business/First Class vorhanden"

    # 2. Langstrecken-Preisplausibilität
    # Flüge nach Asien, Südamerika, Mittelamerika kosten in Business regulär >= 1.000 €.
    # Preise < 750 € Return sind zu 99% Economy-Tarife, außer es ist ausdrücklich ein Error Fare.
    if deal.region in ("Asien", "Südamerika", "Mittelamerika") and deal.price_eur < 750:
        combined_lower = f"{title} {full_text}".lower()
        if not any(ef in combined_lower for ef in ("error fare", "error-fare", "preisfehler", "errorfare")):
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
        }
        aliases = airport_aliases.get(origin, [])
        if not any(alias in combined_upper for alias in aliases):
            return False, f"Abflughafen {origin} kommt im Deal-Text nicht vor (Origin-Hijacking abgefangen)"

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
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS seen_urls (url_clean TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS sent_deals (deal_fingerprint TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )

    # Automatische sanfte Schema-Migration für bestehende Datenbanken
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


def cleanup_db(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute("DELETE FROM seen_urls WHERE created_at < datetime('now', '-90 days')")
    c.execute("DELETE FROM sent_deals WHERE COALESCE(updated_at, created_at) < datetime('now', '-7 days')")
    conn.commit()


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
    c = conn.cursor()
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
    c = conn.cursor()
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


def normalize_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url.strip())
    query_params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    # Behalte Foren-Parameter (z.B. ?t=123), entferne nur Tracking-Parameter
    clean_params = [
        (k, v) for k, v in query_params if not k.lower().startswith(("utm_", "fbclid", "gclid", "mc_", "ref"))
    ]
    clean_query = urllib.parse.urlencode(clean_params)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, clean_query, ""))


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


def build_gf_main_url(deal: FlightDealEvaluation) -> str:
    try:
        info = flights_pb2.Info()
        return_dest = deal.iata_return_end if (deal.is_open_jaw and deal.iata_return_end) else deal.iata_origin
        is_multi = deal.is_open_jaw and return_dest != deal.iata_origin

        airline_code = deal.primary_airline_iata.strip().upper() if deal.primary_airline_iata else ""
        if not airline_code and deal.airline_names:
            m = re.search(r"\b([A-Z0-9]{2})\b", deal.airline_names)
            if m:
                airline_code = m.group(1).upper()

        fd1 = info.data.add()
        fd1.date = deal.sample_outbound_date
        fd1.from_airport.airport = deal.iata_origin
        fd1.to_airport.airport = deal.iata_destination
        if airline_code and len(airline_code) == 2:
            fd1.airlines.append(airline_code)

        fd2 = info.data.add()
        fd2.date = deal.sample_return_date
        fd2.from_airport.airport = deal.iata_destination
        fd2.to_airport.airport = return_dest
        if airline_code and len(airline_code) == 2:
            fd2.airlines.append(airline_code)

        info.seat = 3  # Business
        info.trip = 3 if is_multi else 1  # 3 = Multi-city, 1 = Round-trip
        info.passengers.append(1)

        tfs = base64.b64encode(info.SerializeToString()).decode("utf-8")
        return f"https://www.google.com/travel/flights/search?tfs={tfs}&hl=de&curr=EUR"
    except Exception as e:
        logging.warning(f"Fallback bei Google Flights Hauptflug-Link: {e}")
        airline_str = f" with {deal.primary_airline_iata}" if deal.primary_airline_iata else ""
        if deal.is_open_jaw and deal.iata_return_end and deal.iata_return_end != deal.iata_origin:
            query = f"Multi-city flights from {deal.iata_origin} to {deal.iata_destination} on {deal.sample_outbound_date} and from {deal.iata_destination} to {deal.iata_return_end} on {deal.sample_return_date}{airline_str} business class"
        else:
            query = f"Round trip flights from {deal.iata_origin} to {deal.iata_destination} on {deal.sample_outbound_date} returning {deal.sample_return_date}{airline_str} business class"
        return f"https://www.google.com/travel/flights?hl=en&gl=de&curr=EUR&q={urllib.parse.quote_plus(query)}"


def build_positioning_url(origin: str, out_date: str, ret_date: str, home: str | None = None) -> str:
    home_hub = home or HOME_AIRPORT
    try:
        pos_out = (datetime.strptime(out_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        pos_ret = (datetime.strptime(ret_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        q = create_query(
            flights=[
                FlightQuery(date=pos_out, from_airport=home_hub, to_airport=origin),
                FlightQuery(date=pos_ret, from_airport=origin, to_airport=home_hub),
            ],
            seat="economy",
            trip="round-trip",
            passengers=Passengers(adults=1),
            currency="EUR",
            language="de",
        )
        return q.url()
    except Exception as e:
        logging.warning(f"Fallback bei Google Flights Positionierungs-Link: {e}")
        query = f"Round trip flights from {home_hub} to {origin} on {out_date} returning {ret_date} economy"
        return f"https://www.google.com/travel/flights?hl=en&gl=de&curr=EUR&q={urllib.parse.quote_plus(query)}"


def generate_with_gemini_fallback(
    client: genai.Client,
    prompt: str,
    response_schema: Type[BaseModel] | None = None,
    preferred_model: str = "gemini-3.8-flash",
    enable_thinking: bool = False,
    max_transient_retries: int = 4,
) -> Any:
    """Führt Content-Generierung mit automatischer Modell-Kaskade und intelligenter Retry-Logik aus."""
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

        # Konfiguration vorbereiten
        gen_config: dict[str, Any] = {
            "temperature": 0.1,
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

            # Fall C: Transiente Fehler (429 Quota, 503 Overload, Timeout) -> Wait & Shift
            is_transient = (isinstance(exc, errors.APIError) and exc.code in (408, 429, 500, 502, 503, 504)) or any(
                code in exc_str
                for code in (
                    "503",
                    "429",
                    "UNAVAILABLE",
                    "ResourceExhausted",
                    "RESOURCE_EXHAUSTED",
                    "timeout",
                    "Timeout",
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


def evaluate_with_gemini(client: genai.Client, title: str, clean_text: str) -> FlightDealEvaluation:
    hubs_str = ", ".join(sorted(PREFERRED_HUBS))
    dest_regions = CONFIG["destinations"].get("regions", ["Asien", "Südamerika", "Mittelamerika"])
    regions_str = ", ".join(dest_regions)
    sweet_spots = CONFIG["destinations"].get("sweet_spots", ["DXB", "AUH", "JED", "AMM", "TLV", "BEY"])
    sweet_spots_str = ", ".join(sweet_spots)
    price_hubs = CONFIG["pricing"].get("max_price_home_hubs", 2200)
    price_ideal = max(0, price_hubs - 200)
    price_ex_eu = CONFIG["pricing"].get("max_price_ex_eu", 1750)
    price_top_cabin = CONFIG["pricing"].get("max_price_top_cabin_ex_eu", 1900)
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
2. Zielregion: {regions_str} (oder Nahost-Sweet-Spots für Miles & More QP: {sweet_spots_str}).
3. STARTFLUGHAFEN & PRIORISIERUNG (KEIN ORIGIN-HIJACKING):
   - 'iata_origin' MUSS zwingend ein Flughafen sein, der im Artikel oder Buchungslink EXPLIZIT als buchbarer Abflugort für diesen Tarif genannt ist!
   - Wenn ein Deal NUR ab Warschau (WAW) oder Oslo (OSL) angeboten wird, MUSS iata_origin 'WAW' bzw. 'OSL' sein.
   - Es ist STRENGSTENS VERBOTEN, einen bevorzugten Hub ({hubs_str}) als iata_origin einzusetzen, wenn dieser Flughafen nicht im Artikel als buchbarer Abflugort aufgeführt ist!
   - Nur wenn der Deal MEHRERE Abflughäfen anbietet (z.B. 'Ab Frankfurt, München und Amsterdam'), wähle den bevorzugten Hub ({hubs_str}) als iata_origin. Nenne weitere Abflughäfen in 'routing_advice'.
4. Preis (Fremdwährungen wie USD, CHF, GBP, PLN, SEK, DKK zwingend in EUR umrechnen!):
   - Ab {hubs_str}: max. {price_hubs} EUR Return (Ideal < {price_ideal} EUR).
   - Ab Ex-EU (z.B. WAW, OSL, ARN, DUB, MXP, AMS, BUD): max. {price_ex_eu} EUR Return (bis {price_top_cabin} EUR nur bei Top-Kabinen wie Etihad A350/787 oder Qatar Qsuite).
5. Miles & More Punkte-Logik (Stand Oktober 2026 | FTL-Statusziel: 650 Points, davon mind. 325 QP):
   - Vollintegriert (Points UND QP): LH (Lufthansa), VL (City Airlines), LX (SWISS), OS (Austrian Airlines), SN (Brussels Airlines), EW (Eurowings), 4Y (Discover Airlines), EN (Air Dolomiti), LO (LOT Polish Airlines), AZ (ITA Airways), LG (Luxair), OU (Croatia Airlines).
     -> KRITISCH LOT POLISH AIRLINES (LO): LOT ist ein VOLLINTEGRIERTER Miles & More Partner!
     -> Flüge mit LOT Polish Airlines bringen IMMER Points UND Qualifying Points (QP) im Verhältnis 1:1! NIEMALS 0 QP für LOT vergeben!
     -> deal_category für LOT MUSS 'FTL-Booster (Points + QP)' sein (NIEMALS 'Star Alliance (Nur Points)')!
     -> Kontinental Business = 40 Points + 40 QP pro Segment. Interkontinental Business = 200 Points + 200 QP pro Segment (First = 400).
     -> Beispiel LOT Return ab Deutschland/Europa via Warschau (z.B. FRA-WAW-NRT-WAW-FRA): 2x Zubringer (2x40) + 2x Langstrecke (2x200) = 480 Points und 480 QP!
     -> Beispiel LOT Return Direktflug ab Warschau (WAW-NRT-WAW): 2x Langstrecke (2x200) = 400 Points und 400 QP!
   - KRITISCH Nordafrika: Ägypten (CAI/HRG), Marokko (CMN/RAK), Tunesien (TUN), Algerien (ALG) zählen als KONTINENTAL (nur 40 Points/QP pro Segment, niemals 200!).
   - KRITISCH Edelweiss (WK): Gibt 0 Points/0 QP unter WK-Flugnummer! Nur unter LX-Codeshare (LX8000-8499) gibt es Punkte/QP.
   - KRITISCH Asiana (OZ): Seit 15.10.2026 ausgeschieden -> 0 Points / 0 QP!
   - Andere Star Alliance (SQ, TP, TK, BR, NH, UA, AC, CM, AV, TG, ET, CA, MS, AI, ZH): Bringen NUR Points (40 kontinental / 200 pro Langstrecke), aber 0 QP!
   - Fremd-Allianzen (EY, QR, AY, WY, GF, BA, AF, KL, CX, JL): 0 Points / 0 QP, aber als 'Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)' relevant wenn Preis (< {price_ex_eu}-{price_top_cabin} EUR) & Hardware (z.B. Qsuite, Etihad Business Studio, Oman Air Apex Suite) stimmen.
6. Besonderheiten & Tarifdetails ('routing_advice'):
   - Nenne HIER nur echte operative Zusatzfakten aus dem Text: weitere buchbare Abflughäfen, außergewöhnlich lange Layover (> 8h), Flughafenwechsel oder Tarifregeln (Stopover/Mindestaufenthalt).
   - WICHTIG & STRENG VERBOTEN: Wiederhole NIEMALS Meilen, 0 Points/QP, Allianzen, Positionierungshinweise (24h-Puffer) oder generische Preis-Checks / Buchungstipps (wie 'OTA-Bestpreis vs. Direktbuchung', Momondo/Kayak/Skyscanner, Zahlungsaufschläge, Phantom-Verfügbarkeit)!
   - Wenn keine solchen echten Besonderheiten im Artikel stehen: Setze routing_advice = "".
7. Reisedaten & Airline-Code:
   - WICHTIG: Nutze ZWINGEND die im Text oder Buchungslink genannten Beispieldaten (Hinflug / Rückflug), da Airlines oft nur an bestimmten Wochentagen fliegen (z.B. nur Mo/Mi/Sa)!
   - Falls mehrere Termine im Link/Text vorkommen, nimm das erste konkrete Beispieldatum im Format YYYY-MM-DD.
   - Nur wenn absolut kein konkretes Datum im Artikel oder Link genannt ist, wähle ein plausibles Datum im Reisezeitraum.
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
    deal = normalize_and_verify_deal_mm_points(deal)
    is_ignored, ignored_air = is_deal_ignored_airline(deal, ignored_airlines)
    if is_ignored:
        deal.is_relevant = False
        deal.deal_category = "Irrelevant"
        if not deal.rejection_reason:
            deal.rejection_reason = f"Airline ignoriert ({ignored_air})"

    return deal


def build_telegram_message(
    deal: FlightDealEvaluation, title: str, url: str, sample_booking: str = "", prev_price: int = 0
) -> str:
    gf_main = build_gf_main_url(deal)
    pos_line = ""
    if deal.iata_origin not in PREFERRED_HUBS:
        gf_pos = build_positioning_url(
            deal.iata_origin, deal.sample_outbound_date, deal.sample_return_date, home=HOME_AIRPORT
        )
        pos_line = f'\n🔹 <a href="{gf_pos}">Positionierung: {html.escape(HOME_AIRPORT)}–{html.escape(deal.iata_origin)} (+/- 24h Stopover)</a>'

    booking_line = ""
    if sample_booking and sample_booking.startswith("http"):
        booking_line = f'\n🔹 <a href="{html.escape(sample_booking, quote=True)}">Geprüftes Buchungsbeispiel (Skyscanner / OTA)</a>'

    route_str = f"<code>{html.escape(deal.iata_origin)}</code> ➔ <code>{html.escape(deal.iata_destination)}</code>"
    if deal.is_open_jaw and deal.iata_return_end != deal.iata_origin:
        route_str += f" ➔ <code>{html.escape(deal.iata_return_end)}</code> (Gabelflug)"

    ftl_info = ""
    if deal.mm_points > 0:
        pct_p = round((deal.mm_points / 650) * 100)
        pct_qp = round((deal.mm_qualifying_points / 325) * 100)
        ftl_info = f" (FTL-Ziel: ~{pct_p}% Points / ~{pct_qp}% QP)"

    airline_label = f" ({html.escape(deal.primary_airline_iata)})" if deal.primary_airline_iata else ""

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

    # Header & Preisangabe (bei Preissenkungen hervorheben)
    if prev_price > 0 and prev_price > deal.price_eur:
        header_line = f"📉 <b>PREIS-UPDATE: {html.escape(deal.deal_category)}</b>"
        diff = prev_price - deal.price_eur
        price_line = f"• <b>Preis:</b> ~{deal.price_eur} € 📉 <i>(-{diff} € gegenüber bisher ~{prev_price} €!)</i> (Buchungsklasse: <code>{html.escape(deal.booking_class)}</code>)"
    else:
        header_line = f"✈️ <b>{html.escape(deal.deal_category)}</b>"
        price_line = (
            f"• <b>Preis:</b> ~{deal.price_eur} € (Buchungsklasse: <code>{html.escape(deal.booking_class)}</code>)"
        )

    msg = (
        f"{header_line}\n\n"
        f"<b>{html.escape(title)}</b>\n\n"
        f"• <b>Route:</b> {route_str} ({html.escape(deal.airline_names)})\n"
        f"{price_line}\n"
        f"• <b>Miles & More:</b> {deal.mm_points} Points | <b>{deal.mm_qualifying_points} QP</b>{ftl_info}\n"
        f"• <b>Bordprodukt:</b> {html.escape(deal.cabin_assessment)}\n"
        f"{advice_line}\n"
        f"🔗 <b>Direktlinks:</b>\n"
        f'🔹 <a href="{html.escape(url, quote=True)}">Deal-Quelle & Tarifdetails</a>\n'
        f'🔹 <a href="{gf_main}">Google Flights: {html.escape(deal.iata_origin)}–{html.escape(deal.iata_destination)}{airline_label} (Business)</a>'
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
):
    msg = build_telegram_message(deal, title, url, sample_booking, prev_price=prev_price)
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


def main():
    global SESSION
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    validate_startup()
    conn = init_db()
    client = genai.Client(api_key=GEMINI_API_KEY)

    # Initialer Heartbeat direkt beim Booten, damit Container-Healthcheck sofort grün ist
    Path(HEARTBEAT_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(HEARTBEAT_FILE).touch()

    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM seen_urls")
    is_new_db = c.fetchone()[0] == 0

    seed_mode = (
        os.environ.get("INITIAL_SEED_MODE", CONFIG.get("system", {}).get("initial_seed_mode", "eval_recent"))
        .strip()
        .lower()
    )
    seed_max_deals = int(
        os.environ.get("INITIAL_SEED_MAX_DEALS", CONFIG.get("system", {}).get("initial_seed_max_deals", 5))
    )

    if is_new_db:
        logging.info(
            f"Datenbank ist neu. Initialer Startmodus: '{seed_mode}' "
            f"(Max. {seed_max_deals} Deals werden bewertet, Rest wird als gesehen markiert)..."
        )

    while not STOP_EVENT.is_set():
        cleanup_db(conn)

        # 1. Paralleler Abruf aller Feeds in einer einzigen Welle (max_workers = len(RSS_FEEDS))
        feeds_entries = fetch_all_feeds(RSS_FEEDS, max_workers=len(RSS_FEEDS))

        # 2. Round-Robin Interleaving: Reihum 1 Eintrag aus jedem Feed
        interleaved_entries = []
        max_entries = max((len(entries) for _, entries in feeds_entries), default=0)
        for idx in range(max_entries):
            for feed_url, entries in feeds_entries:
                if idx < len(entries):
                    interleaved_entries.append((entries[idx], feed_url))

        evaluated_seed_deals = 0

        # 3. Durchlaufe die ausgewogenen Einträge
        for entry, feed_url in interleaved_entries:
            if STOP_EVENT.is_set():
                logging.info("Stopp-Signal erhalten. Breche aktuellen Verarbeitungszyklus ab.")
                break

            try:
                raw_link = entry.get("link", "")
                if not raw_link:
                    continue
                clean_link = normalize_url(raw_link)
                c.execute("SELECT 1 FROM seen_urls WHERE url_clean = ?", (clean_link,))
                if c.fetchone():
                    continue

                title = entry.get("title", "")
                content_list = entry.get("content")
                raw_content = (content_list[0].get("value") if content_list else None) or entry.get("summary") or title

                # 1. Deterministischer Pre-Filter (Negativ-Keywords & Positiv-Gate für gemischte Feeds)
                is_eligible, pre_reason = is_feed_entry_eligible_pre_filter(title, raw_content, feed_url)
                if not is_eligible:
                    logging.info(f"Eintrag im Pre-Filter verworfen ({pre_reason}): {title}")
                    c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                    conn.commit()
                    continue

                # Airline-Ignorier-Filter (Pre-Filter auf Titelebene)
                is_ignored_title, matched_air = is_airline_ignored_in_text(title, IGNORED_AIRLINES)
                if is_ignored_title:
                    logging.info(f"Eintrag vorab ignoriert (Airline im Titel: '{matched_air}'): {title}")
                    c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                    conn.commit()
                    continue

                # Initial Seeding Logik:
                if is_new_db:
                    if seed_mode == "silent":
                        # Stummer Modus: Alle Deals still schlucken
                        c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                        conn.commit()
                        continue
                    elif seed_mode == "eval_recent":
                        # Bewertet nur die ersten seed_max_deals Deals, restliche alte Einträge überspringen
                        if evaluated_seed_deals >= seed_max_deals:
                            c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                            conn.commit()
                            continue

                full_text, sample_booking = get_enriched_deal_context(raw_content, raw_link)
                deal = evaluate_with_gemini(client, title, full_text)

                # 2. Deterministischer Post-Validation Airbag
                is_safe, safety_veto_reason = validate_deal_safety(deal, title, full_text)
                if not is_safe:
                    logging.warning(
                        f"Sicherheits-Veto für {deal.iata_origin}->{deal.iata_destination} ({deal.price_eur}€): {safety_veto_reason}"
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
                        send_telegram(deal, title, raw_link, sample_booking, prev_price=prev_price)
                        record_sent_deal(conn, deal)
                        if send_reason == "price_drop":
                            logging.info(
                                f"Deal-Preisupdate gepusht: {fingerprint} ({deal.price_eur}€ statt bisher {prev_price}€, {deal.airline_names})"
                            )
                        else:
                            logging.info(f"Neuer Deal gepusht: {fingerprint} ({deal.price_eur}€, {deal.airline_names})")
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
                        f"Deal verworfen ({deal.iata_origin}->{deal.iata_destination}, {deal.price_eur}€, {deal.airline_names}): {reason}"
                    )

                # Transaktionssicher: Erst nach erfolgreicher Verarbeitung URL als gesehen speichern
                c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                conn.commit()
            except Exception as entry_err:
                logging.error(f"Fehler bei Eintrag {entry.get('link', 'unbekannt')}: {entry_err}")

        if is_new_db:
            logging.info("Initialer Start-Zyklus erfolgreich abgeschlossen. Live-Überwachung aktiv.")
            is_new_db = False

        # Heartbeat nach jedem Zyklus aktualisieren
        Path(HEARTBEAT_FILE).touch()

        # Idle TCP-Verbindungen im Session-Pool schließen und frischen Pool für den nächsten Zyklus bereitstellen
        SESSION.close()
        SESSION = create_http_session()

        if STOP_EVENT.is_set():
            break

        logging.info(f"Zyklus abgeschlossen. Nächster Durchlauf in {CHECK_INTERVAL}s...")
        if STOP_EVENT.wait(timeout=CHECK_INTERVAL):
            logging.info("Bot wurde gestoppt. Fahre geordnet herunter.")
            break

    conn.close()
    SESSION.close()


if __name__ == "__main__":
    main()
