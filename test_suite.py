import os
import sys
import tempfile
from datetime import datetime

# .env manuell laden
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                os.environ[k] = v

import feedparser
import requests
from google import genai

from bot import (
    IGNORED_AIRLINES,
    RSS_FEEDS,
    FlightDealEvaluation,
    build_gf_main_url,
    build_positioning_url,
    build_telegram_message,
    clean_html,
    get_enriched_deal_context,
    init_db,
    is_airline_ignored_in_text,
    is_deal_ignored_airline,
    is_feed_entry_eligible_pre_filter,
    normalize_url,
    record_sent_deal,
    should_send_deal,
    validate_deal_safety,
)

TEST_RESULTS = []


def record_result(name: str, passed: bool, details: str = ""):
    status = "✅ PASS" if passed else "❌ FAIL"
    TEST_RESULTS.append((name, passed, details))
    print(f"{status} | {name}" + (f": {details}" if details else ""))


def test_1_env_variables():
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or os.environ.get("TELEGRAM_TOKEN", "").strip()
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    passed = bool(gemini_key and tg_token and tg_chat)
    details = f"GEMINI_KEY={'SET' if gemini_key else 'MISSING'}, TG_TOKEN={'SET' if tg_token else 'MISSING'}, TG_CHAT={'SET' if tg_chat else 'MISSING'}"
    record_result("Test 1: Umgebungsvariablen geladen", passed, details)
    return passed


def test_2_url_normalization_and_html_cleaning():
    url_with_tracking = "https://travel-dealz.de/deal/test-deal/?utm_source=rss&utm_medium=feed&ref=newsletter"
    clean = normalize_url(url_with_tracking)
    passed_url = clean == "https://travel-dealz.de/deal/test-deal/"

    forum_url = "https://www.vielfliegertreff.de/forum/threads/test-deal.12345/?t=12345&utm_campaign=track"
    clean_forum = normalize_url(forum_url)
    passed_forum = clean_forum == "https://www.vielfliegertreff.de/forum/threads/test-deal.12345/?t=12345"

    raw_html = "<p>Super <b>Business Class</b> Flug! <script>alert(1)</script>&amp; mehr Details.</p>"
    cleaned = clean_html(raw_html)
    passed_html = cleaned == "Super Business Class Flug! & mehr Details."

    all_passed = passed_url and passed_forum and passed_html
    record_result(
        "Test 2: URL-Normalisierung & HTML-Bereinigung",
        all_passed,
        f"URL={passed_url}, Forum-Param={passed_forum}, HTML={passed_html}",
    )
    return all_passed


def test_3_database_and_deduplication():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        test_db = tmp.name

    try:
        conn = init_db(test_db)
        c = conn.cursor()

        # 1. Schema-Prüfung (Migration von price_eur, airline_code, updated_at)
        c.execute("PRAGMA table_info(sent_deals)")
        cols = {col[1] for col in c.fetchall()}
        schema_ok = {"price_eur", "airline_code", "updated_at", "deal_fingerprint"}.issubset(cols)

        # 2. Test Stufe 1: seen_urls
        test_url = "https://travel-dealz.de/deal/biz-bangkok/"
        c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (test_url,))
        conn.commit()
        c.execute("SELECT 1 FROM seen_urls WHERE url_clean = ?", (test_url,))
        seen_found = c.fetchone() is not None

        # 3. Test Stufe 2: Intelligenter Airline- & Preissturz-Check
        deal_rj = FlightDealEvaluation(
            detected_cabin_class="Business Class",
            cabin_class_quote="Fliege mit Royal Jordanian nach Bangkok",
            is_relevant=True,
            deal_category="Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)",
            airline_names="Royal Jordanian (RJ)",
            primary_airline_iata="RJ",
            iata_origin="FRA",
            iata_destination="BKK",
            is_open_jaw=False,
            iata_return_end="FRA",
            region="Asien",
            sample_outbound_date="2026-11-15",
            sample_return_date="2026-11-28",
            price_eur=1697,
            booking_class="I",
            cabin_assessment="Boeing 787 Lie-Flat",
            mm_points=0,
            mm_qualifying_points=0,
            routing_advice="",
            rejection_reason="",
        )

        # A) Neuer Deal -> Muss gesendet werden
        send_1, reason_1, prev_1 = should_send_deal(conn, deal_rj)
        record_sent_deal(conn, deal_rj)

        # B) Cross-Feed-Duplikat (gleicher oder teurerer Preis) -> Muss unterdrückt werden
        deal_rj_dup = deal_rj.model_copy(update={"price_eur": 1720})
        send_dup, reason_dup, _ = should_send_deal(conn, deal_rj_dup)

        # C) Gleiche Strecke, aber ANDERE Airline (z.B. SWISS LX) -> Darf NICHT blockiert werden!
        deal_swiss = deal_rj.model_copy(
            update={"primary_airline_iata": "LX", "airline_names": "SWISS (LX)", "price_eur": 1740}
        )
        send_swiss, reason_swiss, _ = should_send_deal(conn, deal_swiss)
        record_sent_deal(conn, deal_swiss)

        # D) Echter Preissturz bei gleicher Airline (> 75€ günstiger) -> Muss als Preisupdate durchgelassen werden!
        deal_rj_cheap = deal_rj.model_copy(update={"price_eur": 1490})
        send_drop, reason_drop, prev_drop = should_send_deal(conn, deal_rj_cheap)

        conn.close()

        passed = (
            schema_ok
            and seen_found
            and send_1
            and reason_1 == "new_deal"
            and (not send_dup)
            and reason_dup == "duplicate"
            and send_swiss
            and reason_swiss == "new_deal"
            and send_drop
            and reason_drop == "price_drop"
            and prev_drop == 1697
        )
        details = (
            f"Schema={schema_ok}, SeenUrls={seen_found}, NewDeal={send_1}, "
            f"DupBlocked={not send_dup}, MultiAirline={send_swiss}, PriceDropAllowed={send_drop}"
        )
        record_result(
            "Test 3: SQLite WAL & Intelligente Deduplizierung (seen_urls & price-aware sent_deals)", passed, details
        )
        return passed
    finally:
        if os.path.exists(test_db):
            os.remove(test_db)


def test_4_google_flights_url_builders():
    deal_rt = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Fliege in der SWISS Business Class nach Bangkok",
        is_relevant=True,
        deal_category="FTL-Booster (Points + QP)",
        airline_names="SWISS (LX)",
        primary_airline_iata="LX",
        iata_origin="FRA",
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2026-11-15",
        sample_return_date="2026-11-28",
        price_eur=1650,
        booking_class="P",
        mm_points=480,
        mm_qualifying_points=480,
        cabin_assessment="Lie-Flat B777",
        routing_advice="Layover in ZRH ca. 3h",
        rejection_reason="",
    )
    gf_url = build_gf_main_url(deal_rt)
    passed_rt = "google.com/travel/flights" in gf_url and ("tfs=" in gf_url or "FRA" in gf_url)

    # Open Jaw
    deal_oj = deal_rt.model_copy(update={"is_open_jaw": True, "iata_return_end": "MUC"})
    gf_oj_url = build_gf_main_url(deal_oj)
    passed_oj = "google.com/travel/flights" in gf_oj_url and ("tfs=" in gf_oj_url or "MUC" in gf_oj_url)

    # Positionierung
    pos_url = build_positioning_url("WAW", "2026-11-15", "2026-11-28")
    passed_pos = "google.com/travel/flights" in pos_url and ("tfs=" in pos_url or "WAW" in pos_url)

    all_passed = passed_rt and passed_oj and passed_pos
    record_result("Test 4: Google Flights URL-Generierung (Return, Open-Jaw, Positionierung)", all_passed)
    return all_passed


def test_5_telegram_api_connectivity():
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip() or os.environ.get("TELEGRAM_TOKEN", "").strip()
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not tg_token or not tg_chat:
        record_result("Test 5: Telegram Bot API", False, "Token oder Chat-ID fehlt")
        return False

    try:
        # 1. getMe
        r = requests.get(f"https://api.telegram.org/bot{tg_token}/getMe", timeout=10)
        if not r.ok:
            record_result("Test 5: Telegram Bot API", False, f"getMe HTTP {r.status_code}: {r.text}")
            return False
        bot_info = r.json().get("result", {})
        bot_username = bot_info.get("username", "Unknown")

        # 2. Testnachricht senden
        test_msg = (
            "🤖 <b>[flight-deal-bot Testlauf]</b>\n"
            "Systemtest erfolgreich gestartet.\n"
            f"• Zeit: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            "• Modus: Lokaler Pre-Deployment Testlauf v3.0"
        )
        send_r = requests.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            json={"chat_id": tg_chat, "text": test_msg, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        if not send_r.ok:
            record_result("Test 5: Telegram Bot API", False, f"sendMessage HTTP {send_r.status_code}: {send_r.text}")
            return False

        record_result(
            "Test 5: Telegram Bot API (getMe & Test-Push)",
            True,
            f"Bot: @{bot_username}, Msg-ID: {send_r.json().get('result', {}).get('message_id')}",
        )
        return True
    except Exception as e:
        record_result("Test 5: Telegram Bot API", False, str(e))
        return False


def test_6_gemini_api_and_pydantic_schema():
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        record_result("Test 6: Gemini GenAI SDK Evaluation", False, "GEMINI_API_KEY fehlt")
        return False

    sample_title = "SWISS Business Class ab Frankfurt nach Bangkok ab 1.620€ (Return)"
    sample_text = (
        "SWISS bietet aktuell sehr attraktive Flüge in der Business Class von Frankfurt (FRA) über Zürich (ZRH) nach Bangkok (BKK) "
        "ab 1.620 Euro an. Buchungsklasse ist P. Reisezeitraum ist von November 2026 bis März 2027. "
        "Auf der Langstrecke kommt die Boeing 777-300ER mit Lie-Flat Sitzen zum Einsatz. "
        "Miles & More Punkte: 40 Points + QP Zubringer, 200 Points + QP Langstrecke je Richtung."
    )

    prompt = f"""
Analysiere diesen Flug-Deal für einen Reisenden mit Basis Hamburg (Bevorzugte Hubs: HAM, FRA, BER, CPH). Aktuelles Datum: {datetime.now().strftime("%Y-%m-%d")}.

STRIKTE REGELN FÜR 'is_relevant = True':
1. Reiseklasse: Nur Business Class oder First Class.
2. Zielregion: Asien, Südamerika, Mittelamerika (oder Nahost-Sweet-Spots).
3. Multi-Airport-Priorisierung: Prio auf HAM, FRA, BER, CPH.
4. Preis: Ab FRA max. 2.200 EUR Return.
5. Miles & More: Vollintegriert (LX) -> 40 P + 40 QP kontinental, 200 P + 200 QP interkontinental je Segment. Gesamt Return = (40+200)*2 = 480 Points und 480 QP.

Titel: {sample_title}
Inhalt: {sample_text}
"""
    try:
        from bot import evaluate_with_gemini

        client = genai.Client(api_key=gemini_key)
        deal = evaluate_with_gemini(client, sample_title, sample_text)
        if deal is None:
            record_result("Test 6: Gemini GenAI SDK Evaluation", False, "response.parsed ist None")
            return False
        passed = (
            deal.is_relevant is True
            and deal.deal_category == "FTL-Booster (Points + QP)"
            and deal.iata_origin == "FRA"
            and deal.iata_destination == "BKK"
            and deal.price_eur <= 1700
            and deal.mm_points >= 400
            and deal.mm_qualifying_points >= 400
        )
        details = (
            f"Rel={deal.is_relevant}, Cat={deal.deal_category}, Route={deal.iata_origin}->{deal.iata_destination}, "
            f"Preis={deal.price_eur}€, P={deal.mm_points}/QP={deal.mm_qualifying_points}"
        )
        record_result("Test 6: Gemini GenAI SDK Evaluation (gemini-2.5-flash & Schema)", passed, details)
        return passed
    except Exception as e:
        record_result("Test 6: Gemini GenAI SDK Evaluation", False, str(e))
        return False


def test_7_rss_feeds_availability():
    from bot import fetch_feed_content

    accessible = 0
    total = len(RSS_FEEDS)

    for feed_url in RSS_FEEDS:
        try:
            content = fetch_feed_content(feed_url)
            if content:
                feed = feedparser.parse(content)
                if len(feed.entries) > 0:
                    accessible += 1
                else:
                    print(f"  [Feed Warning] Keine Einträge: {feed_url}")
            else:
                print(f"  [Feed Notice] Blockiert oder leer: {feed_url}")
        except Exception as e:
            print(f"  [Feed Timeout/Exception]: {feed_url} ({e})")

    passed = accessible >= 2  # Mindestens 2 Feeds müssen aktiv Daten liefern
    record_result("Test 7: RSS-Feeds Erreichbarkeit", passed, f"{accessible}/{total} Feeds liefern aktiv Deals")
    return passed


def test_8_trafilatura_enrichment():
    short_teaser = "<p>Air China offers business class fares from Brussels to Japan.</p>"
    sample_url = "https://premium-flights.com/business-class-brussles-to-japan/"

    enriched_text, booking_link = get_enriched_deal_context(short_teaser, sample_url)

    # Text muss wesentlich länger als der Teaser sein und Buchungslink sollte gefunden werden
    passed = len(enriched_text) > 1000 and (
        "fave.co" in booking_link or "fave.co" in enriched_text or "Air China" in enriched_text
    )
    details = f"Länge={len(enriched_text)} Zeichen, Booking-Link={'Gefunden (' + booking_link[:30] + '...)' if booking_link else 'Nicht gefunden'}"
    record_result("Test 8: Trafilatura-Volltext-Anreicherung & Link-Erkennung", passed, details)
    return passed


def test_9_airline_ignore_filter():
    # 1. Prüfe ob konfigurierte Airlines geladen wurden
    has_azal = any("AZAL" in a.upper() or "AZERBAIJAN" in a.upper() for a in IGNORED_AIRLINES)
    has_t5 = any("TURKMENISTAN" in a.upper() or "T5" == a.upper() for a in IGNORED_AIRLINES)
    cfg_loaded = has_azal and has_t5

    # 2. Teste Titel-Erkennung (Pre-Filter)
    match_azal, _ = is_airline_ignored_in_text("Azerbaijan Airlines: Business Class nach Baku ab 499€")
    match_azal_code, _ = is_airline_ignored_in_text("AZAL Sale: Flüge nach Dubai in Business Class")
    match_t5, _ = is_airline_ignored_in_text("Turkmenistan Airlines Business Class nach Bangkok ab 1.250€")
    match_t5_context, _ = is_airline_ignored_in_text("Flüge nach Asien mit T5 Business Class")

    # Sicherstellen, dass Terminal 5 (LHR T5, mit T5 Umstieg) NICHT fälschlicherweise als T5 Airline matcht
    no_false_t5, _ = is_airline_ignored_in_text("British Airways ab London Heathrow (LHR T5) nach New York")
    no_false_ba_t5, _ = is_airline_ignored_in_text(
        "British Airways Club World ab London mit T5 Umstieg nach Tokio für 1600€"
    )
    no_false_swiss, _ = is_airline_ignored_in_text("SWISS Business Class ab FRA nach BKK für 1.600€")

    title_checks = (
        match_azal
        and match_azal_code
        and match_t5
        and match_t5_context
        and not no_false_t5
        and not no_false_ba_t5
        and not no_false_swiss
    )

    # 3. Teste Deal-Erkennung (Post-Filter)
    base_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Fliege in der SWISS Business Class nach Bangkok",
        is_relevant=True,
        deal_category="Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)",
        airline_names="",
        primary_airline_iata="",
        iata_origin="FRA",
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2026-11-15",
        sample_return_date="2026-11-28",
        price_eur=1200,
        booking_class="C",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="Lie-Flat",
        routing_advice="",
        rejection_reason="",
    )

    deal_t5 = base_deal.model_copy(update={"primary_airline_iata": "T5", "airline_names": "Turkmenistan Airlines (T5)"})
    deal_azal = base_deal.model_copy(
        update={"primary_airline_iata": "J2", "airline_names": "Azerbaijan Airlines (AZAL)"}
    )
    deal_swiss = base_deal.model_copy(update={"primary_airline_iata": "LX", "airline_names": "SWISS (LX)"})

    ignored_t5, _ = is_deal_ignored_airline(deal_t5)
    ignored_azal, _ = is_deal_ignored_airline(deal_azal)
    ignored_swiss, _ = is_deal_ignored_airline(deal_swiss)

    deal_checks = ignored_t5 and ignored_azal and not ignored_swiss

    passed = cfg_loaded and title_checks and deal_checks
    details = f"Config={cfg_loaded}, TitleFilter={title_checks}, DealFilter={deal_checks}"
    record_result("Test 9: Airline-Ignorier-Filter (TOML-Config, Pre- & Post-Filter)", passed, details)
    return passed


def test_10_pre_filter_defense():
    fly4free_feed = "https://www.fly4free.com/flights/flight-deals/europe/feed/"
    travel_dealz_feed = "https://travel-dealz.de/kategorie/fluege/business-class/feed/"

    # 1. Fall Condor Phuket: Gemischter Feed, kein Business-Signal -> MUSS abgelehnt werden
    ok_condor, reason_condor = is_feed_entry_eligible_pre_filter(
        entry_title="Last minute direct flights from Frankfurt to Phuket, Thailand for €564",
        entry_content="Direct flights with Condor. Baggage allowance: personal item only.",
        feed_url=fly4free_feed,
    )
    passed_condor = (not ok_condor) and ("Business/First-Signalwort" in reason_condor)

    # 2. Fall Air China Warschau: 'Full-service' im Titel -> MUSS abgelehnt werden
    ok_ca, reason_ca = is_feed_entry_eligible_pre_filter(
        entry_title="Full-service flights from Warsaw to Hong Kong for €567",
        entry_content="Fly with Air China to Hong Kong with meals and bags included.",
        feed_url=fly4free_feed,
    )
    passed_ca = (not ok_ca) and ("Negativ-Keyword im Titel" in reason_ca or "Business/First-Signalwort" in reason_ca)

    # 3. Positiv-Fall auf gemischtem Feed: Echtes Business-Class Signal
    ok_mixed_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Qatar Airways Qsuite from Paris to Bangkok for €1650",
        entry_content="Fly in business class with top lie-flat product.",
        feed_url=fly4free_feed,
    )

    # 4. Dedizierter Premium-Feed: Wird durchgelassen
    ok_dedicated, _ = is_feed_entry_eligible_pre_filter(
        entry_title="SWISS: Fernost ab 1.620€",
        entry_content="Attraktive Tarife ab Frankfurt nach Bangkok.",
        feed_url=travel_dealz_feed,
    )

    # 5. You Have Been Upgraded (Dedizierter Premium-Feed): GDS/Matrix Tarife direkt durchgelassen
    yhbu_feed = "https://youhavebeenupgraded.boardingarea.com/feed/"
    ok_yhbu, _ = is_feed_entry_eligible_pre_filter(
        entry_title="DEAL: Air India Business Frankfurt - Indien (Return) 1.373€",
        entry_content="Air India bietet günstige Flüge in Business Class nach Indien.",
        feed_url=yhbu_feed,
    )

    # 6. Frankfurtflyer (Gemischter Feed): Meilen-/Kreditkarten-Deals blockiert, echte Biz-Sales durchgelassen
    ff_feed = "https://frankfurtflyer.de/?feed=rss2"
    ok_ff_bad, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Hilton Hotel Promo oder Kreditkarte mit 50.000 Punkten",
        entry_content="Aktion für Hotel und Kreditkarte.",
        feed_url=ff_feed,
    )
    ok_ff_good, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Star Alliance Business Class Sale ab Deutschland",
        entry_content="Fliege in der Business Class ab Frankfurt nach Singapur mit Lie-Flat Sitzen.",
        feed_url=ff_feed,
    )

    # 7. Secret Flying & Flynous (Gemischte Feeds): Economy ohne Signalwort geblockt, echte Biz-Deals durchgelassen
    sf_feed = "https://www.secretflying.com/category/flight-deals/europe-deals/feed/"
    ok_sf_eco, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Cheap flights from London to Barcelona for only £35 roundtrip",
        entry_content="Fly with Ryanair or easyJet.",
        feed_url=sf_feed,
    )
    ok_sf_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="HOT! Business Class flights from Rome to Tokyo for €1,350 roundtrip",
        entry_content="Fly in Qatar Airways business class with lie-flat seats.",
        feed_url=sf_feed,
    )

    flynous_feed = "https://www.flynous.com/feed/"
    ok_fn_eco, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Non-stop flights from Milan to New York for €299",
        entry_content="Cheap transatlantic flight deal.",
        feed_url=flynous_feed,
    )
    ok_fn_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Lufthansa Business Class from Amsterdam to Singapore from €1490",
        entry_content="Enjoy Lufthansa long-haul business class lie-flat comfort.",
        feed_url=flynous_feed,
    )

    # 8. FlyerTalk Premium Fare Deals (Dedizierter Premium-Feed)
    ft_feed = "https://www.flyertalk.com/forum/external.php?type=RSS2&forumids=372"
    ok_ft, _ = is_feed_entry_eligible_pre_filter(
        entry_title="BA: AMS-SIN €1400 return in CW",
        entry_content="British Airways Club World deal booked on BA.com.",
        feed_url=ft_feed,
    )

    # 9. False-Negative-Schutz: 'Full-service' im Titel darf Business Class NICHT blockieren
    ok_fs_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Full-service 5* Business Class from Frankfurt to Tokyo for 1450€",
        entry_content="Enjoy lie-flat business class seats.",
        feed_url=fly4free_feed,
    )

    # 10. False-Negative-Schutz: 'Economy' im Titel darf Business Class Vergleich NICHT blockieren
    ok_eco_comp_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Lufthansa Business Class günstiger als Premium Economy nach Tokyo ab 1400€",
        entry_content="Star Alliance business class sale.",
        feed_url=fly4free_feed,
    )

    all_passed = (
        passed_condor
        and passed_ca
        and ok_mixed_biz
        and ok_dedicated
        and ok_yhbu
        and (not ok_ff_bad)
        and ok_ff_good
        and (not ok_sf_eco)
        and ok_sf_biz
        and (not ok_fn_eco)
        and ok_fn_biz
        and ok_ft
        and ok_fs_biz
        and ok_eco_comp_biz
    )
    details = (
        f"CondorBlocked={passed_condor}, FullServiceBlocked={passed_ca}, "
        f"MixedBizAllowed={ok_mixed_biz}, DedicatedAllowed={ok_dedicated}, "
        f"YHBUAllowed={ok_yhbu}, FFPromoBlocked={not ok_ff_bad}, FFBizAllowed={ok_ff_good}, "
        f"SFEcoBlocked={not ok_sf_eco}, SFBizAllowed={ok_sf_biz}, "
        f"FlynousEcoBlocked={not ok_fn_eco}, FlynousBizAllowed={ok_fn_biz}, FlyerTalkAllowed={ok_ft}, "
        f"FSBizAllowed={ok_fs_biz}, EcoCompBizAllowed={ok_eco_comp_biz}"
    )
    record_result("Test 10: Pre-Filter Defense (Gemischte Feeds & Negativ-Keywords)", all_passed, details)
    return all_passed


def test_11_post_validation_airbag():
    valid_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Fliege in der SWISS Business Class nach Bangkok",
        is_relevant=True,
        deal_category="FTL-Booster (Points + QP)",
        airline_names="SWISS (LX)",
        primary_airline_iata="LX",
        iata_origin="FRA",
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2026-11-15",
        sample_return_date="2026-11-28",
        price_eur=1620,
        booking_class="P",
        mm_points=480,
        mm_qualifying_points=480,
        cabin_assessment="Lie-Flat B777",
        routing_advice="",
        rejection_reason="",
    )
    sample_text = "SWISS Business Class ab Frankfurt nach Bangkok ab 1.620 Euro mit Lie-Flat B777."

    # 1. Gültiger Deal muss passieren
    safe_valid, _ = validate_deal_safety(valid_deal, "SWISS Business Class nach BKK", sample_text)

    # 2. Veto bei fehlendem Textnachweis
    deal_no_quote = valid_deal.model_copy(update={"cabin_class_quote": "KEIN NACHWEIS"})
    safe_no_quote, reason_no_quote = validate_deal_safety(deal_no_quote, "Deal", sample_text)
    passed_no_quote = (not safe_no_quote) and ("Kein Textnachweis" in reason_no_quote)

    # 3. Veto bei unklarer Reiseklasse
    deal_eco = valid_deal.model_copy(update={"detected_cabin_class": "Economy / Premium Eco"})
    safe_eco, reason_eco = validate_deal_safety(deal_eco, "Deal", sample_text)
    passed_eco = (not safe_eco) and ("Reiseklasse nicht Business/First" in reason_eco)

    # 4. Veto bei unplausiblem Langstreckenpreis (< 750 € ohne Error Fare)
    deal_cheap = valid_deal.model_copy(update={"price_eur": 564})
    safe_cheap, reason_cheap = validate_deal_safety(
        deal_cheap, "Condor nach Phuket", "Condor Business Class ab Frankfurt nach Phuket für 564 Euro."
    )
    passed_cheap = (not safe_cheap) and ("Unplausibler Langstreckenpreis" in reason_cheap)

    # 5. Veto bei Origin-Hijacking (z.B. BER eingesetzt, obwohl nur WAW im Text vorkommt)
    deal_hijack = valid_deal.model_copy(update={"iata_origin": "BER"})
    waw_text = "Air China Business Class ab Warschau (WAW) nach Hongkong ab 1.500 Euro."
    safe_hijack, reason_hijack = validate_deal_safety(deal_hijack, "Air China nach Hongkong", waw_text)
    passed_hijack = (not safe_hijack) and ("Origin-Hijacking" in reason_hijack)

    all_passed = safe_valid and passed_no_quote and passed_eco and passed_cheap and passed_hijack
    details = f"Valid={safe_valid}, NoQuoteVeto={passed_no_quote}, EcoVeto={passed_eco}, CheapVeto={passed_cheap}, HijackVeto={passed_hijack}"
    record_result(
        "Test 11: Post-Validation Airbag (Quote-Proof, Preisplausibilität, Anti-Hijacking)", all_passed, details
    )
    return all_passed


def test_12_telegram_formatting_and_redundancy_defense():
    deal_base = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Business Class Round Trip",
        is_relevant=True,
        deal_category="Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)",
        airline_names="Etihad Airways",
        primary_airline_iata="EY",
        iata_origin="PRG",
        iata_destination="PEK",
        is_open_jaw=False,
        iata_return_end="PRG",
        region="Asien",
        sample_outbound_date="2026-11-15",
        sample_return_date="2026-11-28",
        price_eur=2040,
        booking_class="Z",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="Standard Business Class",
        routing_advice="",
        rejection_reason="",
    )

    # 1. Fall: Redundante Meilen- und Allianzerklärung im advice muss gefiltert werden
    deal_redundant = deal_base.model_copy(
        update={
            "routing_advice": "Ex-EU Abflug ab Prag (PRG). Etihad Airways ist nicht in einer Allianz, daher 0 Miles & More Points/QP."
        }
    )
    msg_red = build_telegram_message(deal_redundant, "Prague -> Beijing", "https://example.com/deal")
    passed_no_red = ("Besonderheiten:" not in msg_red) and ("Etihad Airways ist nicht in einer Allianz" not in msg_red)
    passed_no_preis_check = "Preis-Check" not in msg_red
    passed_category_gap = (
        "✈️ <b>Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)</b>\n\n<b>Prague -&gt; Beijing</b>" in msg_red
    )

    # 2. Fall: Deal ohne advice hat saubere Trennung und keine Besonderheiten-Zeile
    deal_empty = deal_base.model_copy(update={"routing_advice": ""})
    msg_empty = build_telegram_message(deal_empty, "Prague -> Beijing", "https://example.com/deal")
    passed_empty = ("Besonderheiten:" not in msg_empty) and (
        "• <b>Bordprodukt:</b> Standard Business Class\n\n🔗 <b>Direktlinks:</b>" in msg_empty
    )

    # 3. Fall: Deal mit echten Mehrwerten wird korrekt gerendert
    deal_features = deal_base.model_copy(
        update={"routing_advice": "Auch ab BUD und MXP buchbar. Stopover in AUH kostenlos."}
    )
    msg_feat = build_telegram_message(deal_features, "Prague -> Beijing", "https://example.com/deal")
    passed_feat = (
        "• <b>Besonderheiten:</b> Auch ab BUD und MXP buchbar. Stopover in AUH kostenlos.\n\n🔗 <b>Direktlinks:</b>"
        in msg_feat
    )

    # 4. Fall: Generischer OTA / Preis-Check Boilerplate-Hinweis muss gefiltert werden
    deal_boilerplate = deal_base.model_copy(
        update={
            "routing_advice": "Preis-Check: OTA-Bestpreis (Momondo/Kayak/Skyscanner) vs. Airline-Direktbuchung vergleichen (Zahlungsaufschläge & Phantom-Verfügbarkeit prüfen!)."
        }
    )
    msg_boiler = build_telegram_message(deal_boilerplate, "Prague -> Beijing", "https://example.com/deal")
    passed_no_boiler = ("Besonderheiten:" not in msg_boiler) and ("Preis-Check" not in msg_boiler)

    all_passed = (
        passed_no_red
        and passed_no_preis_check
        and passed_category_gap
        and passed_empty
        and passed_feat
        and passed_no_boiler
    )
    details = f"RedundantFiltered={passed_no_red}, NoPreisCheck={passed_no_preis_check}, CategoryGap={passed_category_gap}, CleanEmpty={passed_empty}, FeatureShown={passed_feat}, BoilerplateFiltered={passed_no_boiler}"
    record_result(
        "Test 12: Telegram Formatting & Redundancy Defense (No-Boilerplate, Spacing, Filter)", all_passed, details
    )
    return all_passed


def test_13_miles_and_more_lot_and_partner_qp():
    from bot import build_telegram_message, normalize_and_verify_deal_mm_points

    base_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Fliege in der LOT Business Class nach Tokio",
        is_relevant=True,
        deal_category="Star Alliance (Nur Points)",
        airline_names="LOT Polish Airlines",
        primary_airline_iata="LO",
        iata_origin="FRA",
        iata_destination="NRT",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2026-11-15",
        sample_return_date="2026-11-28",
        price_eur=1490,
        booking_class="P",
        mm_points=480,
        mm_qualifying_points=0,  # Fehlerhafter Modell-Output (bisheriges Problem)
        cabin_assessment="Boeing 787 Dreamliner Lie-Flat",
        routing_advice="",
        rejection_reason="",
    )

    # 1. Fall: LOT mit LO und 0 QP muss auf 480 QP und 'FTL-Booster (Points + QP)' korrigiert werden
    deal_lot_fixed = normalize_and_verify_deal_mm_points(base_deal.model_copy())
    passed_lot_qp = (
        deal_lot_fixed.mm_qualifying_points == 480
        and deal_lot_fixed.deal_category == "FTL-Booster (Points + QP)"
        and deal_lot_fixed.primary_airline_iata == "LO"
    )

    # 2. Fall: LOT mit 3-Letter Code 'LOT' muss zu 'LO' normalisiert und korrigiert werden
    deal_lot_3letter = base_deal.model_copy(update={"primary_airline_iata": "LOT"})
    deal_lot_3letter_fixed = normalize_and_verify_deal_mm_points(deal_lot_3letter)
    passed_lot_3letter = (
        deal_lot_3letter_fixed.primary_airline_iata == "LO"
        and deal_lot_3letter_fixed.mm_qualifying_points == 480
        and deal_lot_3letter_fixed.deal_category == "FTL-Booster (Points + QP)"
    )

    # 3. Fall: LOT ohne primary_airline_iata, aber Name 'LOT Polish Airlines'
    deal_lot_name_only = base_deal.model_copy(update={"primary_airline_iata": ""})
    deal_lot_name_fixed = normalize_and_verify_deal_mm_points(deal_lot_name_only)
    passed_lot_name = (
        deal_lot_name_fixed.primary_airline_iata == "LO"
        and deal_lot_name_fixed.mm_qualifying_points == 480
        and deal_lot_name_fixed.deal_category == "FTL-Booster (Points + QP)"
    )

    # 4. Fall: Andere vollintegrierte Airline (z.B. Austrian OS) mit 0 QP
    deal_os = base_deal.model_copy(update={"airline_names": "Austrian Airlines (OS)", "primary_airline_iata": "OS"})
    deal_os_fixed = normalize_and_verify_deal_mm_points(deal_os)
    passed_os = deal_os_fixed.mm_qualifying_points == 480 and deal_os_fixed.deal_category == "FTL-Booster (Points + QP)"

    # 5. Fall: Reiner Star Alliance Partner (z.B. Turkish Airlines TK) darf KEINE QP bekommen
    deal_tk = base_deal.model_copy(
        update={
            "airline_names": "Turkish Airlines (TK)",
            "primary_airline_iata": "TK",
            "deal_category": "Star Alliance (Nur Points)",
            "mm_points": 400,
            "mm_qualifying_points": 0,
        }
    )
    deal_tk_fixed = normalize_and_verify_deal_mm_points(deal_tk)
    passed_tk = deal_tk_fixed.mm_qualifying_points == 0 and deal_tk_fixed.deal_category == "Star Alliance (Nur Points)"

    # 6. Fall: Telegram Nachricht für LOT muss 'FTL-Booster' und '480 QP' anzeigen
    tg_msg = build_telegram_message(deal_lot_fixed, "LOT Business Class nach Tokio", "https://example.com/lot")
    passed_tg_render = (
        "FTL-Booster (Points + QP)" in tg_msg
        and "480 Points | <b>480 QP</b>" in tg_msg
        and "<b>0 QP</b>" not in tg_msg
        and "Star Alliance (Nur Points)" not in tg_msg
    )

    all_passed = (
        passed_lot_qp and passed_lot_3letter and passed_lot_name and passed_os and passed_tk and passed_tg_render
    )
    details = (
        f"LOTFixed={passed_lot_qp}, LOT3Letter={passed_lot_3letter}, LOTName={passed_lot_name}, "
        f"OSFixed={passed_os}, TKUntouched={passed_tk}, TGRender={passed_tg_render}"
    )
    record_result("Test 13: Miles & More Partner & LOT Polish Airlines QP Defense", all_passed, details)
    return all_passed


def main():
    print("=" * 60)
    print("🛫 FLIGHT-DEAL-BOT v3.0: LOKALER INTEGRATIONS-TESTLAUF")
    print("=" * 60)

    t1 = test_1_env_variables()
    t2 = test_2_url_normalization_and_html_cleaning()
    t3 = test_3_database_and_deduplication()
    t4 = test_4_google_flights_url_builders()
    t5 = test_5_telegram_api_connectivity()
    t6 = test_6_gemini_api_and_pydantic_schema()
    t7 = test_7_rss_feeds_availability()
    t8 = test_8_trafilatura_enrichment()
    t9 = test_9_airline_ignore_filter()
    t10 = test_10_pre_filter_defense()
    t11 = test_11_post_validation_airbag()
    t12 = test_12_telegram_formatting_and_redundancy_defense()
    t13 = test_13_miles_and_more_lot_and_partner_qp()

    print("=" * 60)
    all_ok = all(r[1] for r in TEST_RESULTS)
    if all_ok:
        print("🎉 ALLE TESTS ERFOLGREICH BESTANDEN!")
        print("Der Bot ist bereit für das Docker-Build & Deployment.")
    else:
        print("⚠️ EINIGE TESTS SIND FEHLGESCHLAGEN. Bitte Ausgaben oben prüfen.")
    print("=" * 60)

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
