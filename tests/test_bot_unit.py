import base64
import logging
import os
import sqlite3
import tempfile
import threading
import time
import urllib.parse
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import requests
from fast_flights.pb import flights_pb2
from google.genai import _extra_utils, _transformers, types

from bot import (
    CONFIG,
    IGNORED_AIRLINES,
    ActivityWatchdog,
    FlightDealEvaluation,
    GoogleFlightsLiveCheckResult,
    build_gf_main_url,
    build_positioning_url,
    build_telegram_message,
    checkpoint_db,
    clean_html,
    cleanup_db,
    clear_gemini_model_cooldowns,
    close_db,
    create_genai_client,
    ensure_valid_future_dates,
    evaluate_with_gemini,
    extract_entry_published_utc,
    extract_gf_lowest_price_from_html,
    fetch_all_feeds,
    format_deal_route_plain,
    generate_with_gemini_fallback,
    get_enriched_deal_context,
    init_db,
    is_airline_ignored_in_text,
    is_deal_ignored_airline,
    is_feed_entry_eligible_pre_filter,
    main,
    normalize_additional_destinations,
    normalize_airport_iata,
    normalize_and_verify_deal_mm_points,
    normalize_iata_via,
    normalize_url,
    record_sent_deal,
    resolve_deal_category,
    resolve_deal_stops,
    run_google_flights_live_check,
    send_telegram,
    should_evaluate_url,
    should_send_deal,
    should_trip_circuit_breaker,
    upsert_seen_url,
    validate_deal_safety,
    wait_for_next_cycle,
)


def test_url_normalization_and_html_cleaning():
    url_with_tracking = "https://travel-dealz.de/deal/test-deal/?utm_source=rss&utm_medium=feed&ref=newsletter"
    clean = normalize_url(url_with_tracking)
    assert clean == "https://travel-dealz.de/deal/test-deal/"

    forum_url = "https://www.vielfliegertreff.de/forum/threads/test-deal.12345/?t=12345&utm_campaign=track"
    clean_forum = normalize_url(forum_url)
    assert clean_forum == "https://www.vielfliegertreff.de/forum/threads/test-deal.12345/?t=12345"

    raw_html = "<p>Super <b>Business Class</b> Flug! <script>alert(1)</script>&amp; mehr Details.</p>"
    cleaned = clean_html(raw_html)
    assert cleaned == "Super Business Class Flug! & mehr Details."


def test_database_and_deduplication():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        test_db = tmp.name

    try:
        conn = init_db(test_db)
        c = conn.cursor()

        # 1. Schema-Prüfung
        c.execute("PRAGMA table_info(sent_deals)")
        cols = {col[1] for col in c.fetchall()}
        assert {"price_eur", "airline_code", "updated_at", "deal_fingerprint"}.issubset(cols)

        # 2. Test Stufe 1: seen_urls
        test_url = "https://travel-dealz.de/deal/biz-bangkok/"
        c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (test_url,))
        conn.commit()
        c.execute("SELECT 1 FROM seen_urls WHERE url_clean = ?", (test_url,))
        assert c.fetchone() is not None

        # 3. Test Stufe 2: Intelligenter Airline- & Preissturz-Check
        deal_rj = FlightDealEvaluation(
            detected_cabin_class="Business Class",
            cabin_class_quote="Fliege mit Royal Jordanian nach Bangkok",
            is_relevant=True,
            deal_category="Top-Bordprodukt (Oneworld)",
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
        assert send_1 is True
        assert reason_1 == "new_deal"
        record_sent_deal(conn, deal_rj)

        # B) Cross-Feed-Duplikat (gleicher oder teurerer Preis) -> Muss unterdrückt werden
        deal_rj_dup = deal_rj.model_copy(update={"price_eur": 1720})
        send_dup, reason_dup, _ = should_send_deal(conn, deal_rj_dup)
        assert send_dup is False
        assert reason_dup == "duplicate"

        # C) Gleiche Strecke, aber ANDERE Airline -> Darf NICHT blockiert werden
        deal_swiss = deal_rj.model_copy(
            update={"primary_airline_iata": "LX", "airline_names": "SWISS (LX)", "price_eur": 1740}
        )
        send_swiss, reason_swiss, _ = should_send_deal(conn, deal_swiss)
        assert send_swiss is True
        assert reason_swiss == "new_deal"
        record_sent_deal(conn, deal_swiss)

        # D) Echter Preissturz bei gleicher Airline (> 75€ günstiger)
        deal_rj_cheap = deal_rj.model_copy(update={"price_eur": 1490})
        send_drop, reason_drop, prev_drop = should_send_deal(conn, deal_rj_cheap)
        assert send_drop is True
        assert reason_drop == "price_drop"
        assert prev_drop == 1697

        conn.close()
    finally:
        if os.path.exists(test_db):
            os.remove(test_db)


def test_google_flights_url_builders():
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
    assert "google.com/travel/flights" in gf_url
    assert "tfs=" in gf_url or "FRA" in gf_url

    # Open Jaw
    deal_oj = deal_rt.model_copy(update={"is_open_jaw": True, "iata_return_end": "MUC"})
    gf_oj_url = build_gf_main_url(deal_oj)
    assert "google.com/travel/flights" in gf_oj_url
    assert "tfs=" in gf_oj_url or "MUC" in gf_oj_url

    # Positionierung
    pos_url = build_positioning_url("WAW", "2026-11-15", "2026-11-28")
    assert "google.com/travel/flights" in pos_url
    assert "tfs=" in pos_url or "WAW" in pos_url


def test_airline_ignore_filter():
    has_azal = any("AZAL" in a.upper() or "AZERBAIJAN" in a.upper() for a in IGNORED_AIRLINES)
    has_t5 = any("TURKMENISTAN" in a.upper() or "T5" == a.upper() for a in IGNORED_AIRLINES)
    assert has_azal and has_t5

    match_azal, _ = is_airline_ignored_in_text("Azerbaijan Airlines: Business Class nach Baku ab 499€")
    match_azal_code, _ = is_airline_ignored_in_text("AZAL Sale: Flüge nach Dubai in Business Class")
    match_t5, _ = is_airline_ignored_in_text("Turkmenistan Airlines Business Class nach Bangkok ab 1.250€")
    match_t5_context, _ = is_airline_ignored_in_text("Flüge nach Asien mit T5 Business Class")

    no_false_t5, _ = is_airline_ignored_in_text("British Airways ab London Heathrow (LHR T5) nach New York")
    no_false_ba_t5, _ = is_airline_ignored_in_text(
        "British Airways Club World ab London mit T5 Umstieg nach Tokio für 1600€"
    )
    no_false_swiss, _ = is_airline_ignored_in_text("SWISS Business Class ab FRA nach BKK für 1.600€")

    assert match_azal and match_azal_code and match_t5 and match_t5_context
    assert not no_false_t5
    assert not no_false_ba_t5
    assert not no_false_swiss

    base_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Fliege in der SWISS Business Class nach Bangkok",
        is_relevant=True,
        deal_category="Top-Bordprodukt",
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

    assert ignored_t5
    assert ignored_azal
    assert not ignored_swiss


def test_pre_filter_defense():
    fly4free_feed = "https://www.fly4free.com/flights/flight-deals/europe/feed/"
    travel_dealz_feed = "https://travel-dealz.de/kategorie/fluege/business-class/feed/"

    # 1. Condor Phuket ohne Business Signal -> Reject
    ok_condor, reason_condor = is_feed_entry_eligible_pre_filter(
        entry_title="Last minute direct flights from Frankfurt to Phuket, Thailand for €564",
        entry_content="Direct flights with Condor. Baggage allowance: personal item only.",
        feed_url=fly4free_feed,
    )
    assert not ok_condor
    assert "Business/First-Signalwort" in reason_condor

    # 2. Air China Warschau: 'Full-service' -> Reject
    ok_ca, reason_ca = is_feed_entry_eligible_pre_filter(
        entry_title="Full-service flights from Warsaw to Hong Kong for €567",
        entry_content="Fly with Air China to Hong Kong with meals and bags included.",
        feed_url=fly4free_feed,
    )
    assert not ok_ca

    # 3. Positiv-Fall auf gemischtem Feed
    ok_mixed_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Qatar Airways Qsuite from Paris to Bangkok for €1650",
        entry_content="Fly in business class with top lie-flat product.",
        feed_url=fly4free_feed,
    )
    assert ok_mixed_biz

    # 4. Dedizierter Premium-Feed
    ok_dedicated, _ = is_feed_entry_eligible_pre_filter(
        entry_title="SWISS: Fernost ab 1.620€",
        entry_content="Attraktive Tarife ab Frankfurt nach Bangkok.",
        feed_url=travel_dealz_feed,
    )
    assert ok_dedicated

    # 5. False-Negative-Schutz: 'Full-service' im Titel darf Business Class nicht blockieren
    ok_fs_biz, _ = is_feed_entry_eligible_pre_filter(
        entry_title="Full-service 5* Business Class from Frankfurt to Tokyo for 1450€",
        entry_content="Enjoy lie-flat business class seats.",
        feed_url=fly4free_feed,
    )
    assert ok_fs_biz


def test_post_validation_airbag():
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

    safe_valid, _ = validate_deal_safety(valid_deal, "SWISS Business Class nach BKK", sample_text)
    assert safe_valid

    # Veto bei fehlendem Quote-Nachweis
    deal_no_quote = valid_deal.model_copy(update={"cabin_class_quote": "KEIN NACHWEIS"})
    safe_no_quote, reason_no_quote = validate_deal_safety(deal_no_quote, "Deal", sample_text)
    assert not safe_no_quote
    assert "Kein Textnachweis" in reason_no_quote

    # Veto bei falscher Kabinenklasse
    deal_eco = valid_deal.model_copy(update={"detected_cabin_class": "Economy / Premium Eco"})
    safe_eco, reason_eco = validate_deal_safety(deal_eco, "Deal", sample_text)
    assert not safe_eco

    # Veto bei unrealistischem Preis (< 750 EUR)
    deal_cheap = valid_deal.model_copy(update={"price_eur": 564})
    safe_cheap, _ = validate_deal_safety(deal_cheap, "Condor Deal", "Condor Business Class ab Frankfurt")
    assert not safe_cheap

    # Veto bei Origin-Hijacking
    deal_hijack = valid_deal.model_copy(update={"iata_origin": "BER"})
    waw_text = "Air China Business Class ab Warschau (WAW) nach Hongkong ab 1.500 Euro."
    safe_hijack, reason_hijack = validate_deal_safety(deal_hijack, "Air China nach Hongkong", waw_text)
    assert not safe_hijack
    assert "Origin-Hijacking" in reason_hijack


def test_telegram_formatting_and_redundancy_defense():
    deal_base = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Business Class Round Trip",
        is_relevant=True,
        deal_category="Top-Bordprodukt",
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

    deal_redundant = deal_base.model_copy(
        update={
            "routing_advice": "Ex-EU Abflug ab Prag (PRG). Etihad Airways ist nicht in einer Allianz, daher 0 Miles & More Points/QP."
        }
    )
    msg_red = build_telegram_message(deal_redundant, "Prague -> Beijing", "https://example.com/deal")
    assert "✈️ <b>Top-Bordprodukt</b>\n" in msg_red
    assert "Non-Alliance" not in msg_red
    assert "Oneworld" not in msg_red
    assert "SkyTeam" not in msg_red
    assert "Besonderheiten:" not in msg_red
    assert "Etihad Airways ist nicht in einer Allianz" not in msg_red

    deal_empty = deal_base.model_copy(update={"routing_advice": ""})
    msg_empty = build_telegram_message(deal_empty, "Prague -> Beijing", "https://example.com/deal")
    assert "Besonderheiten:" not in msg_empty

    deal_features = deal_base.model_copy(
        update={"routing_advice": "Auch ab BUD und MXP buchbar. Stopover in AUH kostenlos."}
    )
    msg_feat = build_telegram_message(deal_features, "Prague -> Beijing", "https://example.com/deal")
    assert "Stopover in AUH kostenlos" in msg_feat


def test_alliance_display_in_top_bordprodukt():
    from google.genai import _transformers

    schema = _transformers.t_schema(None, FlightDealEvaluation)
    cat_enum = schema.properties["deal_category"].enum
    assert "Top-Bordprodukt (Non-Alliance/Oneworld/SkyTeam)" not in cat_enum
    assert "Top-Bordprodukt" in cat_enum
    assert "Top-Bordprodukt (Oneworld)" in cat_enum
    assert "Top-Bordprodukt (SkyTeam)" in cat_enum

    # 1. Allianzlose Airlines (z.B. Etihad EY, China Southern CZ, Emirates EK, Gulf Air GF):
    #    Keine Allianz vorhanden -> es darf gar kein Allianz-Klammerzusatz angezeigt werden!
    deal_ey = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Amsterdam -> East & South East Asia Business Class Round Trip 1785€",
        is_relevant=True,
        deal_category="Top-Bordprodukt (Oneworld)",  # selbst bei falscher KI-Zuordnung korrigiert
        airline_names="Etihad Airways (EY)",
        primary_airline_iata="EY",
        iata_origin="AMS",
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="AMS",
        region="Asien",
        sample_outbound_date="2027-01-15",
        sample_return_date="2027-01-29",
        price_eur=1785,
        booking_class="Z",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="Business Studio",
        routing_advice="",
        rejection_reason="",
    )
    deal_ey_norm = normalize_and_verify_deal_mm_points(deal_ey.model_copy())
    assert deal_ey_norm.deal_category == "Top-Bordprodukt"
    msg_ey = build_telegram_message(
        deal_ey,
        "Amsterdam → East & South East Asia Business Class Round Trip 1785€",
        "https://example.com/ams-asia",
    )
    assert msg_ey.startswith("✈️ <b>Top-Bordprodukt</b>\n")
    assert "(" not in msg_ey.splitlines()[0]

    msg_ey_drop = build_telegram_message(
        deal_ey_norm,
        "Amsterdam → East & South East Asia Business Class Round Trip 1785€",
        "https://example.com/ams-asia",
        prev_price=1950,
    )
    assert msg_ey_drop.startswith("📉 <b>PREIS-UPDATE: Top-Bordprodukt</b>\n")

    deal_cz = deal_ey.model_copy(
        update={
            "deal_category": "Top-Bordprodukt (SkyTeam)",
            "airline_names": "China Southern Airlines (CZ)",
            "primary_airline_iata": "CZ",
        }
    )
    assert resolve_deal_category(deal_cz) == "Top-Bordprodukt"

    # 2. Oneworld-Airline (z.B. Qatar Airways QR, Oman Air WY, Finnair AY) -> Top-Bordprodukt (Oneworld)
    deal_qr = deal_ey.model_copy(
        update={
            "deal_category": "Top-Bordprodukt",
            "airline_names": "Qatar Airways (QR)",
            "primary_airline_iata": "QR",
        }
    )
    deal_qr_norm = normalize_and_verify_deal_mm_points(deal_qr)
    assert deal_qr_norm.deal_category == "Top-Bordprodukt (Oneworld)"
    msg_qr = build_telegram_message(deal_qr_norm, "Amsterdam → Bangkok", "https://example.com/qr")
    assert msg_qr.startswith("✈️ <b>Top-Bordprodukt (Oneworld)</b>\n")

    # 3. SkyTeam-Airline (z.B. KLM KL, Vietnam Airlines VN) -> Top-Bordprodukt (SkyTeam)
    deal_kl = deal_ey.model_copy(
        update={
            "deal_category": "Top-Bordprodukt",
            "airline_names": "KLM (KL)",
            "primary_airline_iata": "KL",
        }
    )
    deal_kl_norm = normalize_and_verify_deal_mm_points(deal_kl)
    assert deal_kl_norm.deal_category == "Top-Bordprodukt (SkyTeam)"
    msg_kl = build_telegram_message(deal_kl_norm, "Amsterdam → Bangkok", "https://example.com/kl")
    assert msg_kl.startswith("✈️ <b>Top-Bordprodukt (SkyTeam)</b>\n")


def test_required_intermediate_stops_in_telegram_message():
    from google.genai import _transformers

    # 1. Gemini Schema-Check: 'iata_via' muss im generierten Schema als Pflichtfeld (required) markiert sein
    schema = _transformers.t_schema(None, FlightDealEvaluation)
    assert "iata_via" in (schema.required or [])

    # 2. Exaktes Nutzerbeispiel: Finnair AMS -> BKK (muss HEL als Zwischenstopp in der Route zeigen,
    #    sowohl bei explizitem iata_via=['HEL'] als auch per deterministischem Hub-Fallback bei iata_via=[])
    deal_finnair = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="European Cities -> Bangkok Business Class Round Trip from 1785€",
        is_relevant=True,
        deal_category="Top-Bordprodukt (Oneworld)",
        airline_names="Finnair (AY)",
        primary_airline_iata="AY",
        iata_origin="AMS",
        iata_via=[],
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="AMS",
        region="Asien",
        sample_outbound_date="2027-01-15",
        sample_return_date="2027-01-29",
        price_eur=1785,
        booking_class="Nicht genannt",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="AirLounge seat",
        routing_advice="Auch ab Tallinn (TLL) sowie weiteren europäischen Städten buchbar, Stopover in Helsinki (HEL) möglich.",
        rejection_reason="",
    )
    assert resolve_deal_stops(deal_finnair) == ["HEL"]
    assert format_deal_route_plain(deal_finnair) == "AMS->HEL->BKK"
    msg_ay = build_telegram_message(
        deal_finnair,
        "European Cities → Bangkok Business Class Round Trip from 1785€",
        "https://premium-flights.com/business-class-european-cities-to-bangkok/",
    )
    assert "• <b>Route:</b> <code>AMS</code> ➔ <code>HEL</code> ➔ <code>BKK</code> (Finnair (AY))" in msg_ay
    assert "TLL" not in resolve_deal_stops(deal_finnair)

    # 3. Echter Nonstop-Flug ab Hub (z.B. HEL -> BKK mit Finnair) darf KEINEN doppelten Stopp zeigen
    deal_ay_nonstop = deal_finnair.model_copy(
        update={"iata_origin": "HEL", "iata_return_end": "HEL", "routing_advice": ""}
    )
    assert resolve_deal_stops(deal_ay_nonstop) == []
    msg_ay_nonstop = build_telegram_message(deal_ay_nonstop, "Helsinki -> Bangkok", "https://example.com/ay")
    assert "• <b>Route:</b> <code>HEL</code> ➔ <code>BKK</code> (Finnair (AY))" in msg_ay_nonstop

    # 4. Sanitizer für unsaubere Modell-Outputs (Klammernotation, Origin/Dest-Wiederholung, Direktflug-Marker)
    assert normalize_iata_via(["Helsinki (HEL)"], "AMS", "BKK") == ["HEL"]
    assert normalize_iata_via(["AMS", "HEL", "BKK"], "AMS", "BKK") == ["HEL"]
    assert normalize_iata_via(["FRA / MUC"], "HAM", "BKK") == ["FRA/MUC"]
    assert normalize_iata_via(["Direktflug", "Nonstop"], "FRA", "BKK") == []

    # 5. Mehrere Stopps & Open-Jaw (inkl. Gabelflug zurück zum Hub wie AMS -> HEL -> BKK -> HEL)
    deal_multi_oj = deal_finnair.model_copy(update={"iata_via": ["HEL"], "is_open_jaw": True, "iata_return_end": "HEL"})
    msg_oj = build_telegram_message(deal_multi_oj, "Open Jaw Deal", "https://example.com/oj")
    assert (
        "• <b>Route:</b> <code>AMS</code> ➔ <code>HEL</code> ➔ <code>BKK</code> ➔ <code>HEL</code> (Gabelflug) (Finnair (AY))"
        in msg_oj
    )

    # 6. Multi-Hub-Airline (Lufthansa): Nonstop ab FRA bleibt ohne Stopp, ab HAM Fallback auf FRA/MUC bzw. expliziter Hub
    deal_lh_fra = deal_finnair.model_copy(
        update={
            "airline_names": "Lufthansa (LH)",
            "primary_airline_iata": "LH",
            "iata_origin": "FRA",
            "iata_return_end": "FRA",
            "routing_advice": "",
        }
    )
    assert resolve_deal_stops(deal_lh_fra) == []

    deal_lh_ham = deal_lh_fra.model_copy(update={"iata_origin": "HAM", "iata_return_end": "HAM"})
    assert resolve_deal_stops(deal_lh_ham) == ["FRA/MUC"]


def test_miles_and_more_lot_and_partner_qp():
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
        mm_qualifying_points=0,
        cabin_assessment="Boeing 787 Dreamliner Lie-Flat",
        routing_advice="",
        rejection_reason="",
    )

    # LOT LO mit 0 QP -> 480 QP
    deal_lot_fixed = normalize_and_verify_deal_mm_points(base_deal.model_copy())
    assert deal_lot_fixed.mm_qualifying_points == 480
    assert deal_lot_fixed.deal_category == "FTL-Booster (Points + QP)"
    assert deal_lot_fixed.primary_airline_iata == "LO"

    # LOT 3-Letter Code 'LOT' -> 'LO'
    deal_lot_3letter = base_deal.model_copy(update={"primary_airline_iata": "LOT"})
    deal_lot_3letter_fixed = normalize_and_verify_deal_mm_points(deal_lot_3letter)
    assert deal_lot_3letter_fixed.primary_airline_iata == "LO"
    assert deal_lot_3letter_fixed.mm_qualifying_points == 480

    # Austrian OS
    deal_os = base_deal.model_copy(update={"airline_names": "Austrian Airlines (OS)", "primary_airline_iata": "OS"})
    deal_os_fixed = normalize_and_verify_deal_mm_points(deal_os)
    assert deal_os_fixed.mm_qualifying_points == 480
    assert deal_os_fixed.deal_category == "FTL-Booster (Points + QP)"

    # Turkish TK -> Keine QP
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
    assert deal_tk_fixed.mm_qualifying_points == 0
    assert deal_tk_fixed.deal_category == "Star Alliance (Nur Points)"


def test_genai_client_timeout_and_fallback_resilience():
    # 1. Prüfe, dass create_genai_client das Millisekunden-Timeout korrekt in Sekunden (45.0s) im SDK verankert
    client = create_genai_client(api_key="test-dummy-key", timeout_ms=45_000)
    req = client._api_client._build_request("post", "models/gemini-3.8-flash:generateContent", {"contents": []})
    assert req.timeout == 45.0

    # 2. Prüfe, dass GenerateContentConfig die übergebenen Parameter (http_options + AFC-Deaktivierung) akzeptiert
    captured_configs: list[dict] = []
    mock_client = MagicMock()

    def side_effect(model, contents, config):
        # Validiere gegen das echte google-genai Pydantic-Schema
        validated = types.GenerateContentConfig.model_validate(config)
        assert validated.automatic_function_calling.disable is True
        assert validated.http_options.timeout == 30_000
        captured_configs.append((model, config))
        if len(captured_configs) == 1:
            raise requests.exceptions.ReadTimeout("HTTPSConnectionPool: Read timed out. (read timeout=30.0)")
        if len(captured_configs) == 2:
            raise requests.exceptions.ConnectionError("RemoteDisconnected('Remote end closed connection')")
        mock_resp = MagicMock()
        mock_resp.text = "ok"
        return mock_resp

    mock_client.models.generate_content.side_effect = side_effect

    with patch("bot.interruptible_sleep", return_value=False):
        resp = generate_with_gemini_fallback(
            client=mock_client,
            prompt="Test Prompt",
            response_schema=FlightDealEvaluation,
            preferred_model="gemini-3.8-flash",
            http_timeout_ms=30_000,
            model_cooldowns={},
        )

    assert resp.text == "ok"
    assert len(captured_configs) == 3
    # Sicherstellen, dass bei Timeout/ConnectionError jeweils auf das nächste Modell weitergeschaltet wurde
    assert captured_configs[0][0] == "gemini-3.8-flash"
    assert captured_configs[1][0] == "gemini-flash-latest"
    assert captured_configs[2][0] == "gemini-3.5-flash"


def test_circuit_breaker_threshold():
    assert should_trip_circuit_breaker(0, threshold=3) is False
    assert should_trip_circuit_breaker(2, threshold=3) is False
    assert should_trip_circuit_breaker(3, threshold=3) is True
    assert should_trip_circuit_breaker(5, threshold=3) is True
    assert should_trip_circuit_breaker(5, threshold=0) is False


def test_activity_watchdog_and_cycle_wait():
    with tempfile.TemporaryDirectory() as tmpdir:
        hb_path = os.path.join(tmpdir, "heartbeat")
        stop_evt = threading.Event()
        exit_codes: list[int] = []
        triggered_evt = threading.Event()

        def fake_exit(code: int) -> None:
            exit_codes.append(code)
            triggered_evt.set()

        wd = ActivityWatchdog(
            max_inactivity_seconds=1,
            check_interval_seconds=0.05,
            heartbeat_file=hb_path,
            stop_event=stop_evt,
            exit_fn=fake_exit,
        )
        wd.beat(update_file=True)
        assert os.path.exists(hb_path)
        assert wd.check_once() is False
        assert exit_codes == []

        # Starte Hintergrund-Thread: ohne neuen Beat muss nach >1s der Selbstheilungs-Exit ausgelöst werden
        wd.start()
        assert triggered_evt.wait(timeout=2.5) is True
        stop_evt.set()
        assert exit_codes == [1]

        # Prüfe unterbrechbares Warten zwischen Zyklen
        cycle_stop = threading.Event()
        wd_active = ActivityWatchdog(
            max_inactivity_seconds=60,
            heartbeat_file=hb_path,
            stop_event=cycle_stop,
            exit_fn=fake_exit,
        )
        threading.Timer(0.1, cycle_stop.set).start()
        t0 = time.monotonic()
        interrupted = wait_for_next_cycle(30, stop_event=cycle_stop, watchdog=wd_active, heartbeat_interval=1)
        assert interrupted is True
        assert (time.monotonic() - t0) < 2.0


def test_wal_checkpoint_cleanup_and_cycle_close():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "deals.db")
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"

        conn = init_db(db_path)
        limit_row = conn.execute("PRAGMA journal_size_limit;").fetchone()
        assert limit_row is not None and limit_row[0] == 65536

        # Schreibe zahlreiche Zeilen, sodass die WAL-Datei befüllt wird
        c = conn.cursor()
        for i in range(250):
            c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (f"https://example.com/deal-{i}",))
        # Füge veraltete Einträge hinzu, die cleanup_db löschen und per VACUUM kompaktieren muss
        c.execute(
            "INSERT INTO seen_urls (url_clean, created_at) VALUES (?, datetime('now', '-120 days'))",
            ("https://example.com/old-url",),
        )
        c.execute(
            "INSERT INTO sent_deals (deal_fingerprint, price_eur, airline_code, created_at, updated_at) "
            "VALUES (?, 1500, 'LH', datetime('now', '-25 days'), datetime('now', '-25 days'))",
            ("FRA-BKK-LH",),
        )
        c.execute(
            "INSERT INTO sent_deals (deal_fingerprint, price_eur, airline_code, created_at, updated_at) "
            "VALUES (?, 1800, 'LO', datetime('now', '-10 days'), datetime('now', '-10 days'))",
            ("VIE-DEL-LO",),
        )
        conn.commit()
        c.close()

        assert os.path.exists(wal_path)
        assert os.path.getsize(wal_path) > 0

        # 1. checkpoint_db kürzt die -wal-Datei bei offener Verbindung sofort auf 0 Byte
        checkpoint_db(conn)
        assert os.path.getsize(wal_path) == 0

        # 2. cleanup_db löscht abgelaufene Zeilen (>21 Tage), behält neuere (10 Tage) und hält -wal auf 0 Byte
        cleanup_db(conn)
        assert os.path.getsize(wal_path) == 0
        remaining_old = conn.execute(
            "SELECT COUNT(*) FROM seen_urls WHERE url_clean = 'https://example.com/old-url'"
        ).fetchone()[0]
        assert remaining_old == 0
        remaining_expired_deals = conn.execute(
            "SELECT COUNT(*) FROM sent_deals WHERE deal_fingerprint = 'FRA-BKK-LH'"
        ).fetchone()[0]
        assert remaining_expired_deals == 0
        remaining_active_deals = conn.execute(
            "SELECT COUNT(*) FROM sent_deals WHERE deal_fingerprint = 'VIE-DEL-LO'"
        ).fetchone()[0]
        assert remaining_active_deals == 1

        # 3. close_db schließt die Verbindung sauber -> -wal und -shm verschwinden vollständig
        close_db(conn)
        assert os.path.exists(db_path)
        assert not os.path.exists(wal_path)
        assert not os.path.exists(shm_path)

        # 4. Prüfe, dass main() während wait_for_next_cycle keine offenen -wal/-shm Dateien hinterlässt
        hb_path = os.path.join(tmpdir, "heartbeat")
        observed_during_sleep: dict[str, bool] = {}

        def fake_wait(*args, **kwargs) -> bool:
            observed_during_sleep["wal_exists"] = os.path.exists(wal_path)
            observed_during_sleep["shm_exists"] = os.path.exists(shm_path)
            return True  # Beende Hauptschleife nach 1 Zyklus

        fake_entries = [
            (
                "https://travel-dealz.de/kategorie/fluege/business-class/feed/",
                [{"title": "Frage: Meilen kaufen", "link": "https://example.com/ignored-question"}],
            )
        ]

        with (
            patch("bot.DB_PATH", db_path),
            patch("bot.HEARTBEAT_FILE", hb_path),
            patch("bot.validate_startup"),
            patch("bot.create_genai_client", return_value=MagicMock()),
            patch("bot.fetch_all_feeds", return_value=fake_entries),
            patch("bot.wait_for_next_cycle", side_effect=fake_wait),
            patch("bot.STOP_EVENT", threading.Event()),
        ):
            main()

        assert observed_during_sleep == {"wal_exists": False, "shm_exists": False}
        assert not os.path.exists(wal_path)
        assert not os.path.exists(shm_path)


def test_central_asia_region_integration():
    # 1. Konfiguration enthält Zentralasien standardmäßig, preferred_hubs ohne CPH & Option-A-Preislimits (2600 / 2000)
    assert CONFIG["origin"]["preferred_hubs"] == ["HAM", "FRA", "BER"]
    assert "Zentralasien" in CONFIG["destinations"]["regions"]
    assert "Ozeanien" not in CONFIG["destinations"]["regions"]
    assert {"DWC", "RUH", "MCT", "SLL", "DOH", "AQJ", "TBS", "BUS", "KUT"}.issubset(
        set(CONFIG["destinations"]["sweet_spots"])
    )
    assert CONFIG["pricing"]["max_price_home_hubs"] == 2600
    assert CONFIG["pricing"]["max_price_ex_eu"] == 2000

    # 2. FlightDealEvaluation akzeptiert 'Zentralasien' als eigene Region (z.B. TK FRA -> IST -> NQZ)
    deal_nqz = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Turkish Airlines Business Class ab Frankfurt nach Astana",
        is_relevant=True,
        deal_category="Star Alliance (Nur Points)",
        airline_names="Turkish Airlines (TK)",
        primary_airline_iata="TK",
        iata_origin="FRA",
        iata_via=[],
        iata_destination="NQZ",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Zentralasien",
        sample_outbound_date="2027-02-10",
        sample_return_date="2027-02-20",
        price_eur=2089,
        booking_class="K",
        mm_points=480,
        mm_qualifying_points=0,
        cabin_assessment="Lie-Flat / Recliner Mix",
        routing_advice="",
        rejection_reason="",
    )
    assert resolve_deal_stops(deal_nqz) == ["IST"]
    safe_valid, _ = validate_deal_safety(
        deal_nqz,
        "Turkish Airlines Business Class von Frankfurt nach Astana",
        "Fliege in der Turkish Airlines Business Class ab Frankfurt (FRA) nach Astana (NQZ) für 2.089 Euro.",
    )
    assert safe_valid is True

    # 3. Preisplausibilitäts-Airbag (< 750 € ohne Error Fare) greift auch bei Zentralasien
    deal_cheap = deal_nqz.model_copy(update={"price_eur": 372})
    safe_cheap, veto_reason = validate_deal_safety(
        deal_cheap,
        "Flüge nach Astana",
        "Turkish Airlines Flüge ab Frankfurt (FRA) nach Astana (NQZ) für 372 Euro.",
    )
    assert safe_cheap is False
    assert "Unplausibler Langstreckenpreis" in veto_reason

    # 4. evaluate_with_gemini übergibt den expliziten Zentralasien-Hinweis im Prompt (auch bei Legacy-Config nur mit 'Asien')
    captured_prompts: list[str] = []
    mock_resp = MagicMock()
    mock_resp.parsed = deal_nqz

    def fake_gen(client, prompt, **kwargs):
        captured_prompts.append(prompt)
        return mock_resp

    with (
        patch.dict(CONFIG["destinations"], {"regions": ["Asien", "Südamerika", "Mittelamerika"]}),
        patch("bot.generate_with_gemini_fallback", side_effect=fake_gen),
    ):
        evaluated = evaluate_with_gemini(
            MagicMock(),
            "Turkish Airlines Business Class Frankfurt Astana",
            "Business Class ab Frankfurt (FRA) nach Astana (NQZ) ab 2.089 Euro.",
        )

    assert evaluated.iata_via == ["IST"]
    assert len(captured_prompts) == 1
    assert "ZENTRALASIEN" in captured_prompts[0]
    assert "Kasachstan" in captured_prompts[0]
    assert "Usbekistan" in captured_prompts[0]
    assert "Australien / Ozeanien" in captured_prompts[0]
    assert "Bevorzugte Hubs: BER, FRA, HAM)" in captured_prompts[0]
    assert "max. 2600 EUR Return" in captured_prompts[0]
    assert "TBS" in captured_prompts[0]
    assert "max. 2000 EUR Return." in captured_prompts[0]


def test_google_flights_url_sanitizes_metro_codes_and_past_dates():
    # 1. Metro-Codes werden auf echte Hauptflughäfen gemappt, echte Flughäfen bleiben unverändert
    assert normalize_airport_iata("PAR") == "CDG"
    assert normalize_airport_iata("ROM") == "FCO"
    assert normalize_airport_iata("LON") == "LHR"
    assert normalize_airport_iata("TYO") == "HND"
    assert normalize_airport_iata("JKT") == "CGK"
    assert normalize_airport_iata("CDG") == "CDG"
    assert normalize_airport_iata("ORY") == "ORY"
    assert normalize_airport_iata("SHA") == "SHA"

    # 2. Vergangene Daten (wie PAR->CMB am 2026-11-24 bei Stichtag 2026-11-27) werden wochentagserhaltend in die Zukunft geschoben
    ref_today = date(2026, 11, 27)
    out_fixed, ret_fixed = ensure_valid_future_dates("2026-11-24", "2026-12-08", reference_date=ref_today)
    assert out_fixed == "2026-12-15"  # +3 Wochen (bleibt Dienstag!) und >= +14 Tage ab 2026-11-27
    assert ret_fixed == "2026-12-29"  # Reisedauer 14 Tage bleibt exakt erhalten

    # Bereits in der Zukunft liegende Daten bleiben unverändert
    assert ensure_valid_future_dates("2027-02-10", "2027-02-24", reference_date=ref_today) == (
        "2027-02-10",
        "2027-02-24",
    )

    # 3. Exakter Regressionsfall: Paris (PAR) -> Colombo (CMB) mit Etihad (EY) und vergangenem Datum
    deal_par_cmb = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Paris -> Colombo Business Class Round Trip 1430€",
        is_relevant=True,
        deal_category="Top-Bordprodukt",
        airline_names="Etihad Airways (EY)",
        primary_airline_iata="EY",
        iata_origin="PAR",
        iata_via=["AUH"],
        iata_destination="CMB",
        is_open_jaw=False,
        iata_return_end="PAR",
        region="Asien",
        sample_outbound_date="2026-11-24",
        sample_return_date="2026-12-08",
        price_eur=1430,
        booking_class="Nicht genannt",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="Fully lie-flat Business Class seats",
        routing_advice="Minimum stay 4 days, maximum stay 3 months.",
        rejection_reason="",
    )

    gf_url = build_gf_main_url(deal_par_cmb, reference_date=ref_today)
    parsed = urllib.parse.urlsplit(gf_url)
    tfs_param = urllib.parse.parse_qs(parsed.query)["tfs"][0]
    info = flights_pb2.Info()
    info.ParseFromString(base64.b64decode(tfs_param))

    assert info.data[0].from_airport.airport == "CDG"
    assert info.data[0].to_airport.airport == "CMB"
    assert list(info.data[0].airlines) == ["EY"]
    assert info.data[0].date == "2026-12-15"
    assert info.data[1].from_airport.airport == "CMB"
    assert info.data[1].to_airport.airport == "CDG"
    assert info.data[1].date == "2026-12-29"
    assert info.seat == 3

    # 4. Positionierungs-Link HAM -> PAR wird ebenfalls auf CDG & Zukunftsdaten normalisiert
    pos_url = build_positioning_url("PAR", "2026-11-24", "2026-12-08", home="HAM", reference_date=ref_today)
    pos_parsed = urllib.parse.urlsplit(pos_url)
    pos_tfs = urllib.parse.parse_qs(pos_parsed.query)["tfs"][0]
    pos_info = flights_pb2.Info()
    pos_info.ParseFromString(base64.b64decode(pos_tfs))

    assert pos_info.data[0].from_airport.airport == "HAM"
    assert pos_info.data[0].to_airport.airport == "CDG"
    assert pos_info.data[0].date == "2026-12-14"
    assert pos_info.data[1].from_airport.airport == "CDG"
    assert pos_info.data[1].to_airport.airport == "HAM"
    assert pos_info.data[1].date == "2026-12-30"


def test_google_flights_live_check_and_positioning_price():
    ref_today = date(2026, 11, 27)
    deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Paris -> Colombo Business Class Round Trip 1430€",
        is_relevant=True,
        deal_category="Top-Bordprodukt",
        airline_names="Etihad Airways (EY)",
        primary_airline_iata="EY",
        iata_origin="PAR",
        iata_via=["AUH"],
        iata_destination="CMB",
        is_open_jaw=False,
        iata_return_end="PAR",
        region="Asien",
        sample_outbound_date="2026-11-24",
        sample_return_date="2026-12-08",
        price_eur=1430,
        booking_class="Nicht genannt",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="Fully lie-flat Business Class seats",
        routing_advice="",
        rejection_reason="",
    )

    # 1. Erster Termin (15.12.) ist zu teuer (2450€), zweiter (+14 Tage = 29.12.) bestätigt 1448€, Positionierung HAM->CDG kostet 138€
    seen_queries: list[tuple[str, str, str, str]] = []

    def fake_fetcher(q):
        fd0 = q.flight_data[0]
        key = (fd0.from_airport.airport, fd0.to_airport.airport, fd0.date, q.get_seat_type())
        seen_queries.append(key)
        if key == ("CDG", "CMB", "2026-12-15", "business"):
            return 2450
        if key == ("CDG", "CMB", "2026-12-29", "business"):
            return 1448
        if key == ("HAM", "CDG", "2026-12-28", "economy"):
            return 138
        return None

    live = run_google_flights_live_check(deal, home="HAM", reference_date=ref_today, price_fetcher=fake_fetcher)
    assert live.checked is True
    assert live.main_price_eur == 1448
    assert live.price_matches_deal is True
    assert live.main_outbound_date == "2026-12-29"
    assert live.main_return_date == "2027-01-12"
    assert live.positioning_price_eur == 138
    assert deal.sample_outbound_date == "2026-12-29"
    assert deal.sample_return_date == "2027-01-12"
    # Nach Treffer am 2. Termin wird der 3. Termin (+28 Tage) nicht mehr unnötig abgefragt
    assert len(seen_queries) == 3

    msg_ok = build_telegram_message(deal, "Paris -> Colombo", "https://example.com/deal", live_check=live)
    assert "• <b>Google Flights Live-Check:</b> ✅ ab ~1448 € bestätigt (29.12.–12.01.)" in msg_ok
    assert "Positionierung: HAM–CDG (+/- 24h Stopover, ab ~138 €)" in msg_ok

    # 2. Warn-Badge wenn Live-Preis deutlich über Deal-Preis liegt
    live_warn = GoogleFlightsLiveCheckResult(
        checked=True,
        main_price_eur=2180,
        main_outbound_date="2026-12-15",
        main_return_date="2026-12-29",
        price_matches_deal=False,
        positioning_price_eur=None,
    )
    msg_warn = build_telegram_message(deal, "Paris -> Colombo", "https://example.com/deal", live_check=live_warn)
    assert "⚠️ am Beispieltermin ab ~2180 €" in msg_warn
    assert "Positionierung: HAM–CDG (+/- 24h Stopover)" in msg_warn

    # 3. Fail-Open bei Google-Blockade / Exception -> kein Crash, keine Live-Check-Zeile
    def failing_fetcher(_q):
        raise RuntimeError("Consent wall / blocked")

    live_fail = run_google_flights_live_check(
        deal.model_copy(), home="HAM", reference_date=ref_today, price_fetcher=failing_fetcher
    )
    assert live_fail.main_price_eur is None
    assert live_fail.positioning_price_eur is None
    msg_fail = build_telegram_message(deal, "Paris -> Colombo", "https://example.com/deal", live_check=live_fail)
    assert "Google Flights Live-Check" not in msg_fail

    # 4. Bei bevorzugtem Hub (FRA) wird kein Positionierungsflug abgefragt
    deal_fra = deal.model_copy(update={"iata_origin": "FRA", "iata_return_end": "FRA"})
    pos_calls: list[str] = []

    def hub_fetcher(q):
        pos_calls.append(q.get_seat_type())
        return 1450

    live_fra = run_google_flights_live_check(deal_fra, home="HAM", reference_date=ref_today, price_fetcher=hub_fetcher)
    assert live_fra.main_price_eur == 1450
    assert live_fra.positioning_price_eur is None
    assert pos_calls == ["business"]


def test_additional_and_priority_destinations():
    # 1. Normalisierung entfernt Hauptziel, Duplikate, Platzhalter und kappt auf max. 6 Einträge
    cleaned = normalize_additional_destinations(
        [
            "KUL (~1.820 €)",
            "BKK (~1.785 €)",
            "SIN (~1.850 €)",
            "Kuala Lumpur (KUL)",
            "none",
            "-",
            "MNL (~1.890 €)",
            "CGK (~1.860 €)",
            "SGN (~1.870 €)",
            "HKT (~1.810 €)",
            "HAN (~1.900 €)",
            "DPS (~1.920 €)",
        ],
        primary_dest="BKK",
    )
    assert cleaned == [
        "KUL (~1.820 €)",
        "SIN (~1.850 €)",
        "MNL (~1.890 €)",
        "CGK (~1.860 €)",
        "SGN (~1.870 €)",
        "HKT (~1.810 €)",
    ]

    # 2. evaluate_with_gemini enthält Wunschziele & Sammel-Deal-Regeln im Prompt und bereinigt additional_destinations
    deal_kul = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="China Southern Business Class ab Amsterdam nach Asien",
        is_relevant=True,
        deal_category="Top-Bordprodukt",
        airline_names="China Southern (CZ)",
        primary_airline_iata="CZ",
        iata_origin="AMS",
        iata_via=["CAN"],
        iata_destination="KUL",
        additional_destinations=["KUL (~1.820 €)", "BKK (~1.785 €)", "SIN (~1.850 €)"],
        is_open_jaw=False,
        iata_return_end="AMS",
        region="Asien",
        sample_outbound_date="2027-02-10",
        sample_return_date="2027-02-24",
        price_eur=1820,
        booking_class="I",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="1-2-1 Lie-Flat A350",
        routing_advice="",
        rejection_reason="",
    )

    captured_prompts: list[str] = []
    mock_resp = MagicMock()
    mock_resp.parsed = deal_kul

    def fake_gen(client, prompt, **kwargs):
        captured_prompts.append(prompt)
        return mock_resp

    with (
        patch.dict(CONFIG["destinations"], {"priority_destinations": ["MNL", "KUL"]}),
        patch("bot.generate_with_gemini_fallback", side_effect=fake_gen),
    ):
        evaluated = evaluate_with_gemini(
            MagicMock(),
            "China Southern Business Class ab Amsterdam nach Asien ab 1.785 €",
            "Business Class ab AMS nach Bangkok (1.785 €), Kuala Lumpur (1.820 €) und Singapur (1.850 €).",
        )
        msg_priority = build_telegram_message(evaluated, "China Southern AMS -> Asien", "https://example.com/cz-sale")

    assert len(captured_prompts) == 1
    assert "WUNSCHZIELE BEI SAMMEL-DEALS" in captured_prompts[0]
    assert "MNL, KUL" in captured_prompts[0]
    assert "additional_destinations" in captured_prompts[0]
    assert evaluated.additional_destinations == ["BKK (~1.785 €)", "SIN (~1.850 €)"]
    assert "✈️ <b>Top-Bordprodukt ⭐</b>" in msg_priority
    assert "• <b>Weitere Ziele im Sale:</b> BKK (~1.785 €), SIN (~1.850 €)" in msg_priority

    # 3. Normaler Einzelziel-Deal ohne Wunschziel hat weder ⭐ noch 'Weitere Ziele im Sale'
    deal_bkk = deal_kul.model_copy(update={"iata_destination": "BKK", "additional_destinations": []})
    with patch.dict(CONFIG["destinations"], {"priority_destinations": ["MNL", "KUL"]}):
        msg_normal = build_telegram_message(deal_bkk, "China Southern AMS -> BKK", "https://example.com/cz-bkk")
    assert "⭐" not in msg_normal
    assert "Weitere Ziele im Sale" not in msg_normal


def test_deterministic_airbag_regions_origins_and_prices():
    base_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Cathay Pacific Business Class ab Frankfurt nach Hongkong",
        is_relevant=True,
        deal_category="Top-Bordprodukt (Oneworld)",
        airline_names="Cathay Pacific (CX)",
        primary_airline_iata="CX",
        iata_origin="FRA",
        iata_via=[],
        iata_destination="HKG",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2027-03-10",
        sample_return_date="2027-03-22",
        price_eur=2509,
        booking_class="I",
        mm_points=0,
        mm_qualifying_points=0,
        cabin_assessment="1-2-1 Aria Suite A350",
        routing_advice="",
        rejection_reason="",
    )

    # 1. Home-Hub FRA bis 2.600 € erlaubt, darüber abgelehnt
    ok_fra, _ = validate_deal_safety(base_deal, "Cathay Business ab Frankfurt", "Ab FRA nach HKG für 2.509 €")
    assert ok_fra is True
    ok_fra_over, reason_fra_over = validate_deal_safety(
        base_deal.model_copy(update={"price_eur": 2650}),
        "Cathay Business ab Frankfurt",
        "Ab FRA nach HKG für 2.650 €",
    )
    assert ok_fra_over is False
    assert "2600" in reason_fra_over

    # 2. CPH ist nicht mehr in preferred_hubs -> Ex-EU-Limit (2.000 €) greift!
    cph_ok = base_deal.model_copy(update={"iata_origin": "CPH", "iata_return_end": "CPH", "price_eur": 1980})
    ok_cph, _ = validate_deal_safety(cph_ok, "Business ab Kopenhagen", "Ab Kopenhagen (CPH) nach HKG für 1.980 €")
    assert ok_cph is True
    ok_cph_over, reason_cph_over = validate_deal_safety(
        cph_ok.model_copy(update={"price_eur": 2099}),
        "Business ab Kopenhagen",
        "Ab Kopenhagen (CPH) nach HKG für 2.099 €",
    )
    assert ok_cph_over is False
    assert "2000" in reason_cph_over

    # 3. Regression PAR-SEZ-KQ: Seychellen (SEZ) werden auch dann geblockt, wenn das Modell region='Asien' setzt
    deal_sez = base_deal.model_copy(
        update={"iata_origin": "CDG", "iata_return_end": "CDG", "iata_destination": "SEZ", "price_eur": 1860}
    )
    ok_sez, reason_sez = validate_deal_safety(deal_sez, "Business Paris Seychellen", "Ab Paris (CDG) nach SEZ")
    assert ok_sez is False
    assert "SEZ" in reason_sez

    # 4. Regression HAM-DAM-EW: Nahost außerhalb von sweet_spots (DAM) wird strikt abgelehnt
    deal_dam = base_deal.model_copy(
        update={
            "iata_origin": "HAM",
            "iata_return_end": "HAM",
            "iata_destination": "DAM",
            "region": "Nahost",
            "price_eur": 700,
        }
    )
    ok_dam, reason_dam = validate_deal_safety(deal_dam, "Eurowings Business ab Hamburg", "Ab HAM nach Damaskus (DAM)")
    assert ok_dam is False
    assert "Sweet-Spots" in reason_dam

    # 5. Georgien (TBS) in sweet_spots ist auch bei günstigem Kurz-/Mittelstreckenpreis (540 €) erlaubt, < 280 € jedoch gesperrt
    deal_tbs = base_deal.model_copy(
        update={
            "iata_origin": "FRA",
            "iata_return_end": "FRA",
            "iata_destination": "TBS",
            "region": "Zentralasien",
            "price_eur": 540,
        }
    )
    ok_tbs, _ = validate_deal_safety(deal_tbs, "Lufthansa Business nach Tiflis", "Ab Frankfurt (FRA) nach Tiflis (TBS)")
    assert ok_tbs is True
    ok_tbs_eco, _ = validate_deal_safety(
        deal_tbs.model_copy(update={"price_eur": 199}),
        "Flug nach Tiflis",
        "Ab Frankfurt (FRA) nach Tiflis (TBS)",
    )
    assert ok_tbs_eco is False

    # 6. Nicht-Europa-Origins (z.B. CAI, PEK oder unbekannter Code mit is_origin_in_europe=False) werden geblockt
    deal_cai = base_deal.model_copy(update={"iata_origin": "CAI", "iata_return_end": "CAI", "price_eur": 1200})
    ok_cai, reason_cai = validate_deal_safety(deal_cai, "Business ex Cairo", "Business Class from CAI to HKG")
    assert ok_cai is False
    assert "außerhalb Europas" in reason_cai

    deal_unknown_non_eu = base_deal.model_copy(
        update={"iata_origin": "CBR", "iata_return_end": "CBR", "price_eur": 1500, "is_origin_in_europe": False}
    )
    ok_cbr, _ = validate_deal_safety(deal_unknown_non_eu, "Business ex Canberra", "Business Class from CBR to HKG")
    assert ok_cbr is False

    # 7. Preis 0 € (Übersichtsartikel) wird geblockt
    ok_zero, _ = validate_deal_safety(
        base_deal.model_copy(update={"price_eur": 0}), "Übersicht", "Business Class ab FRA"
    )
    assert ok_zero is False


def test_seen_urls_reevaluation_forum_guard_and_upsert():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        test_db = tmp.name

    try:
        # Simuliere Alt-Datenbank ohne last_seen_at / entry_published_at und prüfe automatische Migration
        raw_conn = sqlite3.connect(test_db)
        raw_conn.execute(
            "CREATE TABLE seen_urls (url_clean TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        raw_conn.execute(
            "INSERT INTO seen_urls (url_clean, created_at) VALUES (?, ?)",
            ("https://reisetopia.de/deals/finnair-business-class-angebote-deutschland-seoul/", "2026-11-02 10:58:07"),
        )
        raw_conn.commit()
        raw_conn.close()

        conn = init_db(test_db)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(seen_urls)").fetchall()}
        assert {"url_clean", "created_at", "last_seen_at", "entry_published_at"}.issubset(cols)

        reisetopia_url = "https://reisetopia.de/deals/finnair-business-class-angebote-deutschland-seoul/"
        ref_now = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)

        # 1. Recycelter Artikel mit frischem RSS-Datum (22 Tage neuer & innerhalb 72h) -> MUSS re-evaluiert werden!
        fresh_entry = {"published_parsed": time.strptime("2026-11-24 08:30:00", "%Y-%m-%d %H:%M:%S")}
        pub_utc = extract_entry_published_utc(fresh_entry)
        assert pub_utc == "2026-11-24 08:30:00"
        assert (
            should_evaluate_url(
                conn,
                reisetopia_url,
                entry_published_utc=pub_utc,
                feed_url="https://reisetopia.de/feed/",
                now_utc=ref_now,
            )
            is True
        )

        # 2. upsert_seen_url aktualisiert bestehenden Eintrag ohne UNIQUE-Constraint-Fehler; direkter Zweitlauf überspringt
        upsert_seen_url(conn, reisetopia_url, pub_utc)
        assert (
            should_evaluate_url(
                conn,
                reisetopia_url,
                entry_published_utc=pub_utc,
                feed_url="https://reisetopia.de/feed/",
                now_utc=ref_now,
            )
            is False
        )

        # 3. Rollout-Schutz: Veraltetes Update (> 72h alt) wird NICHT re-evaluiert
        stale_url = "https://reisetopia.de/deals/old-recycled-deal/"
        conn.execute(
            "INSERT INTO seen_urls (url_clean, created_at) VALUES (?, ?)",
            (stale_url, "2026-10-31 12:00:00"),
        )
        conn.commit()
        assert (
            should_evaluate_url(
                conn,
                stale_url,
                entry_published_utc="2026-11-10 09:00:00",
                feed_url="https://reisetopia.de/feed/",
                now_utc=ref_now,
            )
            is False
        )

        # 4. Foren-Schutz: Vielfliegertreff-Threads werden bei neuen Antworten NICHT re-evaluiert
        vft_url = "https://www.vielfliegertreff.de/forum/threads/test-deal.167000/"
        upsert_seen_url(conn, vft_url, "2026-11-01 10:00:00")
        assert (
            should_evaluate_url(
                conn,
                vft_url,
                entry_published_utc="2026-11-24 11:00:00",
                feed_url="https://www.vielfliegertreff.de/forum/forums/53/index.rss",
                now_utc=ref_now,
            )
            is False
        )

        # 5. cleanup_db nutzt COALESCE(last_seen_at, created_at): kürzlich aktualisierte Alt-URL bleibt erhalten
        conn.execute(
            "INSERT INTO seen_urls (url_clean, created_at, last_seen_at) VALUES (?, datetime('now', '-120 days'), datetime('now', '-5 days'))",
            ("https://reisetopia.de/deals/evergreen-deal/",),
        )
        conn.commit()
        cleanup_db(conn)
        kept = conn.execute(
            "SELECT COUNT(*) FROM seen_urls WHERE url_clean = 'https://reisetopia.de/deals/evergreen-deal/'"
        ).fetchone()[0]
        assert kept == 1
        close_db(conn)
    finally:
        if os.path.exists(test_db):
            os.remove(test_db)


def test_url_path_segment_pre_filter_and_newsletter_normalization():
    # 1. Newsletter-Tracking (__s, vgo_ee) & Host-Casing werden sauber bereinigt
    nl_url = (
        "HTTPS://Reisetopia.de/deals/cathay-pacific-business-class-angebot-hongkong/?__s=abc&utm_source=drip&vgo_ee=xyz"
    )
    assert normalize_url(nl_url) == "https://reisetopia.de/deals/cathay-pacific-business-class-angebot-hongkong/"

    # 2. Redaktionelle Pfade werden selbst mit 'Business Class' im Titel sofort verworfen
    for blocked_url in (
        "https://reisetopia.de/news/eurowings-naher-osten/",
        "https://reisetopia.de/reviews/lufthansa-business-class-kurz-und-mittelstrecke/",
        "https://reisetopia.de/guides/amex-business-platinum-guthaben/",
        "https://reisetopia.de/gutscheine/qatar-airways-gutschein-angebote/",
        "https://reisetopia.de/kolumne/business-class-kleinraummaschinen/",
        "https://reisetopia.de/stories/singapore-airlines-neue-business-class-erste-eindruecke/",
        "https://travel-dealz.de/blog/bewertung-cathay-aria-suite/",
    ):
        ok, reason = is_feed_entry_eligible_pre_filter(
            "Neue Business Class vorgestellt",
            "Alle Infos zur Business Class.",
            "https://reisetopia.de/feed/",
            entry_url=blocked_url,
        )
        assert ok is False
        assert "Redaktioneller URL-Pfad" in reason

    # 3. Echte Deal-Pfade (auch mit Substring 'news' im Slug oder '/ticker/') bleiben erlaubt
    for allowed_url in (
        "https://reisetopia.de/deals/news-flash-sale-business-class/",
        "https://travel-dealz.de/ticker/lufthansa-business-class-bangkok/",
    ):
        ok, _ = is_feed_entry_eligible_pre_filter(
            "Lufthansa Business Class Sale nach Bangkok",
            "Business Class ab 1.799 Euro.",
            "https://reisetopia.de/feed/",
            entry_url=allowed_url,
        )
        assert ok is True

    # 4. Neue Finanz-/Redaktions-Keywords greifen hart
    ok_fin, _ = is_feed_entry_eligible_pre_filter(
        "Festgeld Zinsen sichern & Business Class fliegen",
        "Business Class",
        "https://reisetopia.de/feed/",
        entry_url="https://reisetopia.de/deals/festgeld-aktion/",
    )
    assert ok_fin is False


def test_afc_config_emits_no_sdk_warning(caplog):
    # 1. Echtes Gemini-API-Schema-Transform prüfen (fängt 'Default value is not supported in the response schema' ohne Mock ab!)
    client = create_genai_client(api_key="dummy-key")
    transformed_schema = _transformers.t_schema(client._api_client, FlightDealEvaluation)
    assert "is_origin_in_europe" in transformed_schema.required
    assert "iata_via" in transformed_schema.required
    assert "additional_destinations" in transformed_schema.required

    # 2. Keine AFC-SDK-Warnung beim Bauen der GenerateContentConfig
    with caplog.at_level(logging.WARNING):
        cfg = types.GenerateContentConfig(
            temperature=0.1,
            automatic_function_calling={"disable": True, "maximum_remote_calls": None},
        )
        assert _extra_utils.should_disable_afc(cfg) is True
    assert not any("automatic_function_calling" in rec.message for rec in caplog.records)


def test_gf_html_price_extractor_handles_none_metadata_and_best_flights():
    # Simuliert Google-Flights-HTML, bei dem fast_flights.parser.parse mit
    # "'NoneType' object is not subscriptable" abstürzt (payload[7] = None, extras = None,
    # einzelne Flüge ohne Preis) und Preise in 'Beste Flüge' (payload[2][0]) stehen.
    payload_best_only = [
        None,
        None,
        [
            [
                [["LO", ["LOT"], [], None, None], [[None, 168]]],
                [["LH", ["Lufthansa"], [], None, None], [[None, 219]]],
            ]
        ],
        [
            [
                [["EW", ["Eurowings"], []], [None]],  # fehlender Preisblock
                [["LO", ["LOT"], []], [[None, 189]]],
            ]
        ],
        None,
        None,
        None,
        None,  # payload[7] = None -> lässt fast_flights.parser.parse abstürzen!
    ]
    import json as _json

    html_doc = (
        '<html><head><script class="ds:1">'
        f"AF_initDataCallback({{key: 'ds:1', hash: '1', data:{_json.dumps(payload_best_only)}, sideChannel: {{}}}});"
        "</script></head></html>"
    )
    assert extract_gf_lowest_price_from_html(html_doc) == 168

    # Fehler-Status liefert sauber None statt Exception
    err_html = '<html><head><script class="ds:1">AF_initDataCallback({data:null, errorHasStatus: true});</script></head></html>'
    assert extract_gf_lowest_price_from_html(err_html) is None


def test_send_telegram_retries_and_no_silent_drop():
    deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="SWISS Business Class ab Frankfurt nach Bangkok",
        is_relevant=True,
        deal_category="FTL-Booster (Points + QP)",
        airline_names="SWISS (LX)",
        primary_airline_iata="LX",
        iata_origin="FRA",
        iata_via=["ZRH"],
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="FRA",
        region="Asien",
        sample_outbound_date="2027-02-10",
        sample_return_date="2027-02-24",
        price_eur=1790,
        booking_class="P",
        mm_points=480,
        mm_qualifying_points=480,
        cabin_assessment="X" * 300,
        routing_advice="Auch ab MUC buchbar. " + ("Detail " * 100),
        rejection_reason="",
    )

    # 1. Pre-Escape-Kappung hält Nachricht kompakt & HTML-Tags intakt
    msg = build_telegram_message(deal, "T" * 400, "https://example.com/deal")
    assert len(msg) < 4096
    assert msg.count("<b>") == msg.count("</b>")
    assert "…" in msg

    # 2. 4x HTTP 429 wirft Exception (kein stiller Deal-Verlust!)
    resp_429 = MagicMock()
    resp_429.status_code = 429
    resp_429.json.return_value = {"parameters": {"retry_after": 1}}
    resp_429.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
    sess_429 = MagicMock()
    sess_429.post.return_value = resp_429

    with patch("bot.interruptible_sleep", return_value=False):
        try:
            send_telegram(deal, "Test Deal", "https://example.com/deal", session=sess_429)
            assert False, "Expected HTTPError after repeated 429"
        except requests.HTTPError:
            pass
    assert sess_429.post.call_count == 4

    # 3. Transientes HTTP 503 -> 200 wird sauber wiederholt
    resp_503 = MagicMock()
    resp_503.status_code = 503
    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.raise_for_status.return_value = None
    sess_503 = MagicMock()
    sess_503.post.side_effect = [resp_503, resp_200]

    with patch("bot.interruptible_sleep", return_value=False):
        send_telegram(deal, "Test Deal", "https://example.com/deal", session=sess_503)
    assert sess_503.post.call_count == 2

    # 4. ReadTimeout (nicht idempotent) macht maximal 1 Retry und wirft dann
    sess_timeout = MagicMock()
    sess_timeout.post.side_effect = requests.exceptions.ReadTimeout("timed out")
    with patch("bot.interruptible_sleep", return_value=False):
        try:
            send_telegram(deal, "Test Deal", "https://example.com/deal", session=sess_timeout)
            assert False, "Expected ReadTimeout"
        except requests.exceptions.ReadTimeout:
            pass
    assert sess_timeout.post.call_count == 2

    # 5. Shutdown-Event bricht Retry sofort ab
    stop_evt = threading.Event()
    stop_evt.set()
    sess_stop = MagicMock()
    sess_stop.post.return_value = resp_503
    try:
        send_telegram(deal, "Test Deal", "https://example.com/deal", session=sess_stop, stop_event=stop_evt)
        assert False, "Expected RuntimeError on shutdown"
    except RuntimeError as exc:
        assert "Shutdown" in str(exc)


def test_origin_hijacking_word_boundaries_and_german_compounds():
    base_deal = FlightDealEvaluation(
        detected_cabin_class="Business Class",
        cabin_class_quote="Air China Business Class nach Bangkok",
        is_relevant=True,
        deal_category="Star Alliance (Nur Points)",
        airline_names="Air China (CA)",
        primary_airline_iata="CA",
        iata_origin="BER",
        iata_destination="BKK",
        is_open_jaw=False,
        iata_return_end="BER",
        region="Asien",
        sample_outbound_date="2027-02-10",
        sample_return_date="2027-02-24",
        price_eur=1650,
        booking_class="R",
        mm_points=400,
        mm_qualifying_points=0,
        cabin_assessment="Lie-Flat",
        routing_advice="",
        rejection_reason="",
    )

    # BER darf NICHT durch 'über' / 'Oktober' / 'November' durchgewunken werden
    ok_ber, reason_ber = validate_deal_safety(
        base_deal,
        "Business Class über Warschau im Oktober",
        "Flüge ab Warschau (WAW) nach Bangkok im Oktober und November.",
    )
    assert ok_ber is False
    assert "Origin-Hijacking" in reason_ber

    # FRA darf NICHT durch 'Frage' oder 'Frankreich' durchgewunken werden
    deal_fra = base_deal.model_copy(update={"iata_origin": "FRA", "iata_return_end": "FRA"})
    ok_fra_false, _ = validate_deal_safety(
        deal_fra,
        "Frage zu Deal ab Frankreich",
        "Flüge ab Paris (CDG) nach Bangkok.",
    )
    assert ok_fra_false is False

    # FCO darf NICHT durch 'FROM' (wegen Alias 'ROM') durchgewunken werden
    deal_fco = base_deal.model_copy(update={"iata_origin": "FCO", "iata_return_end": "FCO"})
    ok_fco_false, _ = validate_deal_safety(
        deal_fco,
        "Business Class from Warsaw to Bangkok",
        "Great promo flights from WAW to BKK.",
    )
    assert ok_fco_false is False

    # Deutsche Komposita/Adjektive ('Frankfurter', 'Hamburger') bleiben erlaubt
    ok_fra_compound, _ = validate_deal_safety(
        deal_fra,
        "Frankfurter Flughafen Business Class nach Bangkok",
        "Abflug vom Frankfurter Flughafen nach Bangkok für 1.650 Euro.",
    )
    assert ok_fra_compound is True

    deal_ham = base_deal.model_copy(update={"iata_origin": "HAM", "iata_return_end": "HAM"})
    ok_ham_compound, _ = validate_deal_safety(
        deal_ham,
        "Hamburger Abflüge in Business Class nach Bangkok",
        "Attraktive Business Class Tarife für Hamburger Reisende nach Bangkok.",
    )
    assert ok_ham_compound is True


def test_gemini_model_cooldown_and_interruptible_shutdown():
    cooldowns: dict[str, float] = {}
    called_models: list[str] = []
    mock_client = MagicMock()

    def side_effect(model, contents, config):
        called_models.append(model)
        if model == "gemini-3.8-flash":
            raise RuntimeError("404 NOT_FOUND: model not available")
        resp = MagicMock()
        resp.text = "ok"
        return resp

    mock_client.models.generate_content.side_effect = side_effect

    # 1. Erster Aufruf: gemini-3.8-flash liefert 404 -> Cooldown gesetzt, Fallback auf gemini-flash-latest
    r1 = generate_with_gemini_fallback(
        client=mock_client,
        prompt="Test 1",
        preferred_model="gemini-3.8-flash",
        model_cooldowns=cooldowns,
    )
    assert r1.text == "ok"
    assert called_models == ["gemini-3.8-flash", "gemini-flash-latest"]
    assert "gemini-3.8-flash" in cooldowns

    # 2. Zweiter Aufruf: gemini-3.8-flash wird sofort übersprungen!
    called_models.clear()
    r2 = generate_with_gemini_fallback(
        client=mock_client,
        prompt="Test 2",
        preferred_model="gemini-3.8-flash",
        model_cooldowns=cooldowns,
    )
    assert r2.text == "ok"
    assert called_models == ["gemini-flash-latest"]

    # 3. clear_gemini_model_cooldowns leert den Cache sauber
    clear_gemini_model_cooldowns(cooldowns)
    assert cooldowns == {}

    # 4. Shutdown bricht Backoff-Sleep sofort ab
    stop_evt = threading.Event()
    stop_evt.set()
    mock_client_503 = MagicMock()
    mock_client_503.models.generate_content.side_effect = RuntimeError("503 UNAVAILABLE")
    try:
        generate_with_gemini_fallback(
            client=mock_client_503,
            prompt="Test 3",
            preferred_model="gemini-3.8-flash",
            model_cooldowns={},
            stop_event=stop_evt,
        )
        assert False, "Expected RuntimeError on shutdown"
    except RuntimeError as exc:
        assert "Shutdown" in str(exc)


def test_enriched_deal_context_skips_unnecessary_scraping():
    long_text = "Business Class Deal ab Frankfurt nach Bangkok. " * 45  # > 1800 Zeichen ohne OTA-Link
    forum_text = "Vielfliegertreff Business Class Tarifdetails ab Amsterdam nach Tokio. " * 20  # > 1200 Zeichen
    short_text = "Kurzer Teaser Business Class ab FRA."

    with patch("bot.fetch_article_html", return_value="<html><body>Full page</body></html>") as mock_fetch:
        text_long, _ = get_enriched_deal_context(long_text, "https://example.com/deal/long")
        assert len(text_long) >= 1800
        assert mock_fetch.call_count == 0

        text_forum, _ = get_enriched_deal_context(
            forum_text, "https://www.vielfliegertreff.de/forum/threads/test.12345/"
        )
        assert len(text_forum) >= 1200
        assert mock_fetch.call_count == 0

        get_enriched_deal_context(short_text, "https://example.com/deal/short")
        assert mock_fetch.call_count == 1


def test_fetch_all_feeds_etag_304_and_health_logging(caplog):
    feed_url = "https://example.com/business-class/feed/"
    rss_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<rss version='2.0'><channel><title>Test</title>"
        "<item><title>SWISS Business Class BKK</title><link>https://example.com/deal-1</link></item>"
        "</channel></rss>"
    )
    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.text = rss_xml
    resp_200.headers = {"ETag": '"etag-123"', "Last-Modified": "Fri, 27 Nov 2026 10:00:00 GMT"}

    resp_304 = MagicMock()
    resp_304.status_code = 304
    resp_304.text = ""
    resp_304.headers = {}

    seen_headers: list[dict] = []

    def fake_get(url, headers=None, timeout=15):
        seen_headers.append(dict(headers or {}))
        return resp_200 if len(seen_headers) == 1 else resp_304

    mock_session = MagicMock()
    mock_session.get.side_effect = fake_get
    cache: dict[str, dict[str, str]] = {}

    with caplog.at_level(logging.INFO):
        res1 = fetch_all_feeds([feed_url], max_workers=1, session=mock_session, http_cache=cache)
        assert len(res1) == 1 and len(res1[0][1]) == 1

        caplog.clear()
        res2 = fetch_all_feeds([feed_url], max_workers=1, session=mock_session, http_cache=cache)
        assert res2 == []

    assert seen_headers[1].get("If-None-Match") == '"etag-123"'
    assert seen_headers[1].get("If-Modified-Since") == "Fri, 27 Nov 2026 10:00:00 GMT"
    assert any("Feed-Scan erfolgreich: 1/1 Feeds aktiv" in r.message and "304" in r.message for r in caplog.records)
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_main_cycle_error_recovers_with_backoff():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "deals.db")
        hb_path = os.path.join(tmpdir, "heartbeat")
        observed_waits: list[int] = []

        def fake_wait(total_seconds, *args, **kwargs) -> bool:
            observed_waits.append(total_seconds)
            return True  # Nach dem ersten Backoff geordnet beenden

        with (
            patch("bot.DB_PATH", db_path),
            patch("bot.HEARTBEAT_FILE", hb_path),
            patch("bot.validate_startup"),
            patch("bot.create_genai_client", return_value=MagicMock()),
            patch("bot.fetch_all_feeds", side_effect=RuntimeError("Simulierter Netzwerk-Crash")),
            patch("bot.wait_for_next_cycle", side_effect=fake_wait),
            patch("bot.STOP_EVENT", threading.Event()),
        ):
            main()

        assert observed_waits == [60]
        assert not os.path.exists(f"{db_path}-wal")
        assert not os.path.exists(f"{db_path}-shm")
