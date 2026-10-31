import os
import tempfile

from bot import (
    IGNORED_AIRLINES,
    FlightDealEvaluation,
    build_gf_main_url,
    build_positioning_url,
    build_telegram_message,
    clean_html,
    init_db,
    is_airline_ignored_in_text,
    is_deal_ignored_airline,
    is_feed_entry_eligible_pre_filter,
    normalize_and_verify_deal_mm_points,
    normalize_url,
    record_sent_deal,
    should_send_deal,
    validate_deal_safety,
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

    deal_redundant = deal_base.model_copy(
        update={
            "routing_advice": "Ex-EU Abflug ab Prag (PRG). Etihad Airways ist nicht in einer Allianz, daher 0 Miles & More Points/QP."
        }
    )
    msg_red = build_telegram_message(deal_redundant, "Prague -> Beijing", "https://example.com/deal")
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
