import logging
import time
from datetime import datetime

import feedparser
from google import genai

from bot import (
    GEMINI_API_KEY,
    IGNORED_AIRLINES,
    NEGATIVE_KEYWORDS,
    RSS_FEEDS,
    evaluate_with_gemini,
    fetch_feed_content,
    get_enriched_deal_context,
    init_db,
    is_airline_ignored_in_text,
    normalize_url,
    send_telegram,
    validate_startup,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TARGET_COUNT = 20


def run_batch():
    validate_startup()
    conn = init_db()
    c = conn.cursor()
    client = genai.Client(api_key=GEMINI_API_KEY)

    print("=" * 65)
    print(f"🚀 STARTE INITIALEN LIVE-LAUF FÜR TELEGRAM (Ziel: {TARGET_COUNT} Deals)")
    print(f"Zeitpunkt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    sent_count = 0
    evaluated_count = 0
    skipped_filter_count = 0
    pushed_deals = []

    # 1. Sammle alle Einträge aus allen Feeds
    feeds_entries = []
    print("\n📡 Lade Feeds...")
    for feed_url in RSS_FEEDS:
        try:
            raw_xml = fetch_feed_content(feed_url)
            if not raw_xml:
                continue
            feed = feedparser.parse(raw_xml)
            feeds_entries.append((feed_url, feed.entries))
            print(f"  ✓ {len(feed.entries):2d} Einträge geladen: {feed_url}")
        except Exception as e:
            logging.warning(f"Fehler beim Laden von {feed_url}: {e}")

    # 2. Round-Robin Interleaving: Reihum 1 Eintrag aus jedem Feed
    interleaved_entries = []
    max_entries = max((len(entries) for _, entries in feeds_entries), default=0)
    for idx in range(max_entries):
        for feed_url, entries in feeds_entries:
            if idx < len(entries):
                interleaved_entries.append((entries[idx], feed_url))

    print(f"\nInsgesamt {len(interleaved_entries)} Einträge aus {len(feeds_entries)} Feeds im Round-Robin gemischt.\n")

    # 3. Durchlaufe die ausgewogenen Einträge
    for entry, source_feed in interleaved_entries:
        if sent_count >= TARGET_COUNT:
            break

        try:
            raw_link = entry.get("link", "")
            if not raw_link:
                continue
            clean_link = normalize_url(raw_link)

            # Prüfe ob URL schon gesehen
            c.execute("SELECT 1 FROM seen_urls WHERE url_clean = ?", (clean_link,))
            if c.fetchone():
                continue

            title = entry.get("title", "")

            # Negativ-Keyword Filter
            if any(bad in title.lower() for bad in NEGATIVE_KEYWORDS):
                c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                conn.commit()
                skipped_filter_count += 1
                continue

            # Airline-Ignorier-Filter
            is_ignored, ignored_air = is_airline_ignored_in_text(title, IGNORED_AIRLINES)
            if is_ignored:
                c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                conn.commit()
                skipped_filter_count += 1
                continue

            content_list = entry.get("content")
            raw_content = (content_list[0].get("value") if content_list else None) or entry.get("summary") or title
            full_text, sample_booking = get_enriched_deal_context(raw_content, raw_link)

            evaluated_count += 1
            print(f"[{sent_count + 1}/{TARGET_COUNT}] Analysiere ({evaluated_count}. Check): {title[:70]}...")

            deal = evaluate_with_gemini(client, title, full_text)

            if deal.is_relevant and deal.deal_category != "Irrelevant":
                price_bucket = round(deal.price_eur / 100) * 100
                fingerprint = f"{deal.iata_origin}-{deal.iata_destination}-{price_bucket}"

                c.execute("SELECT 1 FROM sent_deals WHERE deal_fingerprint = ?", (fingerprint,))
                if not c.fetchone():
                    # Telegram Push
                    send_telegram(deal, title, raw_link, sample_booking)
                    c.execute("INSERT INTO sent_deals (deal_fingerprint) VALUES (?)", (fingerprint,))
                    sent_count += 1

                    pushed_deals.append(
                        {
                            "nr": sent_count,
                            "airline": deal.airline_names,
                            "route": f"{deal.iata_origin}->{deal.iata_destination}",
                            "price": f"{deal.price_eur} €",
                            "points": f"{deal.mm_points} P / {deal.mm_qualifying_points} QP",
                            "title": title[:60],
                        }
                    )
                    print(
                        f"  👉 GEPUSHT (#{sent_count}): {deal.iata_origin}->{deal.iata_destination} | {deal.price_eur}€ | {deal.airline_names} | {deal.mm_points}P/{deal.mm_qualifying_points}QP"
                    )
                    time.sleep(2)  # Pause für Telegram Rate-Limits
                else:
                    print(f"  ⏭️ Cross-Feed Duplikat: {fingerprint}")
            else:
                reason = (
                    deal.rejection_reason
                    if getattr(deal, "rejection_reason", None)
                    else f"Kategorie: {deal.deal_category}, Region: {deal.region}"
                )
                print(
                    f"  ❌ Abgelehnt: {deal.iata_origin}->{deal.iata_destination} | {deal.price_eur}€ | {deal.airline_names} | Grund: {reason}"
                )

            # URL als gesehen markieren
            c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
            conn.commit()

            # Höfliche Pause für Gemini API
            time.sleep(1)

        except Exception as err:
            logging.error(f"Fehler bei Eintrag: {err}")
            time.sleep(2)

    print("\n" + "=" * 65)
    print(f"🏁 FERTIG! Insgesamt {sent_count} Deals an Telegram gesendet.")
    print("=" * 65)
    for d in pushed_deals:
        print(f"#{d['nr']:02d} | {d['route']} | {d['price']:>7} | {d['points']:>15} | {d['airline']} ({d['title']})")
    print("=" * 65)


if __name__ == "__main__":
    run_batch()
