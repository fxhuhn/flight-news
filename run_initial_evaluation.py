import logging
import sys
import time

import feedparser
import requests
from google import genai

from bot import (
    GEMINI_API_KEY,
    HEADERS,
    IGNORED_AIRLINES,
    RSS_FEEDS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    clean_html,
    evaluate_with_gemini,
    init_db,
    is_airline_ignored_in_text,
    is_feed_entry_eligible_pre_filter,
    normalize_url,
    send_telegram,
    validate_deal_safety,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    print("=" * 70)
    print("🛫 STARTE INITIALEN PRÜFLAUF (LIVE-FEED AUSWERTUNG & TELEGRAM-PUSH)")
    print("=" * 70)

    if not GEMINI_API_KEY or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Fehler: Umgebungsvariablen nicht vollständig geladen.")
        sys.exit(1)

    conn = init_db()
    c = conn.cursor()
    client = genai.Client(api_key=GEMINI_API_KEY)

    evaluated_count = 0
    relevant_pushed = 0
    max_pushes = 3  # Zeige bis zu 3 echte Deals für die Qualitätskontrolle

    for feed_url in RSS_FEEDS:
        if relevant_pushed >= max_pushes:
            break

        print(f"\n📡 Lese Feed: {feed_url}")
        try:
            resp = requests.get(feed_url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                print(f"  ⚠️ HTTP {resp.status_code} - Überspringe Feed.")
                continue

            feed = feedparser.parse(resp.text)
            print(f"  Gefundene Einträge: {len(feed.entries)}")

            for entry in feed.entries[:10]:
                if relevant_pushed >= max_pushes:
                    break

                raw_link = entry.get("link", "")
                if not raw_link:
                    continue
                clean_link = normalize_url(raw_link)

                title = entry.get("title", "")
                content_list = entry.get("content")
                raw_content = (content_list[0].get("value") if content_list else None) or entry.get("summary") or title

                # Deterministischer Pre-Filter
                is_eligible, pre_reason = is_feed_entry_eligible_pre_filter(title, raw_content, feed_url)
                if not is_eligible:
                    print(f"  ⏭️ Pre-Filter greift bei ({pre_reason}): '{title[:60]}...'")
                    c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                    conn.commit()
                    continue

                # Airline-Ignorier-Filter
                is_ignored, ignored_air = is_airline_ignored_in_text(title, IGNORED_AIRLINES)
                if is_ignored:
                    print(f"  ⏭️ Ignorierte Airline im Titel ('{ignored_air}'): '{title[:60]}...'")
                    c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                    conn.commit()
                    continue

                c.execute("SELECT 1 FROM seen_urls WHERE url_clean = ?", (clean_link,))
                if c.fetchone():
                    print(f"  ⏭️ Bereits gesehen: '{title[:60]}...'")
                    continue

                clean_text = clean_html(raw_content)

                print(f"\n🤖 [KI-Bewertung] Analysiere: '{title}'...")
                evaluated_count += 1

                deal = evaluate_with_gemini(client, title, clean_text)

                # Deterministischer Post-Validation Airbag
                is_safe, safety_veto_reason = validate_deal_safety(deal, title, clean_text)
                if not is_safe:
                    print(f"   🛡️ Sicherheits-Veto: {safety_veto_reason}")
                    deal.is_relevant = False
                    deal.deal_category = "Irrelevant"
                    deal.rejection_reason = safety_veto_reason

                print(f"   -> Relevant: {deal.is_relevant}")
                print(f"   -> Kategorie: {deal.deal_category}")
                print(f"   -> Airline: {deal.airline_names}")
                print(f"   -> Route: {deal.iata_origin} -> {deal.iata_destination} (Gabelflug: {deal.is_open_jaw})")
                print(f"   -> Preis: {deal.price_eur} € (Klasse: {deal.booking_class})")
                print(f"   -> M&M Points: {deal.mm_points} | QP: {deal.mm_qualifying_points}")
                print(f"   -> Bordprodukt: {deal.cabin_assessment}")
                print(f"   -> Routing-Hinweis: {deal.routing_advice}")
                if deal.rejection_reason:
                    print(f"   -> Ablehnungsgrund: {deal.rejection_reason}")

                if deal.is_relevant and deal.deal_category != "Irrelevant":
                    price_bucket = round(deal.price_eur / 100) * 100
                    fingerprint = f"{deal.iata_origin}-{deal.iata_destination}-{price_bucket}"

                    c.execute("SELECT 1 FROM sent_deals WHERE deal_fingerprint = ?", (fingerprint,))
                    if not c.fetchone():
                        print(f"   🚀 Sende Live-Deal an Telegram: {fingerprint}")
                        send_telegram(deal, title, raw_link)
                        c.execute("INSERT INTO sent_deals (deal_fingerprint) VALUES (?)", (fingerprint,))
                        relevant_pushed += 1
                        time.sleep(2)
                    else:
                        print(f"   ℹ️ Fingerprint-Duplikat: {fingerprint}")

                c.execute("INSERT INTO seen_urls (url_clean) VALUES (?)", (clean_link,))
                conn.commit()

        except Exception as e:
            print(f"  ❌ Fehler bei Feed {feed_url}: {e}")

    print("\n" + "=" * 70)
    print("🏁 INITIALER PRÜFLAUF BEENDET:")
    print(f"• Analysierte Artikel mit Gemini: {evaluated_count}")
    print(f"• Erfolgreich an Telegram gepusht: {relevant_pushed}")
    print("=" * 70)


if __name__ == "__main__":
    main()
