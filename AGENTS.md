# Project Guardrails (flight-news)

## 1. Externe SDK-Schemas & Contract-Tests (Pflicht vor Commit)
- Bei jeder Änderung an Pydantic-Modellen für `response_schema` (z. B. `FlightDealEvaluation`) oder an `GenerateContentConfig` reichen gemockte `generate_content`-Tests **nicht** aus.
- Vor jedem Commit muss ein ungepatchter Unit-Test gegen die echte, in `requirements.txt` gepinnte `google-genai`-SDK-Validierung laufen (insbesondere `google.genai._transformers.t_schema(client._api_client, FlightDealEvaluation)` und `_extra_utils.should_disable_afc(cfg)`).
- Im Gemini-`response_schema` niemals `Field(default=...)` verwenden (erzeugt `"default"` im JSON-Schema und führt in `google-genai==1.2.0` zu `ValueError`). Stattdessen `default_factory=...` nutzen und `"default"`-Keys im Schema-Hook (`_make_iata_via_required`) strippen.

## 2. Aufgabenteilung LLM vs. Python
- Das LLM (`flash-lite`) extrahiert strukturierte Fakten (Route, Preis, Airline, Kabine, Region).
- Harte Ausschluss- und Zulassungsregeln (Preislimits, Zielregionen, `sweet_spots`, Europa-Origin, Kabinen-Nachweis) müssen **immer deterministisch in Python** (`validate_deal_safety`) durchgesetzt werden – niemals ausschließlich über Prompt-Text.

## 3. SQLite-Migrationen, Deduplizierung & Deployment
- Neue Spalten in `seen_urls` / `sent_deals` immer per abwärtskompatibler `ALTER TABLE`-Migration einführen und Bestandszeilen mit `NULL`-Werten explizit im Test abdecken.
- Alle Schreibzugriffe auf `seen_urls` über den zentralen Helper `upsert_seen_url()` (`ON CONFLICT DO UPDATE`) führen – keine rohen Einzel-`INSERT`s.
- Bei Änderungen an `config.toml` prüfen, ob `docker-compose.yml` die Datei als Host-Mount einbindet, und bei Bedarf kurz auf das Nachziehen auf dem Server hinweisen.
