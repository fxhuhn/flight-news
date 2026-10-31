# ✈️ Flight News Bot

Automatisierter Telegram-Bot zur Erkennung, Bewertung und Benachrichtigung über außergewöhnliche Business- und First-Class-Flugangebote mit Google Gemini KI und Fokus auf das Miles & More Vielfliegerprogramm.

---

## 🌟 Features

* **Multi-Feed RSS Monitoring:** Überwacht kontinuierlich spezialisierte Flugdeal-Portale und Vielflieger-Foren.
* **Intelligente Vorfilterung:** Schnelle Signalwort- und Airline-Ausschluss-Filterung vor externen KI-Aufrufen zur Kosten- und Effizienzoptimierung.
* **KI-Deal-Bewertung (Google Gemini):** Strukturierte Extraktion von Reiseklasse, Routing, Preisen und Buchungsklassen via Pydantic Schemas.
* **Miles & More Points & QP-Berechnung:** Automatische Identifikation von FTL/Senator-Boostern (z. B. LOT Polish Airlines, Austrian, SWISS und Nahost-Sweet-Spots mit 200 Points + 200 QP).
* **Intelligente Deduplizierung:** SQLite-basiertes Fingerprinting mit Preissensitivität (unterdrückt Dubletten, lässt echte Preisstürze ab 75 € durch).
* **Google Flights Deep-Links:** Generiert automatisch direkte Buchungslinks für Hin- und Rückflug, Open-Jaw sowie optionale Positionierungsflüge ab dem Heimathafen (z. B. HAM).
* **Enterprise Security & CI/CD:** Vollautomatisierte GitHub Actions Workflows für CodeQL, Bandit, Gitleaks, pip-audit, Pytest und Docker-Container-Builds mit Trivy Vulnerability Scanning.

---

## 🛠️ Schnellstart & Lokale Ausführung

### 1. Repository klonen & Virtual Environment erstellen
```bash
git clone https://github.com/fxhuhn/flight-news.git
cd flight-news
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Konfiguration anlegen
Kopiere `.env.example` nach `.env` und trage deine API-Schlüssel ein:
```bash
cp .env.example .env
```

Passe optional die Abflughäfen und Schwellenwerte in `config.toml` an:
```toml
[origin]
home_airport = "HAM"
preferred_hubs = ["HAM", "FRA", "BER", "CPH"]
```

### 3. Bot starten
```bash
python bot.py
```

---

## 🐳 Docker Deployment

### Docker Compose
```bash
docker compose up -d --build
```

### GitHub Container Registry (GHCR)
Das Container-Image wird bei jedem Release automatisch über die GitHub Actions Pipeline gebaut, auf Sicherheitslücken gescannt und bereitgestellt:
```bash
docker pull ghcr.io/fxhuhn/flight-news:main
```

---

## 🧪 Tests

```bash
# Offline-Unit-Tests ausführen (keine API-Keys erforderlich)
pip install -r requirements-dev.txt
pytest tests/ -v
```

---

## 🛡️ Sicherheit & Datenschutz
* **Zero Data Leakage:** Vertrauliche Token (`.env`), Datenbanken (`data/deals.db`) und lokale Caches werden über strikte `.gitignore`- und `.dockerignore`-Regeln geschützt.
* **Gitleaks Scan:** Verhindert das versehentliche Committen von API-Keys sowohl lokal (Pre-Commit-Hook) als auch in der GitHub CI-Pipeline.
* **Non-Root Container:** Der Container läuft standardmäßig unter dem eingeschränkten Benutzer `appuser` (UID 1000).
