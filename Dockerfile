FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Berlin

# Installation von tzdata für korrekte Zeitzonen-Logs im Container
RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Non-Root User für sicheren Betrieb auf NAS / Server
RUN useradd -m -u 1000 appuser && mkdir -p /app/data && chown -R appuser:appuser /app

COPY --chown=appuser:appuser bot.py config.toml ./

USER appuser

VOLUME ["/app/data"]

CMD ["python", "bot.py"]
