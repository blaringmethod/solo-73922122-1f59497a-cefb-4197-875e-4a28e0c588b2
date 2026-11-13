FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000
# A single uvicorn worker is intentional: the one-round / two-in-flight
# guarantees rely on one process and one asyncio event loop.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
