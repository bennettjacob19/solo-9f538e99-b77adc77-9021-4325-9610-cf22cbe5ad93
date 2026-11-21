FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000 8080

# The compose file overrides this command per service.
CMD ["uvicorn", "app.origin.main:app", "--host", "0.0.0.0", "--port", "8080"]
