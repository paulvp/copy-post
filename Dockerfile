FROM python:3.11-alpine AS builder

RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    postgresql-dev \
    g++

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM python:3.11-alpine

RUN apk add --no-cache \
    libpq \
    libffi

WORKDIR /app

COPY --from=builder /root/.local /root/.local

COPY bot.py copybot.py ./

RUN mkdir -p /app/sessions

ENV PYTHONUNBUFFERED=1 \
    PATH=/root/.local/bin:$PATH

CMD ["python", "-u", "bot.py"]
