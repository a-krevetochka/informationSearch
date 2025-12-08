FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копирование зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY crawler.py .
COPY config.yaml ./config/config.yaml

# Создание пользователя для безопасности
RUN useradd -m -u 1000 crawler && chown -R crawler:crawler /app
USER crawler

CMD ["python", "crawler.py", "/app/config/config.yaml"]