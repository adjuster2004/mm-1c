# Используем slim версию для уменьшения размера образа
FROM python:3.9-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Отключаем создание .pyc файлов и буферизацию вывода
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# [cite_start]Копируем код бота И вспомогательный модуль [cite: 3]
COPY bot.py .
COPY teams.py .

# Создаем пользователя без привилегий root (безопасность)
RUN useradd -m botuser
USER botuser

# Запускаем бота
CMD ["python", "bot.py"]
