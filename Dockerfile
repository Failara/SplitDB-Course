# 1. Беремо базовий образ Python
FROM python:3.10-slim

# 2. Встановлюємо робочу директорію всередині контейнера
WORKDIR /app

# 3. Встановлюємо необхідну бібліотеку
RUN pip install cassandra-driver

# 4. Копіюємо наш скрипт у контейнер
COPY lab2_bess.py .

# 5. Команда, яка виконається при запуску контейнера
CMD ["python", "lab2_bess.py"]