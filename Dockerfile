FROM python:3.11-slim

# התקנת תלויות מערכת לפלייוורייט + כרומיום
RUN apt-get update && apt-get install -y \
    wget curl gnupg ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libdbus-1-3 libxkbcommon0 libatspi2.0-0 libx11-6 libxcomposite1 \
    libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# התקנת תלויות Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# התקנת כרומיום לפלייוורייט
RUN playwright install chromium
# העתקת קוד
COPY . .

# תיקיית נתונים קבועה (fallback ל-SQLite אם אין DATABASE_URL)
RUN mkdir -p /data
ENV DB_PATH=/data/rank_tracker.db
# אם DATABASE_URL מוגדר (PostgreSQL), הוא ישמש אוטומטית

EXPOSE 8080

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "300"]
