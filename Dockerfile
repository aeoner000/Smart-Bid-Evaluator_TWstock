# 使用 Python 3.11 輕量版作為基底
FROM python:3.11-slim

# 1. 安裝系統層級的依賴套件 (解決 libgomp.so.1 與其他潛在問題)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libgomp1 \
    libstdc++6 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. 設定工作目錄
WORKDIR /app

# 3. 先處理套件安裝 (利用 Docker Layer Cache 節省後續部署時間)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 4. 複製所有程式碼
COPY . .

# 5. 設定環境變數 (避免 Python 產生 .pyc 檔案並強制即時輸出日誌)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 預設指令 (這裡會被 Cloud Run Job 的 Arguments 覆蓋)
CMD ["python", "main.py"]