# === 第一階段：編譯與安裝 (Builder) ===
FROM python:3.11-slim AS builder
WORKDIR /app

# 安裝編譯所需的系統工具 (CatBoost 和 XGBoost 安裝時需要)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# 1. 安裝所有套件
# 2. 強制刪除那個 384MB 的 nvidia 垃圾
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt && \
    pip uninstall -y --prefix=/install nvidia-nccl-cu12

# === 第二階段：最終運行環境 (Final) ===
FROM python:3.11-slim
WORKDIR /app

# 只複製瘦身後的套件
COPY --from=builder /install /usr/local

# 安裝運行 ML 框架必備的 Runtime 庫 (libgomp1 是必須的)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY . .

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]