# === 第一階段：編譯與安裝 (Builder) ===
FROM python:3.11-slim AS builder
WORKDIR /app

# 安裝編譯所需的系統工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# 修正重點：不要在 uninstall 時使用 --prefix
# 我們直接在 builder 環境安裝，刪除完垃圾後，再複製 site-packages
RUN pip install --no-cache-dir -r requirements.txt && \
    pip uninstall -y nvidia-nccl-cu12 && \
    # 清理緩存
    rm -rf /root/.cache/pip

# === 第二階段：最終運行環境 (Final) ===
FROM python:3.11-slim
WORKDIR /app

# 從 builder 階段把安裝好的套件目錄整份複製過來
# 在 python-slim 中，路徑通常是 /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# 安裝運行必備的 runtime 庫
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 複製程式碼
COPY . .

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]