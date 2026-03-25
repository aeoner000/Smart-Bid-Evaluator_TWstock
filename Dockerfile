FROM python:3.11-slim
WORKDIR /app

# 安裝依賴 (跟之前一樣)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製程式碼 (包含 main.py 和 streamlit_folder)
COPY . .

# 設定環境變數
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# 只要這一行保底就好 (預設啟動網頁)
CMD ["streamlit", "run", "streamlit_folder/app.py", "--server.port=8080", "--server.address=0.0.0.0"]