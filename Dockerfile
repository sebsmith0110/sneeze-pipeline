FROM python:3.11-slim

# System deps for pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./

CMD ["python","-u","main.py"]
