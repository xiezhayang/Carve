FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt

COPY model.py inference.py main.py ./

ENV CARVE_URL=http://carve:8080
ENV TARGET=""
ENV MODEL_NAME=lstm-default
ENV METRICS_PORT=9092

EXPOSE 9092

CMD ["python", "main.py"]