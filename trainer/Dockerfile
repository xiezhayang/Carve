FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# CPU-only PyTorch to keep image smaller (optional, if you need GPU remove this line)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY model.py train.py ./

ENV CARVE_URL=http://carve:8080
ENV CSV_FILENAME=metrics.csv
ENV MODEL_NAME=lstm-default

CMD ["python", "train.py"]