FROM python:3.9-slim

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/data

RUN mkdir -p /data

VOLUME ["/data"]

EXPOSE 5000

CMD ["python", "app.py"]
