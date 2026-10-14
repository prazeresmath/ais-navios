FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Usuário sem privilégios
RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "producer.py"]
