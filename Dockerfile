FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY dbt_project/ dbt_project/

# To avoid redownloading every study, it is possible to mount a volume with the data.
CMD ["python", "main.py"]
