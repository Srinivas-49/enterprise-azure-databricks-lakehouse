# Reproducible local test container; no Azure services are provisioned.
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir pytest
CMD ["python", "-m", "pytest", "-q"]
