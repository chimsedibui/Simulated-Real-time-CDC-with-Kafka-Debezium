# Build context: repository root (needs scripts/ and debezium/ as well as webapp/)

# --- 1. Build the React UI ---
FROM node:20-alpine AS ui
WORKDIR /ui
COPY webapp/frontend/package.json webapp/frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY webapp/frontend/ ./
RUN npm run build

# --- 2. FastAPI backend serving the API and the built UI ---
FROM python:3.11-slim
WORKDIR /app
COPY webapp/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY debezium/postgres-connector.json ./postgres-connector.json
COPY webapp/backend/*.py ./
COPY --from=ui /ui/dist ./static
ENV CONNECTOR_CONFIG=/app/postgres-connector.json
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
