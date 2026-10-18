# Build context: repository root (needs debezium/postgres-connector.json as well as apps/control-center/)

# --- 1. Build the React UI ---
FROM node:20-alpine AS ui
WORKDIR /ui
COPY apps/control-center/frontend/package.json apps/control-center/frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY apps/control-center/frontend/ ./
RUN npm run build

# --- 2. FastAPI backend serving the API and the built UI ---
FROM python:3.11-slim
WORKDIR /app
COPY apps/control-center/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY debezium/postgres-connector.json ./postgres-connector.json
COPY apps/control-center/backend/*.py ./
COPY --from=ui /ui/dist ./static
ENV CONNECTOR_CONFIG=/app/postgres-connector.json
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
