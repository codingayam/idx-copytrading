# Build Stage for Frontend
FROM node:20-slim AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Final Stage
FROM python:3.11-slim
WORKDIR /app

# Install system dependencies
# gcc and libpq-dev are often needed for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Backend Code
COPY . .

# Copy Frontend Build from builder stage
# The API serves static files from frontend/dist
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV TZ="Asia/Jakarta"

# Expose port (Railway will override PORT env var)
EXPOSE 8000

# Default start command (Web Service)
# overridden by cron service start command
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
