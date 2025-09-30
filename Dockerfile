FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (only what's needed)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p logs data

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Copy application code (after dependencies for better caching)
COPY . .

# Expose ports (API, Streamlit, Kafka UI)
EXPOSE 8080 8501 9092

# Default command (can be overridden)
CMD ["python", "pipeline_runner.py"]