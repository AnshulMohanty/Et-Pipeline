# Dockerfile - build the backend image (API + worker)
FROM python:3.11-slim

# system deps for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl \
  && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m appuser
WORKDIR /app

# Copy only what's needed early for caching
COPY requirements.txt /app/requirements.txt

# Install python deps
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source
COPY . /app

# Make sure scripts are executable
RUN chmod -R a+rX /app

# Switch to non-root user
USER appuser

ENV PYTHONUNBUFFERED=1
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Default command is uvicorn for API; override in compose for worker
CMD ["python", "-m", "uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
