# ─── Stage: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim

# Install system dependencies: ffmpeg, curl, and Node.js (required for pytubefix PO Token generation)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg curl nodejs && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Expose the port Uvicorn will listen on
EXPOSE 8000

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
