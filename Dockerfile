FROM python:3.11-slim

# Security: run as non-root
RUN useradd -m -u 1001 botuser

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ ./src/
COPY main.py .

# Switch to non-root user
USER botuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import src.bot" || exit 1

CMD ["python", "main.py"]
