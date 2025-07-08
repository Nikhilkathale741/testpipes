FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openssh-client \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create directory for SSH keys
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh

# Make the migration script executable
RUN chmod +x migrate.py

# Default command
CMD ["python", "migrate.py"]