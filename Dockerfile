FROM python:3.13-slim

WORKDIR /app

EXPOSE 5000

# Upgrade pip to latest version for better package compatibility
RUN pip install --no-cache-dir --upgrade pip

# Install Python dependencies directly via pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY apis ./apis
COPY sindex ./sindex
COPY app.py entrypoint.sh ./

# Ensure the entrypoint script is executable
RUN chmod +x entrypoint.sh

# Set default entrypoint and command using the entrypoint script
ENTRYPOINT [ "/bin/sh", "./entrypoint.sh" ]

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:5000/up || exit 1