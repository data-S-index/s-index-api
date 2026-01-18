FROM python:3.13-alpine

WORKDIR /app

EXPOSE 5000

# Install system dependencies including wget for downloading files
RUN apk add --no-cache gcc libffi-dev musl-dev wget

# Upgrade pip to latest version for better package compatibility
RUN pip install --no-cache-dir --upgrade pip

# Install Python dependencies directly via pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user for security
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

# Copy application files
COPY apis ./apis
COPY sindex ./sindex
COPY app.py entrypoint.sh ./

# Ensure the entrypoint script is executable
RUN chmod +x entrypoint.sh

# Change ownership of app directory to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set default entrypoint and command using the entrypoint script
ENTRYPOINT [ "./entrypoint.sh" ]

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:5000/up || exit 1