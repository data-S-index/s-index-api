FROM python:3.13-alpine

WORKDIR /app

EXPOSE 5000

# Install system dependencies including wget for downloading files
RUN apk add --no-cache gcc libffi-dev musl-dev wget

# Install Python dependencies directly via pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY apis ./apis
COPY sindex ./sindex
COPY app.py utils.py entrypoint.sh ./

# Optional: Ensure the entrypoint script is executable
RUN chmod +x entrypoint.sh

# Create directories for DuckDB files
RUN mkdir -p input/mdc input/mock_norm

# Download DuckDB files from CDN
# CDN_URL should be passed as a build argument, e.g., --build-arg CDN_URL=https://your-cdn.com
ARG CDN_URL
RUN if [ -n "$CDN_URL" ]; then \
    wget -q "${CDN_URL}/mdc_index.duckdb" -O input/mdc/mdc_index.duckdb && \
    wget -q "${CDN_URL}/mock_norm.duckdb" -O input/mock_norm/mock_norm.duckdb; \
    fi

# Set default entrypoint and command
ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]