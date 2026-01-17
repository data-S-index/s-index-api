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

# Set default entrypoint and command
ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]