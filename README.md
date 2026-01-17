# s-index-api

## Getting started

### Prerequisites/Dependencies

You will need the following installed on your system:

- Python 3.8+
- [Pip](https://pip.pypa.io/en/stable/)
- [Docker](https://www.docker.com/) (optional, for containerized deployment)

### Setup

If you would like to update the api, please follow the instructions below.

1. Create a local virtual environment and activate it:

   ```bash
   python -m venv .venv
   source .venv/bin/activate # on Windows use .venv\Scripts\activate
   ```

   If you are using Anaconda, you can create a virtual environment with:

   ```bash
   conda create -n s-index-api-dev-env python=3.13
   conda activate s-index-api-dev-env
   ```

2. Install the dependencies for this package.

   ```bash
   pip install -r requirements.txt
   ```

## Running

For developer mode:

```bash
python app.py --host $HOST --port $PORT
```

or

```bash
flask run --debug
```

For production mode:

```bash
python3 app.py --host $HOST --port $PORT
```

## Docker Deployment

Example Docker run command:

```bash
docker run -p 5000:5000 your-app-image
```

## Health Checks

The application provides several health check endpoints:

- `/echo` - Basic application health check
- `/up` - Health check endpoint

## API Documentation

API documentation is available at `/docs` when the application is running. This provides an interactive Swagger UI for exploring and testing the API endpoints.
