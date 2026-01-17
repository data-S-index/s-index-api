"""Entry point for the application.

This module serves as the main entry point for the Flask application.
It handles application initialization, configuration, and server startup.
"""

import logging

# Flask imports for web framework functionality
from flask import Flask
from flask_cors import CORS  # For handling Cross-Origin Resource Sharing
from waitress import serve  # Production WSGI server

# Import the API blueprint from the apis module
from apis import api


def create_app(config_module=None, log_level="INFO"):
    """Initialize the core application.

    This function creates and configures a Flask application with all necessary
    middleware, extensions, and settings.

    Args:
        config_module: Optional configuration module to use (defaults to "config")
        log_level: Logging level for the application (defaults to "INFO")

    Returns:
        Flask: Configured Flask application instance
    """
    print("[APP] Initializing Flask application")
    print(f"[APP] Config module: {config_module or 'config'}, Log level: {log_level}")

    # Create and configure the Flask app
    app = Flask(__name__)
    print("[APP] Flask app created")

    # Configure Swagger UI settings for API documentation
    # "none" means no endpoints are expanded by default
    app.config["SWAGGER_UI_DOC_EXPANSION"] = "none"
    # Disable masking of Swagger documentation
    app.config["RESTX_MASK_SWAGGER"] = False
    print("[APP] Swagger UI configured")

    # Set up logging configuration
    # This configures the root logger with the specified log level
    logging.basicConfig(level=getattr(logging, log_level))
    print(f"[APP] Logging configured with level: {log_level}")

    # Load application configuration from the specified module
    # If no module is specified, defaults to "config"
    app.config.from_object(config_module or "config")
    print(f"[APP] Configuration loaded from: {config_module or 'config'}")

    # Initialize the API blueprint with the Flask app
    # This registers all API routes and endpoints
    api.init_app(app)
    print("[APP] API blueprint initialized")

    # Configure CORS (Cross-Origin Resource Sharing)
    # This allows the API to be accessed from different domains/origins
    CORS(
        app,
        resources={
            "/*": {  # Apply CORS to all routes
                "origins": "*",  # Allow all origins (for development - restrict in production)
            }
        },
        allow_headers=[
            "Content-Type",
            "Authorization",
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Credentials",
        ],
        supports_credentials=True,  # Allow cookies and authentication headers
    )
    print("[APP] CORS configured")
    print("[APP] Application initialization complete")

    return app


if __name__ == "__main__":
    # Command-line argument parsing for server configuration
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Start the Flask application server")
    parser.add_argument(
        "-P", "--port", default=5000, type=int, help="Port to listen on"
    )
    parser.add_argument(
        "-H", "--host", default="0.0.0.0", type=str, help="Host to bind to"
    )
    parser.add_argument(
        "-L",
        "--loglevel",
        default="INFO",
        type=str,
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in development mode with hot reload enabled",
    )

    # Parse command line arguments
    args = parser.parse_args()
    port = args.port
    host = args.host
    loglevel = args.loglevel
    dev_mode = args.dev

    print(f"[APP] Starting server - Host: {host}, Port: {port}, Log level: {loglevel}")
    if dev_mode:
        print("[APP] Development mode enabled - Hot reload active")

    # Create the Flask application with specified logging level
    flask_app = create_app(log_level=loglevel)

    if dev_mode:
        # Development mode: Use Flask's built-in development server with hot reload
        # This enables automatic code reloading when files change
        flask_app.config["DEBUG"] = True
        logging.info(
            "Starting development server on %s:%s with log level: %s",
            host,
            port,
            loglevel,
        )
        print(f"[APP] Development server starting on {host}:{port}")
        print(
            "[APP] Hot reload enabled - code changes will automatically restart the server"
        )
        print(f"[APP] API documentation available at: http://{host}:{port}/docs")
        flask_app.run(host=host, port=port, debug=True, use_reloader=True)
    else:
        # Production mode: Use Waitress (production-quality WSGI server)
        logging.info(
            "Starting server on %s:%s with log level: %s", host, port, loglevel
        )
        print(f"[APP] Server starting on {host}:{port}")
        print(f"[APP] API documentation available at: http://{host}:{port}/docs")
        serve(flask_app, port=port, host=host)
