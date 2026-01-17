"""Initialize the api system for the backend"""

from flask_restx import Api, Resource

api = Api(
    title="S-Index API",
    description="The backend API system for S-Index",
    doc="/docs",
)


@api.route("/echo", endpoint="echo")
class HelloEveryNyan(Resource):
    """Test if the server is active"""

    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    def get(self):
        """Returns a simple 'Server Active' message"""

        return "Server active!"


@api.route("/up", endpoint="up")
class Up(Resource):
    """Health check for kamal"""

    @api.response(200, "Success")
    def get(self):
        """Returns a simple message"""

        return ":)"
