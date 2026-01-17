"""Initialize the api system for the backend"""

from flask_restx import Api, Resource, reqparse

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


@api.route("/dataset-index-series-from-doi", endpoint="dataset_index_series_from_doi")
class DatasetIndexSeriesFromDoi(Resource):
    """Get dataset index series from a DOI"""

    parser = reqparse.RequestParser()
    parser.add_argument(
        "doi",
        type=str,
        required=True,
        help="DOI string (e.g., '10.13026/kpb9-mt58')",
        location="args",
    )

    @api.expect(parser)
    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    def get(self):
        """Returns dataset report for the given DOI"""
        from sindex.metrics.jobs import dataset_index_series_from_doi

        args = self.parser.parse_args()
        doi = args["doi"]
        return dataset_index_series_from_doi(doi)


@api.route("/dataset-index-series-from-url", endpoint="dataset_index_series_from_url")
class DatasetIndexSeriesFromUrl(Resource):
    """Get dataset index series from a URL"""

    parser = reqparse.RequestParser()
    parser.add_argument(
        "url",
        type=str,
        required=True,
        help="URL string",
        location="args",
    )
    parser.add_argument(
        "identifier",
        type=str,
        required=False,
        help="Identifier string",
        location="args",
    )
    parser.add_argument(
        "pubdate",
        type=str,
        required=False,
        help="Publication date string",
        location="args",
    )
    parser.add_argument(
        "topic_id",
        type=str,
        required=False,
        help="Topic ID string",
        location="args",
    )

    @api.expect(parser)
    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    def get(self):
        """Returns dataset report for the given URL"""
        from sindex.metrics.jobs import dataset_index_series_from_url

        args = self.parser.parse_args()
        url = args["url"]
        identifier = args.get("identifier")
        pubdate = args.get("pubdate")
        topic_id = args.get("topic_id")
        return dataset_index_series_from_url(
            url, identifier=identifier, pubdate=pubdate, topic_id=topic_id
        )
