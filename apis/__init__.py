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
        print("[API] GET /echo - Request received")
        print("[API] GET /echo - Status: Processing")
        result = "Server active!"
        print(f"[API] GET /echo - Status: Success - Response: {result}")
        return result


@api.route("/up", endpoint="up")
class Up(Resource):
    """Health check for kamal"""

    @api.response(200, "Success")
    def get(self):
        """Returns a simple message"""
        print("[API] GET /up - Request received")
        print("[API] GET /up - Status: Processing health check")
        result = ":)"
        print(f"[API] GET /up - Status: Success - Response: {result}")
        return result


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
        print("[API] GET /dataset-index-series-from-doi - Request received")
        from sindex.metrics.jobs import dataset_index_series_from_doi

        args = self.parser.parse_args()
        doi = args["doi"]
        print(
            f"[API] GET /dataset-index-series-from-doi - Status: Processing - DOI: {doi}"
        )
        try:
            result = dataset_index_series_from_doi(doi)
            print(
                f"[API] GET /dataset-index-series-from-doi - Status: Success - DOI: {doi}"
            )
            return result
        except Exception as e:
            print(
                f"[API] GET /dataset-index-series-from-doi - Status: Error - DOI: {doi} - Error: {str(e)}"
            )
            raise


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
        print("[API] GET /dataset-index-series-from-url - Request received")
        from sindex.metrics.jobs import dataset_index_series_from_url

        args = self.parser.parse_args()
        url = args["url"]
        identifier = args.get("identifier")
        pubdate = args.get("pubdate")
        topic_id = args.get("topic_id")
        print(
            f"[API] GET /dataset-index-series-from-url - Status: Processing - "
            f"URL: {url}, identifier: {identifier}, pubdate: {pubdate}, "
            f"topic_id: {topic_id}"
        )
        try:
            result = dataset_index_series_from_url(
                url, identifier=identifier, pubdate=pubdate, topic_id=topic_id
            )
            print(
                f"[API] GET /dataset-index-series-from-url - Status: Success - URL: {url}"
            )
            return result
        except Exception as e:
            print(
                f"[API] GET /dataset-index-series-from-url - Status: Error - URL: {url} - Error: {str(e)}"
            )
            raise
