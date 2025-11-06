import logging
from opensearchpy import OpenSearch, RequestsHttpConnection
from app.shared_modules.open_search.manager import OpenSearchManager

logger = logging.getLogger("opensearch-client")

# Globals
opensearch_client: OpenSearch | None = None
opensearch_manager: OpenSearchManager | None = None


def init_opensearch():
    """
    Initialize and return a global OpenSearch client + manager.
    Should be called once during app or worker startup.
    """
    global opensearch_client, opensearch_manager

    if opensearch_client is not None:
        logger.info("🔁 OpenSearch client already initialized.")
        return opensearch_client, opensearch_manager

    try:
        logger.info("🚀 Initializing OpenSearch client (via SSH tunnel)...")

        # Adjust host/port according to your SSH tunnel setup
        opensearch_client = OpenSearch(
            hosts=[{"host": "host.docker.internal", "port": 5601}],
            http_auth=("searchadmin", "SuperSecurePassword123#"),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=30,
            connection_class=RequestsHttpConnection,
        )

        # Verify connection
        info = opensearch_client.info()
        logger.info(f"✅ Connected to OpenSearch cluster: {info.get('cluster_name', 'unknown')}")

        opensearch_manager = OpenSearchManager(opensearch_client)
        return opensearch_client, opensearch_manager

    except Exception as e:
        logger.exception(f"❌ Failed to initialize OpenSearch: {e}")
        opensearch_client = None
        opensearch_manager = None
        return None, None


def close_opensearch():
    """Close the OpenSearch client gracefully."""
    global opensearch_client, opensearch_manager
    if opensearch_client:
        try:
            opensearch_client.transport.close()
            logger.info("🧹 OpenSearch client connection closed.")
        except Exception as e:
            logger.warning(f"⚠️ Error closing OpenSearch client: {e}")
    opensearch_client = None
    opensearch_manager = None
