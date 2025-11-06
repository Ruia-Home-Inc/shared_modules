from app.api.common.enums.upload_features import FeatureType
from opensearchpy import OpenSearch, RequestsHttpConnection
import asyncio
from fastapi import HTTPException
import time


class OpenSearchManager:
    def __init__(self, client: OpenSearch = None):
        if client:
            self.client = client
        else:
            self.client = OpenSearch(
                hosts=[{"host": "host.docker.internal", "port": 5601}],
                http_auth=("searchadmin", "SuperSecurePassword123#"),
                use_ssl=True,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30,
                connection_class=RequestsHttpConnection,
            )
        

    async def save(self, index: str, doc_id: str, document: dict) -> dict:
        try:
            start = time.time()
            print(f"Saving doc {doc_id} to index '{index}' with {document}")
            response = await asyncio.to_thread(
                self.client.index,
                index=index,
                id=doc_id,
                body=document,
            )
            print(f"Saved doc {doc_id} in {time.time() - start:.2f}s")
            return {"result": response.get("result"), "id": doc_id}
        except Exception as e:
            print("OpenSearch save failed")
            raise HTTPException(status_code=500, detail=str(e))
        
    async def query(self, index: str, query_body: dict) -> dict:
        try:
            response = await asyncio.to_thread(
                self.client.search,
                index=index,
                body=query_body,
            )
            return response
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def update(self, index: str, doc_id: str, fields: dict) -> dict:
        """Update specific fields in an existing document."""
        try:
            start = time.time()
            print(f"✏️ Updating doc {doc_id} in index '{index}' with {fields}")
            response = await asyncio.to_thread(
                self.client.update,
                index=index,
                id=doc_id,
                body={"doc": fields},
            )
            print(f"Updated doc {doc_id} in {time.time() - start:.2f}s")
            return {"result": response.get("result"), "id": doc_id}
        except Exception as e:
            print("OpenSearch update failed")
            raise HTTPException(status_code=500, detail=str(e))
            
    async def bulk_update(self, index: str, doc_id: str, fields: dict):
        """Update an existing document or upsert if missing."""
        from opensearchpy.exceptions import NotFoundError
        try:
            response = await asyncio.to_thread(
                self.client.update,
                index=index,
                id=doc_id,
                body={
                    "doc": fields,
                    "doc_as_upsert": True
                }
            )
            return response
        except NotFoundError:
            # Fallback: create it manually if still missing
            print(f"Document {doc_id} not found in {index}, creating new one")
            return await asyncio.to_thread(
                self.client.index,
                index=index,
                id=doc_id,
                body=fields
            )
        except Exception as e:
            print(f"OpenSearch update failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # async def save_batch(self, feature_type: FeatureType, documents: list[dict]) -> dict:
    #     """
    #     Save a batch of documents to OpenSearch for a specific feature type.
    #     The index name is derived from FeatureType.<FEATURE>.value.
    #     """
    #     if not documents:
    #         return {"status": "skipped", "reason": "No documents to save"}

    #     index = feature_type.value
    #     start_time = time.time()

    #     print(f"🚀 Saving {len(documents)} docs to OpenSearch index '{index}'")

    #     actions = [
    #         {
    #             "_op_type": "index",
    #             "_index": index,
    #             "_id": doc.get("id") or None,
    #             "_source": doc,
    #         }
    #         for doc in documents
    #     ]

    #     try:

    #         success, errors = await asyncio.to_thread(
    #             self._bulk_index, actions
    #         )

    #         print(f"✅ Indexed {success} docs in {time.time() - start_time:.2f}s")
    #         if errors:
    #             print(f"⚠️ {len(errors)} docs failed to index")
    #         return {"indexed": success, "errors": errors}

    #     except Exception as e:
    #         print("❌ Bulk save to OpenSearch failed:", e)
    #         raise HTTPException(status_code=500, detail=str(e))

    # def _bulk_index(self, actions):
    #     """Run bulk indexing synchronously (wrapped by asyncio.to_thread)."""
    #     from opensearchpy.helpers import bulk
    #     success, errors = bulk(self.client, actions, raise_on_error=False)
    #     return success, errors
    

    async def save_batch(self, feature_type: FeatureType, documents: list[dict]) -> dict:
        """
        Save a batch of documents to OpenSearch for a specific feature type.
        Uses `update` with `doc_as_upsert=True` to ensure consistent overwriting.
        """
        if not documents:
            return {"status": "skipped", "reason": "No documents to save"}

        index = feature_type.value
        start_time = time.time()

        print(f"🚀 Saving {len(documents)} docs to OpenSearch index '{index}'")

        actions = [
            {
                "_op_type": "update",
                "_index": index,
                "_id": doc["id"],  # consistent ID ensures overwrite
                "doc": doc,
                "doc_as_upsert": True
            }
            for doc in documents
        ]

        try:
            success, errors = await asyncio.to_thread(self._bulk_index, actions)

            print(f"✅ Indexed {success} docs in {time.time() - start_time:.2f}s")
            if errors:
                print(f"⚠️ {len(errors)} docs failed to index")

            return {"indexed": success, "errors": errors}

        except Exception as e:
            print("❌ Bulk save to OpenSearch failed:", e)
            raise HTTPException(status_code=500, detail=str(e))


    def _bulk_index(self, actions):
        """Run bulk indexing synchronously (wrapped by asyncio.to_thread)."""
        from opensearchpy.helpers import bulk
        success, errors = bulk(self.client, actions, raise_on_error=False)
        return success, errors