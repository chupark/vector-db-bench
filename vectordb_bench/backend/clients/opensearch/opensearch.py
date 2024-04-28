import logging 
import time
from contextlib import contextmanager
from typing import Iterable
from .config import OpenSearchIndexConfig
from ..api import VectorDB
from typing import Any
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

for logger in ("opensearch"):
    logging.getLogger(logger).setLevel(logging.WARNING)

log = logging.getLogger(__name__)

class OpenSearchClient(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: OpenSearchIndexConfig,
        indice: str = "test_openai_500k",  # must be lowercase
        id_col_name: str = "id",
        vector_col_name: str = "vector",
        drop_old: bool = False,
        **kwargs,
    ):
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config
        self.indice = indice
        self.id_col_name = id_col_name
        self.vector_col_name = vector_col_name


        from opensearchpy import OpenSearch
        client = OpenSearch(**self.db_config)
        if drop_old:
            log.info(f"OpenSearch client drop_old indices: {self.indice}")
            is_existed_res = client.indices.exists(index=self.indice)
            if is_existed_res :
                client.indices.delete(index=self.indice)
            self._create_indice(client)
    @contextmanager
    def init(self) -> None:
        """connect to opensearch"""
        from opensearchpy import OpenSearch
        self.client = OpenSearch(**self.db_config)
    
        yield

        self.client = None
        del(self.client)

    def _create_indice(self, client) -> None:
        mappings = {
            "settings": {
                "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100,
                "number_of_shards": 3,
                "number_of_replicas": 0
                }
            },
            "mappings":{
                "_source": {"excludes": [self.vector_col_name]},
                "properties": {
                    self.id_col_name: {"type": "integer", "store": True},
                    self.vector_col_name: {
                        "dimension": self.dim,
                        **self.case_config.index_param(),
                    },
                }
            }
        }

        try:
            client.indices.create(index=self.indice, body=mappings)
        except Exception as e:
            raise e from None
        
    def insert_embeddings(
        self,
        embeddings: Iterable[list[float]],
        metadata: list[int],
        **kwargs,
    ) -> (int, Exception):
        """Insert the embeddings to the OpenSearch."""
        assert self.client is not None, "should self.init() first"


        insert_data: Any = []
        bulk_cnt = 0
        bulk_size = 1000
        for i in range(len(embeddings)):
            insert_data.append({ "index": { "_index": self.indice, "_id": metadata[i] } })
            insert_data.append({ self.id_col_name: metadata[i], self.vector_col_name: embeddings[i] })

            if (len(insert_data) / 2) % bulk_size == 0 :
                bulk_insert_res = self.client.bulk(insert_data)
                if bulk_insert_res['errors'] == True:
                    return (bulk_insert_res[0], None)
                else :
                    bulk_cnt += len(bulk_insert_res['items'])
                    insert_data: Any = []
                    log.info(f"{bulk_cnt} data created")
                
        
        if insert_data:
            bulk_insert_res = self.client.bulk(insert_data)
            if bulk_insert_res['errors'] == True:
                    return (bulk_insert_res[0], None)
            else :
                bulk_cnt += len(bulk_insert_res['items'])
                log.info(f"{bulk_cnt} data created")


        # insert_data = [
        #     {
        #         "_index": self.indice,
        #         "_source": {
        #             self.id_col_name: metadata[i],
        #             self.vector_col_name: embeddings[i],
        #         },
        #     }
        #     #for i in range(len(embeddings))
        #     for i in range(2)
        # ]
        try:
            bulk_cnt
            return (bulk_cnt, None)
        except Exception as e:
            log.warning(f"Failed to insert data: {self.indice} error: {str(e)}")
            return (0, e)
    

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
    ) -> list[int]:
        """Get k most similar embeddings to query vector.

        Args:
            query(list[float]): query embedding to look up documents similar to.
            k(int): Number of most similar embeddings to return. Defaults to 100.
            filters(dict, optional): filtering expression to filter the data while searching.

        Returns:
            list[tuple[int, float]]: list of k most similar embeddings in (id, score) tuple to the query embedding.
        """
        assert self.client is not None, "should self.init() first"
        is_existed_res = self.client.indices.exists(index=self.indice)
        assert is_existed_res == True, "should self.init() first"

        size = k

        if filters:
            knn = {
                "size": size,
                "query": {
                    "knn": {
                        self.vector_col_name: {
                            "vector": query,
                            "k": k
                        }
                    }
                },
                "post_filter": {"range": {self.id_col_name: {"gt": filters["id"]}}}
            }
        else:
            knn = {
                "size": size,
                "query": {
                    "knn": {
                        self.vector_col_name: {
                            "vector": query,
                            "k": k
                        }
                    }
                }
            }

        try:
            res = self.client.search(
                index=self.indice,
                body=knn,
                size=size,
                _source=False,
                docvalue_fields=[self.id_col_name],
                stored_fields="_none_",
                # filter_path=[f"hits.hits.fields.{self.id_col_name}"],
            )
            res = [h["fields"][self.id_col_name][0] for h in res["hits"]["hits"]]
            return res
        except Exception as e:
            log.warning(f"Failed to search: {self.indice} error: {str(e)}")
            raise e from None

    def ready_to_load(self):
        """ready_to_load will be called before load in load cases."""
        pass


    def optimize(self):
        """optimize will be called between insertion and search in performance cases."""
        assert self.client is not None, "should self.init() first"
        self.client.indices.refresh(index=self.indice)
        force_merge_task_id = self.client.indices.forcemerge(index=self.indice, max_num_segments=1, wait_for_completion=False)['task']
        log.info(f"OpenSearch force merge task id: {force_merge_task_id}")
        SECONDS_WAITING_FOR_FORCE_MERGE_API_CALL_SEC = 30
        while True:
            time.sleep(SECONDS_WAITING_FOR_FORCE_MERGE_API_CALL_SEC)
            task_status = self.client.tasks.get(task_id=force_merge_task_id)
            if task_status['completed']:
                return