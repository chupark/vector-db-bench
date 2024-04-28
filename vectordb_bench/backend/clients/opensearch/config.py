from enum import Enum
from pydantic import SecretStr, BaseModel

from ..api import DBConfig, DBCaseConfig, MetricType, IndexType

class OpenSearchConfig(DBConfig):
    id: str
    password: SecretStr
    host: str
    port: int 

    def to_dict(self) -> dict:
        auth = (self.id, self.password.get_secret_value())
        return {
            "hosts": [{'host': self.host, 'port': self.port}],
            "http_compress" : True, # enables gzip compression for request bodies
            "http_auth": auth,
            "use_ssl" : True,
            "verify_certs" : False,
            "ssl_assert_hostname" : False,
            "ssl_show_warn" : False,
            "timeout": 600
        }
    

class OSElementType(str, Enum):
    float = "float"  # 4 byte
    byte = "byte"  # 1 byte, -128 to 127


class OpenSearchIndexConfig(BaseModel, DBCaseConfig):
    element_type: OSElementType = OSElementType.float
    index: IndexType = IndexType.ES_HNSW  # ES only support 'hnsw'

    metric_type: MetricType | None = None
    efConstruction: int | None = None
    M: int | None = None
    num_candidates: int | None = None

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "l2_norm"
        elif self.metric_type == MetricType.IP:
            return "dot_product"
        return "cosine"

    def index_param(self) -> dict:
        space_type = self.parse_metric()
        if space_type == "l2_norm":
            space_type = "l2"
        elif space_type == "dot_product":
            space_type = "dot_product"
        else :
            space_type = "cosinesimil"

        params = {
            "type": "knn_vector",
            "index": True,
            "similarity": self.parse_metric(),
            "method": {
                "name": self.index.value,
                "space_type": space_type,
                "engine": "nmslib",
                "parameters": {
                "m": self.M,
                "ef_construction": self.efConstruction,
            },
            }
        }
        print(params)
        return params

    def search_param(self) -> dict:
        return {
            "num_candidates": self.num_candidates,
        }
