from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/Users/pulkitgahlawat/Desktop/ElasticStack/elasticsearch-8.9.1/config/certs/http_ca.crt",
    basic_auth=("elastic", "Elastic@411")
)

index_name = input("Enter the name for the new index: ")
index_mapping = {
    "mappings": {
        "properties": {
            "IPV4_SRC_ADDR": {"type": "ip"},
            "IPV4_DST_ADDR": {"type": "ip"},
            "INPUT_SNMP": {"type": "integer"},
            "OUTPUT_SNMP": {"type": "integer"},
            "IN_PKTS": {"type": "integer"},
            "IN_BYTES": {"type": "integer"},
            "FIRST_SWITCHED": {"type": "double"},
            "LAST_SWITCHED": {"type": "double"},
            "L4_SRC_PORT": {"type": "integer"},
            "L4_DST_PORT": {"type": "integer"},
            "TCP_FLAGS": {"type": "integer"},
            "PROTOCOL": {"type": "integer"},
            "SRC_TOS": {"type": "integer"},
            "SRC_AS": {"type": "integer"},
            "DST_AS": {"type": "integer"},
            "EXPORTER_IPV4_ADDRESS": {"type": "ip"},
            "IPV6_SRC_ADDR": {"type": "ip"}
        }
    }
}

if es.indices.exists(index=index_name):
    print(f"Index '{index_name}' already exists.")
else:
    es.indices.create(index=index_name, body=index_mapping)
    print(f"Index '{index_name}' is created.")
