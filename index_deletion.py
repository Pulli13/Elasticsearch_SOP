from elasticsearch import Elasticsearch

elasticsearch_url = "https://localhost:9200"
elasticsearch_user = "elastic"
elasticsearch_password = "Elastic@411"

# CA certificates path (optional, if you're using SSL)
ca_certs="/Users/pulkitgahlawat/Desktop/ElasticStack/elasticsearch-8.9.1/config/certs/http_ca.crt"

index_name = input("Enter index name : ")

# Initialize Elasticsearch client
# es = Elasticsearch(
#     elasticsearch_url,
#     http_auth=(elasticsearch_user, elasticsearch_password),
#     ca_certs=ca_certs
# )
es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/Users/pulkitgahlawat/Desktop/ElasticStack/elasticsearch-8.9.1/config/certs/http_ca.crt",
    basic_auth=(elasticsearch_user, elasticsearch_password)
)

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Index '{index_name}' deleted successfully.")
else:
    print(f"Index '{index_name}' does not exist.")
