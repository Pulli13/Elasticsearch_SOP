import csv
import os
import time
from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/Users/pulkitgahlawat/Desktop/ElasticStack/elasticsearch-8.9.1/config/certs/http_ca.crt",
    basic_auth=("elastic", "Elastic@411")
)

csv_directory = '/Users/pulkitgahlawat/Desktop/Education/4-1/SOP/Code'
csv_delimiter = '|'

index_name = input("Enter the name for the Elasticsearch index: ")

if not es.indices.exists(index=index_name):
    print(f"Index '{index_name}' does not exist. Aborting document indexing.")
else:
    fileformat=input("Enter the general file structure of csv files excluding .csv: ")
    csv_files = [file for file in os.listdir(csv_directory) if file.startswith(fileformat) and file.endswith(".csv")]

    rate_csv_file_path = os.path.join(csv_directory, f"insertion_rate_{index_name}.csv")
    rate_csv_headers = ["Time (seconds)", "Documents Inserted"]

    with open(rate_csv_file_path, 'w', newline='') as rate_csv_file:
        rate_csv_writer = csv.writer(rate_csv_file, delimiter=csv_delimiter)
        rate_csv_writer.writerow(rate_csv_headers)

    total_inserted = 0
    start_time = int(time.time())  
    prev_elapsed_time = 0 

    for csv_file in csv_files:
        csv_file_path = os.path.join(csv_directory, csv_file)

        with open(csv_file_path, 'r') as file:
            reader = csv.DictReader(file, delimiter=csv_delimiter)
            for row in reader:
                row["FIRST_SWITCHED"] = int(row["FIRST_SWITCHED"])
                row["LAST_SWITCHED"] = int(row["LAST_SWITCHED"])
                es.index(index=index_name, body=row)
                total_inserted += 1

                current_time = int(time.time())  
                elapsed_time = current_time - start_time  
                documents_in_second = total_inserted

                if elapsed_time > prev_elapsed_time:
                    documents_in_second = total_inserted
                    with open(rate_csv_file_path, 'a', newline='') as rate_csv_file:
                        rate_csv_writer = csv.writer(rate_csv_file, delimiter=csv_delimiter)
                        rate_csv_writer.writerow([elapsed_time, documents_in_second])
                    prev_elapsed_time = elapsed_time

    current_time = int(time.time())
    elapsed_time = current_time - start_time +1
    with open(rate_csv_file_path, 'a', newline='') as rate_csv_file:
        rate_csv_writer = csv.writer(rate_csv_file, delimiter=csv_delimiter)
        rate_csv_writer.writerow([elapsed_time, total_inserted])

    print(f"Document indexing complete for index '{index_name}'.")
    print(f"Insertion rate data saved to '{rate_csv_file_path}'.")
