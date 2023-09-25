import requests
import csv
import os

elasticsearch_url = "https://localhost:9200"
elasticsearch_user = "elastic"
elasticsearch_password = "Elastic@411"
ca_certs = "/Users/pulkitgahlawat/Desktop/ElasticStack/elasticsearch-8.9.1/config/certs/http_ca.crt"
queries_directory = "/Users/pulkitgahlawat/Desktop/Education/4-1/SOP/Code/Queries" 
output_directory = "/Users/pulkitgahlawat/Desktop/Education/4-1/SOP/Code/Output_Queries" 

os.makedirs(output_directory, exist_ok=True)
query_files = sorted([file for file in os.listdir(queries_directory) if file.startswith("query_") and file.endswith(".txt")])

sql_endpoint = f"{elasticsearch_url}/_sql?format=txt"
auth = (elasticsearch_user, elasticsearch_password)

query_execution_times = []

try:
    for i, query_file in enumerate(query_files, start=1):
        query_file_path = os.path.join(queries_directory, query_file)

        with open(query_file_path, "r") as query_file:
            sql_query = query_file.read()

        payload = {
            "query": sql_query
        }
        response = requests.post(sql_endpoint, json=payload, auth=auth, verify=ca_certs)

        if response.status_code == 200:
            took = round(response.elapsed.total_seconds() * 1000, 3)  
            query_execution_times.append({"Query": i, "Time (milliseconds)": took})
            result_data = [line.split("|") for line in response.text.splitlines()]
            headers = [header.strip() for header in result_data[0]]
            data = [dict(zip(headers, [value.strip() for value in values])) for values in result_data[1:] if len(values) == len(headers)]
            output_file = os.path.join(output_directory, f"result_{i}.csv")

            with open(output_file, "w", newline="") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=headers, delimiter="|")
                csv_writer.writeheader()
                csv_writer.writerows(data)

            print(f"Query {i} output saved to '{output_file}'")
            # print(f"Query {i} took {took:.3f} milliseconds to execute")
        else:
            print(f"Error for query {i}: {response.status_code} - {response.text}")

    query_execution_times_file = os.path.join(output_directory, "query_execution_times.csv")
    with open(query_execution_times_file, "w", newline="") as csv_file:
        fieldnames = ["Query", "Time (milliseconds)"]
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames,delimiter="|")
        csv_writer.writeheader()
        csv_writer.writerows(query_execution_times)

    print(f"Query execution times saved to '{query_execution_times_file}'")

except Exception as e:
    print(f"An error occurred: {str(e)}")
