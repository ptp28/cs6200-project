import json
from collections import defaultdict

def merge_json_files(file1_path, file2_path, output_path):
    # Load both JSON files
    with open(file1_path, 'r') as file1:
        data1 = json.load(file1)
    
    with open(file2_path, 'r') as file2:
        data2 = json.load(file2)

    # Merge the data
    merged_data = defaultdict(dict)

    for query, urls in data1.items():
        for url, score in urls.items():
                merged_data[query][url] = score

    for query, urls in data2.items():
        for url, score in urls.items():
                merged_data[query][url] = score

    # Write the merged data to an output JSON file
    with open(output_path, 'w') as output_file:
        json.dump(merged_data, output_file, indent=4)

# Example usage
file1_path = "relevance_feedback.json"
file2_path = "relevance_feedback/okapitf_10_rf.json"
output_path = "merged_file.json"
merge_json_files(file1_path, file2_path, output_path)