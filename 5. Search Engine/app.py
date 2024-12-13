from flask import Flask, request, render_template
import pandas as pd
import numpy as np 
import json
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import math
from collections import defaultdict

stemmer = PorterStemmer()
stop_words = set(stopwords.words('english'))

rawfile = open('doc_term_frequency.json')
terms = [json.loads(line) for line in rawfile]

df = pd.DataFrame(terms)
total_terms = df['count'].sum()  
num_documents = df['url'].nunique()  
non_empty_documents = df[df['count'] > 0]['url'].nunique()  
unique_terms = df['term'].nunique() 
doc_lengths = df.groupby("url")["count"].sum().reset_index()
doc_lengths.rename(columns={"count": "doc_length"}, inplace=True)

def get_doc_length(url):
    result = doc_lengths[doc_lengths['url'] == url]
    if not result.empty:
        return result['doc_length'].values[0]
    else:
        return 0

def get_term_frequencies(term):
    # Document frequency: Number of documents containing the term
    document_frequency = df[df['term'] == term]['url'].nunique()
    # Collection frequency: Sum of the term counts across all documents
    collection_frequency = df[df['term'] == term]['count'].sum()
    
    return document_frequency, collection_frequency

def get_postings_list(term):
    # Filter rows for the specific term
    term_rows = df[df['term'] == term]
    
    postings_list = []
    for _, row in term_rows.iterrows():
        postings_list.append({
            'url': row['url'],
            'tf': row['count'],
        })
    return postings_list

def process_query(query):
    return [
    stemmer.stem(word.lower()) 
    for word in word_tokenize(query) 
    if word.isalnum() and word.lower() not in stop_words
]

def calculate_bm25(query, k1=1.5, b=0.75):
    query_terms = process_query(query)  # Tokenize and process the query
    N = len(doc_lengths)  # Total number of documents in the collection
    total_doc_length = doc_lengths['doc_length'].sum()
    num_documents = doc_lengths['url'].nunique()
    avg_doc_length = total_doc_length / num_documents
    doc_scores = defaultdict(float)  # Dictionary to store scores for each document

    for term in query_terms:
        # Step 1: Compute IDF for the term
        df_t = len(df[df['term'] == term]) 
        idf_t = math.log((N - df_t + 0.5) / (df_t + 0.5) + 1.0)

        # Step 2: Get the posting list for the term
        term_postings = get_postings_list(term)

        for posting in term_postings:
            url = posting['url']
            term_frequency = posting['tf']

            # Step 3: Get document length
            doc_length = doc_lengths.get(url, 0)

            # Step 4: Apply BM25 formula to calculate the score for this document
            term_freq_saturation = (term_frequency * (k1 + 1)) / (term_frequency + k1 * (1 - b + b * (doc_length / avg_doc_length)))
            score = idf_t * term_freq_saturation

            # Accumulate the score for this document
            doc_scores[url] += score

    # Step 5: Sort the documents by score (higher score means more relevant)
    top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

    return top_docs

def okapi_tf(term_frequency, doc_length, avg_doc_length, k1=1.5, b=0.75):
    term_frequency = float(term_frequency)
    doc_length = float(doc_length)
    avg_doc_length = float(avg_doc_length)
    
    # Calculate Okapi TF
    numerator = term_frequency
    denominator = term_frequency + k1 * ((1 - b) + b * (doc_length / avg_doc_length))
    
    return numerator / denominator

def calculate_okapi_tf(query, k1=1.5, b=0.75):
    query_terms = process_query(query)  # Tokenize and process the query
    N = len(doc_lengths)  # Total number of documents in the collection
    total_doc_length = doc_lengths['doc_length'].sum()
    num_documents = doc_lengths['url'].nunique()
    avg_doc_length = total_doc_length / num_documents
    doc_scores = defaultdict(float)  # Dictionary to store scores for each document

    for term in query_terms:
        # Get the posting list for the term
        term_postings = get_postings_list(term)

        for posting in term_postings:
            url = posting['url']
            term_frequency = posting['tf']

            # Step 1: Get document length
            doc_length = doc_lengths.get(url, 0)

            # Step 2: Apply Okapi TF formula to calculate the score for this document
            okapi_tf_value = okapi_tf(term_frequency, doc_length, avg_doc_length, k1, b)

            # Accumulate the Okapi TF score for this document
            doc_scores[url] += okapi_tf_value

    # Step 3: Sort the documents by score (higher score means more relevant)
    top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

    return top_docs

def query_likelihood_dirichlet(query, mu=550):
    query_terms = process_query(query)
    total_terms_in_collection = df['count'].sum()
    
    doc_scores = {}

    for term in query_terms:
        doc_freq, coll_freq = get_term_frequencies(term)
        
        posting_list = get_postings_list(term)
        
        p_collection = coll_freq / total_terms_in_collection
        
        for posting in posting_list:
            url = posting['url']
            term_frequency = posting['tf']

            doc_length = get_doc_length(url)
            
            smoothed_probability = (term_frequency + mu * p_collection) / (doc_length + mu)

            if url not in doc_scores:
                doc_scores[url] = 0
            doc_scores[url] += math.log(smoothed_probability)

    top_k_docs = sorted(doc_scores.items(), key=lambda item: item[1], reverse=True)
    
    return [(url, score) for url, score in top_k_docs]


app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def fetch_results():
    results = []
    model = ""
    query = ""

    if request.method == "POST":
        model = request.form["retrieval_model"]
        query = request.form["query"]

        if model == 'BM25':
            results = calculate_bm25(str(query))
        elif model == 'QLM':
            results = query_likelihood_dirichlet(str(query))
        elif model == 'OKAPITF':
            results = calculate_okapi_tf(str(query))

        
    return render_template("search.html", results=results, result_info=f"Searched for <b>{query}</b> using <b>{model}</b>")


if __name__ == "__main__":
    app.run(debug=True)



