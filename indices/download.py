import json
import sys

from elasticsearch import helpers
from elasticsearch.client import Elasticsearch


def filter_document(document):
    return document


def process_index(index, client, output):
    print index
    for document in helpers.scan(
            client,
            query='{"query": {"match_all": {}}}',
            index=index,
            doc_type='news_v2',
            scroll='5m'
    ):
        processed = filter_document(document)
        if processed:
            json.dump(document['_source'], output)
        output.write('\n')


client = Elasticsearch(sys.argv[1])

with open('index-sample.csv', 'r') as indices:
    for index in indices:
        with open('./outputs/%s.jsonl', 'w') as output:
            process_index(index, client, output)
        break
