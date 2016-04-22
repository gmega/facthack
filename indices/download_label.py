import sys

from elasticsearch.client import Elasticsearch

from indices.download import process_index

client = Elasticsearch(sys.argv[1])
label = sys.argv[2]

query = {
    "query": {
        "nested": {
            "filter": {
                "terms": {
                    "nc:annotations.nc:categoryAnnotation.negativeEvents.nc:categoryID": [label]
                }
            },
            "path": "nc:annotations.nc:categoryAnnotation.negativeEvents"
        }
    }
}

process_index('news_all', client, query, './outputs/%s' % label)
