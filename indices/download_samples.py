import sys
from functools import partial
from multiprocessing import Pool

from elasticsearch.client import Elasticsearch

from indices.download import process_index

client = Elasticsearch(sys.argv[1])

pool = Pool(4)
try:
    with open('index-sample.csv', 'r') as input:
        indices = input.readlines()

    for result in pool.imap_unordered(
            partial(process_index, client=client, query={'query': {'match_all': {}}}),
            indices
    ):
        pass
finally:
    pool.terminate()
