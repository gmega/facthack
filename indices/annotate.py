import json
import sys
from multiprocessing import Pool

import requests

pool = Pool(int(sys.argv[1]))


def process_document(raw_document):
    parsed = json.loads(raw_document)
    sentences = parsed['sentences']
    for sentence in sentences:
        response = json.loads(
            requests.post(
                'https://companytxt.spaziodati.eu/companytxt',
                data={'text': sentence, 'include': ['sameAs', 'types']}
            ).content
        )

        sentence['annotations'] = response

    return json.dumps(parsed)


for result in pool.imap_unordered(process_document, sys.stdin):
    sys.stdout.write(result)
    sys.stdout.write('\n')
