import json
import sys

seen = set()

for raw_document in sys.stdin:
    document = json.loads(raw_document)
    hash = document['nc:contentInfo']['nc:hash']
    if hash not in seen:
        seen.add(hash)
        json.dump(hash, sys.stdout)
