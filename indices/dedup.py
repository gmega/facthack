import json
import sys

seen = set()

for raw_document in sys.stdin:
    # Skip empty lines.
    if not raw_document.strip():
        continue
    document = json.loads(raw_document)
    hash = document['nc:contentInfo']['nc:hash']
    
    # Filters out articles without company mentions.
    if not document['nc:annotations'].get('nc:subjectAnnotation', []):
        continue

    if hash not in seen:
        seen.add(hash)
        json.dump(document, sys.stdout)
        sys.stdout.write('\n')
