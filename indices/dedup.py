import json
import sys

seen = set()

def is_good(annotation):
    threshold = 0.7 if annotation['nc:reconciled'] else 0.8
    return annotation['nc:confidence'] >= threshold

for raw_document in sys.stdin:
    # Skip empty lines.
    if not raw_document.strip():
        continue
    document = json.loads(raw_document)
    hash = document['nc:contentInfo']['nc:hash']

    # Filter out documents not declared to be in italian.
    if document['nc:inLanguage'] != 'it':
        continue
    
    # Filters out bad annotations.
    annotations = [
        x for x in document['nc:annotations'].get('nc:subjectAnnotation', []) if is_good(x)
    ]

    # If nothing is left (or nothing was there in the first place), discards the document.
    if not annotations:
        continue

    if hash not in seen:
        seen.add(hash)
        json.dump(document, sys.stdout)
        sys.stdout.write('\n')
