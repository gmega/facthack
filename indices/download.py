# -*- coding: utf-8 -*-

import json
import logging

from elasticsearch import helpers

FILTER_CATEGORIES = ['Cronaca Locale', 'Prima Pagina', 'Economia / Finanza', 'Non specificata']

EXCLUDE_HASHES = ['0647ac34aa1ed17ba998f7fc1d1f7ef2', 'd41d8cd98f00b204e9800998ecf8427e']
logger = logging.getLogger(__name__)


def should_drop(document):
    info = document['nc:contentInfo']

    body_size = info['nc:bodySize']
    body_dirt = info['nc:bodyDirt']
    empty_title = info['nc:emptyTitle']
    borsa = info['nc:listOfNamesNumbers']
    hash = info['nc:hash']
    language = document['rnews:language']

    categories = get_categories(document)

    return body_dirt >= 0.5 or \
           body_size < 300 or \
           body_size > 50000 or \
           hash in EXCLUDE_HASHES or \
           any(category in FILTER_CATEGORIES for category in categories) or \
           empty_title or \
           borsa or \
           language != 'it'


def clean_annotations(document):
    # Filters out bad annotations.
    document['nc:annotations'] = [
        x for x in document['nc:annotations'].get('nc:subjectAnnotation', []) if is_good(x)
    ]


def is_good(annotation):
    threshold = 0.7 if annotation['nc:reconciled'] else 0.8
    return annotation['nc:confidence'] >= threshold


def get_categories(document):
    categories = document['nc:annotations'].get('nc:categoryAnnotation', {}).get('pt:category', [])
    return [category['nc:categoryID'] for category in categories]


def process_index(index, client, query, output=None):
    index = index.strip()
    query_string = json.dumps(query)
    print 'Processing index %s' % index
    output = './outputs/%s.jsonl' % index if output is None else output
    with open(output, 'w') as ostream:
        for document in helpers.scan(
                client,
                query=query_string,
                index=index,
                doc_type='news_v2',
                scroll='5m'
        ):
            try:
                clean_annotations(document)
                if not should_drop(document):
                    json.dump(document, ostream)
                    ostream.write('\n')
            except:
                logger.error('Error processing document.')
    print 'Done processing %s' % index
