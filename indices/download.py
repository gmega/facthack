# -*- coding: utf-8 -*-

import json
import sys
from multiprocessing import Pool

from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import logging

EXCLUDE_SOURCES = [
    'Noodls', 'The Sidney Morning Herald.com', 'Reuters UK', 'Usa Today.com', 'Stern.de', 'Facts.ch',
    'CalcioMercato.com', 'Herald Scotland.com', 'CalcioMercato.it', 'Calcio Fanpage',
    'The Washington Post.com', 'Basketinside.com', 'Thestar.com', 'TheGuardian', 'BBC News',
    'La Gazzetta dello Sport (Ed. Roma)', 'The Japan Times', 'La Gazzetta dello Sport.it',
    'Frankfurter Allgemeine.faz.net', 'Le Monde.fr', 'La Gazzetta dello Sport', 'ChinaDaily.com.cn',
    'La Nacion', '20 Minuten.ch', 'Bloomberg', 'Libération.fr', 'Adevarul.ro', 'Dagensindustri.se',
    'Corriere dello Sport.it', 'Cinco Dìas', 'Datasport', 'ZDNet.com', 'The Telegraph.com.a',
    'Sport Asti.it', 'COR.COM', 'Janes.com', 'Ok Tennis', 'ZDNet.de', 'Tutto Basket.net',
    'La Gazzetta dello Sport (Ed. Sicilia)', 'FinYear.com', 'The Telegraph.co.uk',
    'La Gazzetta dello Sport (Ed. Puglia)', 'Sport Press', 'SportReggio', 'Food Navigator.com',
    'Tuttosport.com', 'Catholic News Service.com', 'Le Monde Diplomatique', 'Classic Boat.co.uk',
    'Ve.Sport.it', 'Digital Agenda for Europe', 'Milano online'
]

FILTER_CATEGORIES = ['Cronaca Locale', 'Prima Pagina', 'Economia / Finanza', 'Non specificata']

FILTER_SECTORS = ['Generalista', 'Attualità', 'Economia e Finanza']

EXCLUDE_HASHES = ['0647ac34aa1ed17ba998f7fc1d1f7ef2', 'd41d8cd98f00b204e9800998ecf8427e']
client = Elasticsearch(sys.argv[1])
logger = logging.getLogger(__name__)

def should_drop(document):
    info = document['nc:contentInfo']

    body_size = info['nc:bodySize']
    body_dirt = info['nc:bodyDirt']
    empty_title = info['nc:emptyTitle']
    borsa = info['nc:listOfNamesNumbers']
    hash = info['nc:hash']
    # lang_qual = info['nc:langQuality']

    categories = get_categories(document)
    #    source = get_source(document)

    return body_dirt >= 0.5 or \
           body_size < 300 or \
           body_size > 50000 or \
           hash in EXCLUDE_HASHES or \
           any(category in FILTER_CATEGORIES for category in categories) or \
           empty_title or \
           borsa


def get_source(document):
    extras = document['nc:extras'].get('nc:presstoday', None)
    # This is a Noodl (pressrelease)
    if extras is None:
        return None

    return extras['source']


def get_categories(document):
    categories = document['nc:annotations'].get('nc:categoryAnnotation', {}).get('pt:category', [])
    return [category['nc:categoryID'] for category in categories]


def process_index(index):
    index = index.strip()
    print 'Processing index %s' % index
    with open('./outputs/%s.jsonl' % index, 'w') as output:
        for document in helpers.scan(
                client,
                query='{"query": {"match_all": {}}}',
                index=index,
                doc_type='news_v2',
                scroll='5m'
        ):
            try:
                document = document['_source']
                if not should_drop(document):
                    json.dump(document, output)
                output.write('\n')
            except:
                logger.error('Error processing document.')
    print 'Done processing %s' % index


pool = Pool(4)
try:
    with open('index-sample.csv', 'r') as input:
        indices = input.readlines()

    for result in pool.imap_unordered(process_index, indices):
        pass
finally:
    pool.terminate()
