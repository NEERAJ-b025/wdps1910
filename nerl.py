import sys
import gzip
from warcio.archiveiterator import ArchiveIterator
import requests
from functools import reduce
import nltk
from nltk.corpus import stopwords
from io import StringIO
import re
from html.parser import HTMLParser
import spacy
from config import ELASTIC_SEARCH_URL, TRIDENT_URL, ES_RESULTS_COUNT
import math

nltk.download('stopwords')
nltk.download('maxent_ne_chunker')
sw_eng = stopwords.words('english')

SPACY_MODEL = None


def get_spacy_model():
    global SPACY_MODEL
    if not SPACY_MODEL:
        _model = spacy.load("en_core_web_lg")
        # FIX https://github.com/explosion/spaCy/issues/922
        _model.vocab.add_flag(
            lambda s: s.lower() in spacy.lang.en.stop_words.STOP_WORDS,
            spacy.attrs.IS_STOP
        )
        SPACY_MODEL = _model
    return SPACY_MODEL


class WarcRecord:
    def __init__(self, web_arch_record: str):
        self.id = None
        self.payload = None
        self.broken = None
        # self.ner = None
        self._parse(web_arch_record)

    def _parse(self, web_arch_record):
        buffer = StringIO(web_arch_record.strip())
        # Parsing headers
        while True:
            line = buffer.readline().strip()
            if line == '':
                break
            if self.id is None and 'WARC-TREC-ID' in line:
                self.id = line.split('WARC-TREC-ID:')[1].strip()
        if self.id is None:
            self.broken = True
            return None
        # Maybe skip another set of headers
        line = buffer.readline().strip()
        if line.startswith('HTTP/'):
            line = ''
            while True:
                if buffer.readline().strip() == '':
                    break
        # Rest is payload
        self.payload = line + buffer.read().strip()
        self.broken = False


class TextExtractor(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self._inside_script = False
        self._inside_style = False
        self._inside_title = False
        self._words = []
        self._word_regex = re.compile('\w+')  # Regex for words..

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self._inside_script = True
        elif tag == 'style':
            self._inside_style = True
        elif tag == 'title':
            self._inside_title = True

    def handle_endtag(self, tag):
        self._inside_script = False
        self._inside_style = False
        self._inside_title = False

    def handle_data(self, data):
        if self._inside_script or self._inside_style or self._inside_title:
            return
        words = self._word_regex.findall(data)
        if len(words) == 0:
            return
        if 0 < len(words) <= 2:
            self._words.append(' '.join(words))
        else:
            for word in words:
                self._words.append(word)

    @staticmethod
    def get_all_words(text: str) -> list:
        extractor = TextExtractor()
        extractor.feed(text)
        return extractor._words


def ner_linking(words, warc_id):
    if not words:
        return
    sentence = ""
    if words:
        if len(words) > 1:
            sentence = reduce(lambda a, b: a + " " + b, words)
    nlp = get_spacy_model()
    doc = nlp(sentence)
    sentence = ""
    nerList = {}
    # (X.text, X.label_) for X in doc.ents
    disregard_ne_list = ("DATE", "TIME", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL")
    for X in doc.ents:
        if X.label_ not in disregard_ne_list:
            nerList[X.text] = X.label_
    url = ELASTIC_SEARCH_URL
    # key -> Barack Obama
    # val -> PERSON
    for key in nerList:
        query = key
        response = requests.get(
            url, params={
                'q': query,
                'size': ES_RESULTS_COUNT
            })
        result = {}
        id_max = {}
        max_score = 0
        if response:
            response = response.json()
            for hit in response.get('hits', {}).get('hits', []):
                freebase_id = hit.get('_source', {}).get('resource')
                label = hit.get('_source', {}).get('label')
                score = hit.get('_score', 0)
                if result.get(freebase_id) is None:
                    result[freebase_id] = ({
                        'label': label,
                        'score': score,
                        'facts': 0,
                        'rank': 0
                    })
                else:
                    score_1 = max(result[freebase_id]['score'], score)
                    # Return entity with its associated dictionary with the info from elastuc search query
                    result[freebase_id]['score'] = score_1
                if result[freebase_id]['score'] > max_score:
                    max_score = result[freebase_id]['score']
            for f_id, res in result.items():
                if f_id and res:
                    if res['score'] >= max_score:
                        id_max[f_id] = res

            sparql_query = """
                SELECT DISTINCT * WHERE {
                    <http://rdf.freebase.com/ns/%s> ?p ?o.
                }
                """

            for f_id in id_max:
                if f_id:
                    fid = f_id.replace('/m/', 'm.')
                    query = sparql_query % fid
                    response2 = requests.post(TRIDENT_URL, data={'print': False, 'query': query})
                    if response2:
                        response2 = response2.json()
                        n = int(response2.get('stats', {}).get('nresults', 0))
                        id_max[f_id]['facts'] = n
                        if n != 0:
                            id_max[f_id]['rank'] = math.log(n) * id_max[f_id]['score']  # Heuristics based weightage
                            # to elastic search rank and sparql num_results
                        else:
                            id_max[f_id]['rank'] = 0
            linked_ent = dict()
            if id_max.items() is not None:
                linked_ent = dict(sorted(id_max.items(), key=lambda x: (x[1]['rank']), reverse=True)[:1])
            # yield linked_ent
            if linked_ent is not None:
                for f_id2 in linked_ent:
                    if f_id2:
                        with open(OUTFILE, 'a') as fi:
                            fi.write(stringify(warc_id, key, f_id2))


def stringify(warc_id, word, freebase_id):
    return '%s\t%s\t%s\n' % (warc_id, word, freebase_id)


if __name__ == "__main__":
    INFILE = sys.argv[1]
    OUTFILE = sys.argv[2]
    with gzip.open(INFILE, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                if record.http_headers is not None:
                    rec_id = record.rec_headers.get_header('WARC-TREC-ID')
                    html = record.content_stream().read()
                    words = TextExtractor.get_all_words(html.decode("utf-8", errors='ignore'))
                    if len(words) >= 1:
                        ner_linking(words, rec_id)

# spark://node013.cm.cluster:7077