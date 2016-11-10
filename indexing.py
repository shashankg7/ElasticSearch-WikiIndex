# Index the whole wikipedia dump using elasticsearch's python API

from gensim.utils import smart_open
from gensim.corpora.wikicorpus import _extract_pages, filter_wiki
import time
import elasticsearch
import sys
import pdb

es = elasticsearch.Elasticsearch()
WIKI_PATH = sys.argv[1]


# Generator over wikipedia dump to create wikipedia page's title and text
def iter_wiki(dump_file):
    for title, text, pageid in _extract_pages(smart_open(dump_file)):
        text = filter_wiki(text)
        yield title, text

# Indexing part
stream = iter_wiki(WIKI_PATH)
print("-------Starting wikipedia indexing-------------")
try:
    if es.indices.exists('wikipedia'):
        print(es.indices.delete(index='wikipedia'))
    es.indices.create(index = 'wikipedia', body = {"mappings": {"page": {
                    "_source": { "enabled": True }, "properties": {"title": {
                                "type": "string",
                                "similarity" : "BM25"},
                        "text": {"type": "string", "similarity" : "BM25"}}}}})
    print("-----------Index initialized-----------------")
except Exception as e:
    print("Error: Expection " + str(e) + " encountered")

ind = 0
Id = 1
t = time.time()
for title, tokens in stream:
    Id += 1
    es.index(index = 'wikipedia', doc_type = "page", id = Id, body = {
                                "title": title,
                                "text": tokens})
    if Id % 10000 == 0:
        print("%d documents indexed"%Id)
t1 = time.time()
es.indices.refresh('wikipedia')
print("-----------Indexing completed in %f secs ---------"%(t1-t))
print("----------You may enter your query strings to lookup in the index----")
query = raw_input()
while query:
    res = es.search(index='wikipedia', body={"query": {"match":{"text":query}}})
    hits = res['hits']['hits']
    if len(hits) > 0:
        print("Found %d matched, displaying in order"%len(hits))
        for i, match in enumerate(hits):
        	text = match['_source']['text']
		print("Result at %d position is ----------------------"%i)
		print(text.encode('utf-8'))

