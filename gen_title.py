from google.genai import Client
from google.genai import types
from ratelimit import limits, sleep_and_retry
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
load_dotenv()

es = Elasticsearch("http://192.168.1.36:9200")
#es.info()

class SentenceIterator:
    def __init__(self):
        pass
    
    def __iter__(self):
        docs = es.search(index="newsarchive_gql", 
                query={"bool": {
                  "must_not": [
                    {
                      "exists": {
                        "field": "in_reply_to_status_id_str"
                      }
                    }
                  ],
                  "filter": {
                    "terms": {
                      "user.screen_name.keyword": ["bbcpersian", "Entekhab_News", "ManotoNews", "GanjiAkbar", "Tasnimnews_Fa", "EtemadOnline", "VOAfarsi", "IranIntl", "indypersian", "RadioFarda_", "AlainPersian", "isna_farsi", "oxus_tv", "teletabnak", "Digiato", "hamshahrinews"]
                    }
                  }
                }}, 
                _source={"includes": ["full_text"]},
                track_total_hits=True,
                size=1500
        ).body
        for doc in docs['hits']['hits']:
            #print(doc['_source']['full_text'])
            yield doc['_id']

def get_all_converted_ids():
    docs = es.search(index="gemini_titles", query={"match_all": {}}, _source=False)
    return [doc['_id'] for doc in docs['hits']['hits']]

def are_docs_new(ids):
    try:
        docs = [{"_id": id, "_source": False} for id in ids]
        
        result = es.mget(
            index="amag_qa_index",  # Replace with your actual index name
            body={"docs": docs}
        )
        
        if result and 'docs' in result:
            # Filter docs that weren't found and map to their IDs
            new_docs = [doc['_id'] for doc in result['docs'] if not doc['found']]
            return new_docs
        
        return []
        
    except Exception as err:
        print('areDocsNew:', err)
        return []

tweets = SentenceIterator()

model_name = "gemini-2.0-flash-thinking-exp"
model_name = "gemini-2.0-flash"

client = Client(api_key=os.getenv("GOOGLE_API"))


@sleep_and_retry
@limits(calls=1500, period=86400)
@limits(calls=15, period=60)
def generate_title_with_gemini(prompt):
    response = client.models.generate_content(
        model=model_name,
        contents=[prompt]
    )
    return response.text

if __name__ == "__main__":
    c = 1
    ids = get_all_converted_ids()

    for tweet in tweets:
        if tweet['_id'] not in ids:
            print(c, tweet)
            c += 1
