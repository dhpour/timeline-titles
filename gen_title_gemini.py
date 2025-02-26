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
                      "user.screen_name.keyword": ["bbcpersian", "Entekhab_News", "ManotoNews", "GanjiAkbar", "Tasnimnews_Fa", "EtemadOnline", "VOAfarsi", "IranIntl", "indypersian", "RadioFarda_", "AlainPersian", "isna_farsi", "oxus_tv", "teletabnak", "Digiato", "hamshahrinews", "AjaNews_persian"]
                    }
                  }
                }}, 
                _source={"includes": ["full_text", "created_at", "user"]},
                track_total_hits=True,
                size=1500
        ).body
        for doc in docs['hits']['hits']:
            #print(doc['_source']['full_text'])
            yield doc

def get_all_converted_ids():
    docs = es.search(index=os.getenv("GEMINI_EVENTS"), query={"match_all": {}}, _source=False)
    return [doc['_id'] for doc in docs['hits']['hits']]

def set_title(id, title, ftext, created_at, user_screen_name):
    es.index(index=os.getenv("GEMINI_EVENTS"), id=id, body={"title": title, "full_text": ftext, "created_at": created_at, "user_screen_name": user_screen_name})

tweets = SentenceIterator()

model_name = "gemini-2.0-flash-thinking-exp"
model_name = "gemini-2.0-flash"

client = Client(api_key=os.getenv("GOOGLE_API"))


@sleep_and_retry
@limits(calls=1500, period=86400)
@limits(calls=1, period=5)
def generate_title_with_gemini(prompt):
    response = client.models.generate_content(
        model=model_name,
        contents=[prompt]
    )
    return response.text

if __name__ == "__main__":
    prompt = f"""
Please generate a concise, engaging names for the main events in the following text.
Return ONLY the title, nothing else.
The title should be in Persian.

TEXT:

"""
    c = 1
    ids = get_all_converted_ids()

    for tweet in tweets:
        if tweet['_id'] not in ids:
            #print(tweet['_source'])
            new_prompt = prompt + tweet['_source']['full_text'] 
            title = generate_title_with_gemini(new_prompt)
            #print(tweet['_id'])
            #print(tweet['_source']['user'])
            set_title(
                tweet['_id'], 
                title, 
                tweet['_source']['full_text'], 
                tweet['_source']['created_at'],
                tweet['_source']['user']['screen_name'])
            print(c, tweet['_id'], 'recieved title')
            c += 1
