from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
import requests
import json
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
                _source={"includes": ["full_text", "created_at", "user"]},
                track_total_hits=True,
                size=1500
        ).body
        for doc in docs['hits']['hits']:
            #print(doc['_source']['full_text'])
            yield doc

def get_all_converted_ids():
    docs = es.search(index="aya_events", query={"match_all": {}}, _source=False)
    return [doc['_id'] for doc in docs['hits']['hits']]

def set_title(id, title, ftext, created_at, user_screen_name):
    es.index(index="aya_events", id=id, body={"title": title, "full_text": ftext, "created_at": created_at, "user_screen_name": user_screen_name})


tweets = SentenceIterator()

def generate_title_with_aya(prompt):
    url = "http://localhost:1234/v1/chat/completions"  # LM Studio default endpoint
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "max_tokens": 100
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"Error generating title: {e}")
        return None

if __name__ == "__main__":
    prompt = """
Please generate a concise, engaging name for the main event in the following text.
Return ONLY the title, nothing else.
The title should be in Persian.

TEXT:

"""
    c = 1
    ids = get_all_converted_ids()

    for tweet in tweets:
        if tweet['_id'] not in ids:
            new_prompt = prompt + tweet['_source']['full_text'] 
            title = generate_title_with_aya(new_prompt)
            if title:
                set_title(
                    tweet['_id'], 
                    title, 
                    tweet['_source']['full_text'], 
                    tweet['_source']['created_at'],
                    tweet['_source']['user']['screen_name'])
                print(c, tweet['_id'], 'received title')
                c += 1 