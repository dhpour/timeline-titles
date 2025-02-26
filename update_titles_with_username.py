from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Initialize Elasticsearch connection
es = Elasticsearch("http://192.168.1.36:9200")

def get_all_gemini_title_ids():
    """Get all document IDs from the gemini_titles index."""
    docs = es.search(
        index="gemini_titles", 
        query={"match_all": {}}, 
        _source=False,
        size=1000  # Adjust size as needed
    )
    return [doc['_id'] for doc in docs['hits']['hits']]

def get_screen_name_from_newsarchive(doc_id):
    """Retrieve the user.screen_name for a document from newsarchive_gql."""
    try:
        doc = es.get(
            index="newsarchive_gql",
            id=doc_id,
            _source=["user.screen_name"]
        )
        if doc['found'] and 'user' in doc['_source'] and 'screen_name' in doc['_source']['user']:
            return doc['_source']['user']['screen_name']
        return None
    except Exception as e:
        print(f"Error retrieving document {doc_id}: {e}")
        return None

def update_gemini_title_with_screen_name(doc_id, screen_name):
    """Update a document in gemini_titles with the user's screen_name."""
    try:
        es.update(
            index="gemini_titles",
            id=doc_id,
            body={
                "doc": {
                    "user_screen_name": screen_name
                }
            }
        )
        return True
    except Exception as e:
        print(f"Error updating document {doc_id}: {e}")
        return False

def process_in_batches(ids, batch_size=100):
    """Process documents in batches to avoid overwhelming the ES server."""
    total = len(ids)
    processed = 0
    updated = 0
    
    for i in range(0, total, batch_size):
        batch = ids[i:i+batch_size]
        for doc_id in batch:
            screen_name = get_screen_name_from_newsarchive(doc_id)
            if screen_name:
                success = update_gemini_title_with_screen_name(doc_id, screen_name)
                if success:
                    updated += 1
            processed += 1
            
            # Print progress
            if processed % 10 == 0:
                print(f"Processed {processed}/{total} documents. Updated: {updated}")
                
        # Small pause between batches to reduce load
        time.sleep(1)
    
    return processed, updated

if __name__ == "__main__":
    print("Starting update process...")
    
    # Get all document IDs from gemini_titles
    title_ids = get_all_gemini_title_ids()
    print(f"Found {len(title_ids)} documents in gemini_titles index")
    
    # Process the documents
    processed, updated = process_in_batches(title_ids)
    
    print(f"Update complete. Processed {processed} documents, updated {updated} with screen names.") 