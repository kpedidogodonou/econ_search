import json
from pprint import pprint
import os
import time

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
# from sentence_transformers import SentenceTransformer

load_dotenv()


class Search:
    def __init__(self):
        # self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.es = Elasticsearch(
            cloud_id=os.environ['ELASTIC_CLOUD_ID'],
            api_key=os.environ['ELASTIC_API_KEY'],
            timeout=60, max_retries=3, retry_on_timeout=True
        )  # <-- connection options need to be added here
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)

    def create_index(self):
        self.es.indices.delete(index="wbdr_documents", ignore_unavailable=True)
        self.es.indices.create(index="wbdr_documents", mappings={
            'properties': {

                'elser_embedding': {
                    'type': 'sparse_vector',
                }
            }
        }, 
        settings={
            'index': {
                'default_pipeline': 'elser-ingest-pipeline',
            }
        })

    def get_embedding(self, document):
        if len(document["summary"]) == 0:
            embeddings = self.model.encode(document["name"])
            print(f"embeddings generated for document with length {len(document['name'])}")
            return embeddings
        else:
            embeddings = self.model.encode(document['summary'])
            print(f"embeddings generated for document with length {len(document['summary'])}")
            return embeddings

    def insert_document(self, document):
        return self.es.index(index="wbdr_documents", body={
            **document,
            'embedding': self.get_embedding({"summary": document['summary'], "name": document['name']})})

    def insert_documents(self, documents):
        operations = []
        for document in documents:
            operations.append(
                {"index": {"_index": "wbdr_documents", "_id": document["id"]}})
            
            del document["id"] 

            # doc = {**document,
            #        'embedding': self.get_embedding({"summary": document['summary'], "name": document['name']})}
            operations.append(document)
        return self.es.bulk(operations=operations)

    def reindex(self):
        self.create_index()
        # chunk_folder = 'chunks'
        with open('reports.json', 'rt') as f:
            documents = json.loads(f.read())
        chunk_size = 100
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            self.insert_documents(chunk)
            print(f"Inserted chunk {i // chunk_size + 1} of {len(documents) // chunk_size + 1}")
            time.sleep(1)  # To avoid overwhelming the server
        # return self.insert_documents(documents)

    def search(self, **query_args):
        return self.es.search(index="wbdr_documents", **query_args, timeout="60s")

    def retrieve_document(self, id):
        return self.es.get(index="wbdr_documents", id=id)
 
    def deploy_elser(self):
        # download ELSER v2
        self.es.ml.put_trained_model(model_id='.elser_model_2',
                                     input={'field_names': ['text_field']})
        
        # wait until ready
        while True:
            status = self.es.ml.get_trained_models(model_id='.elser_model_2',
                                                   include='definition_status')
            if status['trained_model_configs'][0]['fully_defined']:
                # model is ready
                break
            time.sleep(1)

        # deploy the model
        self.es.ml.start_trained_model_deployment(model_id='.elser_model_2')

        # define a pipeline
        self.es.ingest.put_pipeline(
            id='elser-ingest-pipeline',
            processors=[
                {
                    'inference': {
                        'model_id': '.elser_model_2',
                        'input_output': [
                            {
                                'input_field': 'summary',
                                'output_field': 'elser_embedding',
                            }
                        ]
                    }
                }
            ]
        )