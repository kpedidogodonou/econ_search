import re
from flask import Flask, render_template, request
from search import Search

app = Flask(__name__)

es = Search()


@app.get("/")
def index():
    return render_template("index.html")

@app.post('/')
def handle_search():
    query = request.form.get('query', '')
    filters, parsed_query = extract_filters(query)
    from_ = request.form.get('from_', type=int, default=0)

    results = es.search(
        query={
            'bool': {
                'must': [
                    {
                        'text_expansion': {
                            'elser_embedding': {
                                'model_id': '.elser_model_2',
                                'model_text': parsed_query,
                            }
                        },
                    }
                ],
                **filters,
            }
        },
        aggs={
            # 'doctype-agg': {
            #     'terms': {
            #         'field': 'doctype.keyword',
            #     }
            # },
            'year-agg': {
                'date_histogram': {
                    'field': 'doc_date',
                    'calendar_interval': 'year',
                    'format': 'yyyy',
                },
            },
        },
        size=20,
        from_=from_,
    )
    aggs = {
        # 'Doctype': {
        #     bucket['key']: bucket['doc_count']
        #     for bucket in results['aggregations']['doctype-agg']['buckets']
        # },
        'Year': {
            bucket['key_as_string']: bucket['doc_count']
            for bucket in results['aggregations']['year-agg']['buckets']
            if bucket['doc_count'] > 0
        },
    }
    return render_template('index.html', results=results['hits']['hits'],
                           query=query, from_=from_,
                           total=results['hits']['total']['value'], aggs=aggs)

@app.get("/document/<id>")
def get_document(id):
    document = es.retrieve_document(id)
    title = document['_source']['name']
    paragraphs = document['_source']['content'].split('\n')
    pdf_url = document['_source']['pdfurl']
    source_link = document['_source']['url']
    return render_template('document.html', title=title, paragraphs=paragraphs, pdf_url=pdf_url, source_link=source_link)


@app.cli.command()
def reindex():
    """Regenerate the Elasticsearch index"""
    response = es.reindex()
    print(response)
    print("Indexing complete")
    # print(f"Index with {len(response['items'])} documents created"
    #       f" in {response['took']} ms")


def extract_filters(query):
    filters = []
    filter_regex = r'doctype:([^\s]+(?:\s+[^\s]+)?)\s*'
    m = re.search(filter_regex, query)
    if m:
        filters.append({
            'term': {
                'doctype.keyword': {
                    'value': m.group(1)}
            }
        })
        query = re.sub(filter_regex, '', query)

    filter_regex = f'year:([^\s]+)\s*'
    m = re.search(filter_regex, query)

    if m:
        filters.append({
            'range': {
                'doc_date': {
                    'gte': f'{m.group(1)}||/y',
                    'lte': f'{m.group(1)}||/y',
                }
            }
        })
        query = re.sub(filter_regex, '', query).strip()

    return {'filter': filters}, query


@app.cli.command()
def deploy_elser():
    """Deploy the ELSER v2 model to Elasticsearch"""
    try: 
        es.deploy_elser()
    except Exception as exc:
        print(f"Failed to deploy ELSER: {exc}")
    else:
        print("ELSER deployed.")

 
if __name__ == "__main__":
    app.run(debug=True)