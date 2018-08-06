from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.delete(index="mywords", ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                DOC_TYPE: {  # This mapping applies to products.
                    "properties": {  # Just a magic word.
                        "name": {  # The field we want to configure.
                            "type": "text",  # The kind of data we’re working with.
                            "fields": {  # create an analyzed field.
                                "english_analyzed": {  # Name that field `name.english_analyzed`.
                                    "type": "text",  # It’s also text.
                                    "analyzer": "custom_english_analyzer",  # And here’s the analyzer we want to use.
                                }
                            },
                        },
                        "description": {  # The field we want to configure.
                            "type": "text",  # The kind of data we’re working with.
                            "fields": {  # create an analyzed field.
                                "english_analyzed": {  # Name that field `name.english_analyzed`.
                                    "type": "text",  # It’s also text.
                                    "analyzer": "custom_english_analyzer",  # And here’s the analyzer we want to use.
                                }
                            },
                        }
                    }
                }
            },
            "settings": {
                "analysis": {  # magic word.
                    "analyzer": {  # yet another magic word.
                        "custom_english_analyzer": {  # The name of our analyzer.
                            "type": "english",  # The built in analyzer we’re building on.
                            "stopwords": [
                                "made",
                                "_english_",
                            ],  # Our custom stop words, plus the defaults.
                        }
                    }
                }
            },
        },
    )

    # index_product(es, all_products()[0])
    bulk(es, products_to_index())

    # def gendata():
    #     mywords = ['foo', 'bar', 'baz']
    #     for word in mywords:
    #         yield {
    #             "_index": "mywords",
    #             "_type": "document",
    #             "doc": {"word": word},
    #         }

    # bulk(es, gendata())


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=1,
        body={"name": product.name, "image": product.image},
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format("A Great Product"))


def products_to_index():
    for product in all_products():
        yield {
            "_index": INDEX_NAME,
            "_type": DOC_TYPE,
            "_id": product.id,
            "_source": {"name": product.name, "image": product.image, "description": product.description},
        }


if __name__ == "__main__":
    main()
