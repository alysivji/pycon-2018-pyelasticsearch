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
            'mappings': {},
            'settings': {},
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
        body={
            "name": product.name,
            "image": product.image,
        }
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
            "_source": {
                "name": product.name,
                "image": product.image,
            }
        }


if __name__ == '__main__':
    main()
