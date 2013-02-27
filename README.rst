Elastic Search Full Text Search Integration
===========================================

This module allows tryton records of selected models to be exported to
`Elastic Search <http://www.elasticsearch.org/>`_ full text search engine.


Configuration
-------------

1. Add a new configuration line to trytond.conf
   `elastic_search_server=es.openlabs.co.in`
2. Add the models you want to index into document types. `Administration >
   Elastic Search > Document Types`


How it works
------------

The module reuses the trigger system of Tryton to add records that have
changed to an `Index Backlog` table. A tryton CRON task which runs every 1
minute (by default) looks into the backlog index and makes the
corresponding update to elastic search.

Records, that are deleted are deleted from the index.

Defining what information gets indexed
``````````````````````````````````````

By default the only information indexed from a record is the `rec_name` of
the record. If you need more information to be sent, that is possible by
defining a new method called `elastic_search_json` in the model in a
custom module and it will be used instead of just `rec_name`. An example
of such a method in the product model is below.

.. code-block:: python

    def elastic_search_json(self):
        """
        Return a JSON serializable dictionary of values
        that need to be indexed by the search engine
        """
        return {
            'name': self.name,
            'category': self.category.id,
            'category_name': self.category.name,
        }


Known Issues
------------

This module reuses the `ir.trigger` functionality of Tryton and the
following issues of the trigger system affects this module too.

* `Issue 3026 <https://bugs.tryton.org/issue3026>`_: Records sent as first
  argument are incorrect. (Patch Available)
* `Issue 3027 <https://bugs.tryton.org/issue3027>`_: There seems to be no
  way to get the on_write triggers to work for all write that happen to a
  record.


Workarounds
```````````

1. To get around the above issues, the best way is to set your trigger as
   inactive by unchecking the `active` field. (Since trigger is a required
   field in the Document Type specification).
2. Implement the create, write and delete methods in your custom model to
   manually trigger storing to the index backlog. See example below.


.. code-block:: python

    __metaclass__ = PoolMeta

    class Product:
        __name__ = "product.product"


        @classmethod
        def create(cls, values):
            IndexBacklog = Pool().get('elasticsearch.index_backlog')

            # First save the record
            created = super(Product, cls).create(values)

            # Add the record to backlog
            IndexBacklog.create_from_record(created)

            # Return the value
            return created

        @classmethod
        def write(cls, products, values)
            IndexBacklog = Pool().get('elasticsearch.index_backlog')

            # Write to the record
            super(Product, cls).write(products, values)


            # Create the record to backlog
            IndexBacklog.create_from_records(products)

        @classmethod
        def delete(cls, products):
            IndexBacklog = Pool().get('elasticsearch.index_backlog')

            # Add the deleted records also to backlog
            IndexBacklog.create_from_records(products)

            # Delete the record
            super(Product, cls).delete(products)
