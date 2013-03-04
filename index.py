# -*- coding: utf-8 -*-
"""
    index

    Elastic search by default has indexes and types.

    :copyright: © 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
import json

from pyes import ES
from trytond.model import ModelSQL, ModelView, fields
from trytond.pool import PoolMeta, Pool
from trytond.transaction import Transaction
from trytond.exceptions import UserError
from trytond.config import CONFIG


__all__ = ['IndexBacklog', 'DocumentType',]
__metaclass__ = PoolMeta


class IndexBacklog(ModelSQL, ModelView):
    """
    Index Backlog
    -------------

    This model stores the documents that are yet to be sent to the
    remote full text search index.
    """
    __name__ = "elasticsearch.index_backlog"

    record_model = fields.Char('Record Model', required=True)
    record_id = fields.Integer('Record ID', required=True)

    @classmethod
    def create_from_record(cls, record):
        """
        A convenience create method which can be passed an active record
        and it would be added to the indexing backlog.
        """
        return cls.create({
            'record_model': record.__name__,
            'record_id': record.id,
        })

    @classmethod
    def create_from_records(cls, records):
        """
        A convenience create method which can be passed multiple active
        records and they would all be added to the indexing backlog.
        """
        return [
            cls.create_from_record(record) for record in records
        ]

    @staticmethod
    def _get_es_connection():
        """
        Return a PYES connection
        """
        return ES(CONFIG['elastic_search_server'])

    @staticmethod
    def _build_default_doc(record):
        """
        If a document does not have an `elastic_search_json` method, this
        method tries to build one in lieu.
        """
        return {
            'rec_name': record.rec_name,
        }

    @classmethod
    def update_index(cls):
        conn = cls._get_es_connection()

        for item in cls.search([]):
            Model = Pool().get(item.record_model)

            record = Model(item.record_id)

            try:
                record.create_date
            except UserError, user_error:
                # Record may have been deleted
                conn.delete(
                    Transaction().cursor.dbname,    # Index Name
                    Model.__name__,                 # Document Type
                    item.record_id
                )
                # Delete the item since it has been sent to the index
                cls.delete([item])
                continue

            if hasattr(record, 'elastic_search_json'):
                # A model with the elastic_search_json method
                data = record.elastic_search_json()
            else:
                # A model without elastic_search_json
                data = cls._build_default_doc(record)

            conn.index(
                data,
                Transaction().cursor.dbname,    # Index Name
                record.__name__,                # Document Type
                record.id,                      # ID of the record
            )

            # Delete the item since it has been sent to the index
            cls.delete([item])


class DocumentType(ModelSQL, ModelView):
    """
    Elastic Search Document Type Definition

    This will in future be used for the mapping too.
    """
    __name__ = "elasticsearch.document.type"

    name = fields.Char('Name', required=True)
    active = fields.Boolean('Active', select=True)
    model = fields.Many2One('ir.model', 'Model', required=True, select=True)
    mapping = fields.Text('Mapping', required=True)
    example_mapping = fields.Function(
        fields.Text('Example Mapping'), 'get_example_mapping'
    )

    @staticmethod
    def default_mapping():
        return '{}'

    @staticmethod
    def default_active():
        return True

    def get_example_mapping(self, document_type, name=None):
        """
        Return an example mapping
        """
        sample = {
            'name': {
                'boost': 1.0,
                'index': 'analyzed',
                'store': 'yes',
                'type': 'string',
                "term_vector": "with_positions_offsets",
            },
            'pos': {
                'store': 'yes',
                'type': 'integer',
            }
        }
        return json.dumps(sample, indent=4)

    @classmethod
    def __setup__(cls):
        super(DocumentType, cls).__setup__()

        #TODO: add a unique constraint on model
        cls._buttons.update({
            'refresh_index': {},
            'update_mapping': {},
            'reindex_all_records': {},
            'get_default_mapping': {},
        })

        cls._constraints += [
            ('check_mapping', 'wrong_mapping'),
        ]
        cls._error_messages.update({
            ('wrong_mapping', 'Mapping does not seem to be valid JSON'),
        })

    def check_mapping(self):
        """
        Check if it is possible to at least load the JSON
        as a check for its validity
        """
        try:
            json.loads(self.mapping)
        except:
            return False
        else:
            return True

    @classmethod
    @ModelView.button
    def reindex_all_records(cls, document_types):
        """
        Reindex all of the records in this model

        :param document_types: Document Types
        """
        IndexBacklog = Pool().get('elasticsearch.index_backlog')

        for document_type in document_types:
            Model = Pool().get(document_type.model.model)
            records = Model.search([])

            # Performance speedups
            index_backlog_create = IndexBacklog.create
            model_name = Model.__name__

            for record in map(int, records):
                index_backlog_create({
                    'record_model': model_name,
                    'record_id': record,
                })

    @classmethod
    @ModelView.button
    def refresh_index(cls, document_types):
        """
        Refresh the index on Elastic Search

        :param document_types: Document Types
        """
        IndexBacklog = Pool().get('elasticsearch.index_backlog')

        conn = IndexBacklog._get_es_connection()

        for document_type in document_types:
            conn.indices.refresh(Transaction().cursor.dbname)

    @classmethod
    @ModelView.button
    def get_default_mapping(cls, document_types):
        """
        Tries to get the default mapping from the model object
        """
        for document_type in document_types:
            Model = Pool().get(document_type.model.model)
            if hasattr(Model, 'es_mapping'):
                cls.write(
                    [document_type], {
                        'mapping': json.dumps(Model.es_mapping(), indent=4)
                    }
                )
            else:
                cls.raise_user_error(
                    "Model %s has no mapping specified" % Model.__name__
                )

    @classmethod
    @ModelView.button
    def update_mapping(cls, document_types):
        """
        Update the mapping on the server side
        """
        IndexBacklog = Pool().get('elasticsearch.index_backlog')

        conn = IndexBacklog._get_es_connection()

        for document_type in document_types:
            conn.indices.put_mapping(
                document_type.model.model,  # Type
                {'properties': json.loads(document_type.mapping)},
                [Transaction().cursor.dbname],  # Index
            )
