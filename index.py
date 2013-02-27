# -*- coding: utf-8 -*-
"""
    index

    Elastic search by default has indexes and types.

    :copyright: © 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
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

    @classmethod
    def __setup__(cls):
        super(DocumentType, cls).__setup__()

        #TODO: add a unique constraint on model
        cls._buttons.update({
            'refresh_index': {}
        })

    @classmethod
    @ModelView.button
    def refresh_index(cls, document_types):
        """
        Refresh the index on Elastic Search

        :param document_types: Document Types
        """
        conn = cls._get_es_connection()

        for document_type in document_types:
            conn.indices.refresh(Transaction().cursor.dbname)
