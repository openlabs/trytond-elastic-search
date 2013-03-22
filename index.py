# -*- coding: utf-8 -*-
"""
    index

    Elastic search by default has indexes and types.

    :copyright: © 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
from pyes import ES
from trytond.model import ModelSQL, ModelView, fields
from trytond.pool import Pool
from trytond.transaction import Transaction
from trytond.exceptions import UserError
from trytond.config import CONFIG


class IndexBacklog(ModelSQL, ModelView):
    """
    Index Backlog
    -------------

    This model stores the documents that are yet to be sent to the
    remote full text search index.
    """
    _name = "elasticsearch.index_backlog"
    _description = __doc__

    record_model = fields.Char('Record Model', required=True, select=True)
    record_id = fields.Integer('Record ID', required=True, select=True)

    def create_from_record(self, record):
        """
        A convenience create method which can be passed an active record
        and it would be added to the indexing backlog.
        """
        return self.create({
            'record_model': record._name,
            'record_id': record.id,
        })

    def create_from_records(self, records):
        """
        A convenience create method which can be passed multiple active
        records and they would all be added to the indexing backlog.
        """
        return [
            self.create_from_record(record) for record in records
        ]

    def _get_es_connection(self):
        """
        Return a PYES connection
        """
        return ES(CONFIG.options['elastic_search_server'])

    def _build_default_doc(self, record):
        """
        If a document does not have an `elastic_search_json` method, this
        method tries to build one in lieu.
        """
        return {
            'rec_name': record.rec_name,
        }

    def update_index(self):
        conn = self._get_es_connection()

        for item in self.browse(self.search([])):
            model_obj = Pool().get(item.record_model)

            record = model_obj.browse(item.record_id)

            if not record:
                # Record may have been deleted
                conn.delete(
                    Transaction().cursor.dbname,    # Index Name
                    model_obj._name,                 # Document Type
                    item.record_id
                )
                # Delete the item since it has been sent to the index
                self.delete(item.id)
                continue

            if hasattr(model_obj, 'elastic_search_json'):
                # A model with the elastic_search_json method
                data = model_obj.elastic_search_json(record)
            else:
                # A model without elastic_search_json
                data = self._build_default_doc(record)

            conn.index(
                data,
                Transaction().cursor.dbname,    # Index Name
                record._name,                	# Document Type
                record.id,                      # ID of the record
            )

            # Delete the item since it has been sent to the index
            self.delete(item.id)

IndexBacklog()


class DocumentType(ModelSQL, ModelView):
    """
    Elastic Search Document Type Definition

    This will in future be used for the mapping too.
    """
    _name = "elasticsearch.document.type"
    _description = __doc__

    name = fields.Char('Name', required=True)
    active = fields.Boolean('Active', select=True)
    model = fields.Many2One('ir.model', 'Model', required=True, select=True)

    def __init__(self):
        super(DocumentType, self).__init__()

        #TODO: add a unique constraint on model
        self._buttons.update({
            'refresh_index': {}
        })

    @ModelView.button
    def refresh_index(self, document_types):
        """
        Refresh the index on Elastic Search

        :param document_types: Document Types
        """
        conn = self._get_es_connection()

        for document_type in document_types:
            conn.indices.refresh(Transaction().cursor.dbname)

DocumentType()
