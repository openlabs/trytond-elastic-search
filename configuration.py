# -*- coding: utf-8 -*-
"""
    configuration

    :copyright: © 2014 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
import json
import logging

from trytond.model import ModelView, ModelSQL, ModelSingleton, fields
from trytond.transaction import Transaction
from trytond.config import config
from pyes import ES
from pyes.managers import Indices

__all__ = ['Configuration']


class Configuration(ModelSingleton, ModelSQL, ModelView):
    "ElasticSearch Configuration"
    __name__ = 'elasticsearch.configuration'

    servers = fields.Function(fields.Char('Server(s)'), 'get_server')
    index_name = fields.Function(fields.Char('Index Name'), 'get_index_name')
    settings = fields.Text('Settings', required=True)
    settings_updated = fields.Boolean('Setting updated', readonly=True)

    @classmethod
    def get_es_connection(cls, **kwargs):
        """
        Return a PYES connection object that can be reused by other models
        """
        # TODO: Raise an exception if the configuration object is not
        # created ?
        # Or create one on the fly when connection is requested ?
        configuration = cls(1)
        logger = cls.get_logger()

        if not configuration.settings_updated:
            logger.warning('Settings are not updated on index')

        return ES(
            configuration.servers.split(','),
            default_indices=[configuration.index_name],
            **kwargs
        )

    @classmethod
    def get_logger(cls):
        """
        Returns a logger for this module
        """
        return logging.getLogger('trytond.modules.elasticsearch')

    @staticmethod
    def default_settings_updated():
        """
        By default the settings are not updated
        """
        return False

    @staticmethod
    def default_servers():
        """
        Find the server from config and return
        """
        return config.get('elastic_search', 'server_uri')

    def get_server(self, name):
        """
        This getter function for the servers field, uses the servers
        configuration in trytond.conf. It is not a great idea to integrate with
        a server on the fly.
        """
        return self.default_servers()

    @classmethod
    def default_index_name(cls):
        """
        Return the default index from config
        """
        return Transaction().cursor.dbname

    def get_index_name(self, name):
        """
        The name of the index is the name of the current database
        """
        return self.default_index_name()

    @classmethod
    def default_settings(cls):
        """
        Return a set of useful defaults tryton models can use.
        """
        settings = {
            "analysis": {
                "filter": {
                    "name_ngrams": {
                        "max_gram": 10,
                        "type": "edgeNGram",
                        "side": "front",
                        "min_gram": 1
                    },
                    "name_synonyms": {
                        "synonyms_path": "/config/synonyms/nicknames.txt",
                        "type": "synonym"
                    },
                    "name_metaphone": {
                        "replace": False,
                        "type": "phonetic",
                        "encoder": "metaphone"
                    }
                },
                "analyzer": {
                    "name_metaphone": {
                        "filter": [
                            "name_metaphone"
                        ],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "full_name": {
                        "filter": [
                            "standard",
                            "lowercase",
                            "asciifolding"
                        ],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "partial_name": {
                        "filter": [
                            "standard",
                            "lowercase",
                            "asciifolding",
                            "name_synonyms",
                            "name_ngrams"
                        ],
                        "type": "custom",
                        "tokenizer": "standard"
                    },
                    "html_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "char_filter": [
                            "html_strip"
                        ]
                    }
                }
            }
        }
        return json.dumps(settings, indent=4)

    @classmethod
    def __setup__(cls):
        super(Configuration, cls).__setup__()

        # allow buttons
        cls._buttons.update({
            'update_settings': {},
            'refresh_index': {},
        })
        cls._error_messages.update({
            'invalid_json': 'The JSON data is invalid',
        })

    @classmethod
    def validate(cls, records):
        "Validate the records"
        super(Configuration, cls).validate(records)
        for record in records:
            record.check_valid_json()

    def check_valid_json(self):
        """
        Check if it is possible to at least load the JSON
        as a check for its validity
        """
        try:
            json.loads(self.settings)
        except ValueError:
            self.raise_user_error('invalid_json')

    @classmethod
    @ModelView.button
    def update_settings(cls, records):
        """
        Update the settings on Elastic Search.
        """
        config, = records

        conn = config.get_es_connection()
        indices = Indices(conn)
        logger = cls.get_logger()

        if indices.exists_index(config.index_name):
            # Updating an existing index requires closing it and updating
            # it, then reopening the index
            #
            # See: http://www.elasticsearch.org/guide/en/elasticsearch/
            # reference/current
            # /indices-update-settings.html#update-settings-analysis
            logger.info('Index %s already exists' % config.index_name)

            logger.info('Closing Index %s' % config.index_name)
            indices.close_index(config.index_name)

            logger.info('Updating existing Index %s' % config.index_name)
            indices.update_settings(config.index_name, config.settings)

            logger.info('Opening Index %s' % config.index_name)
            indices.open_index(config.index_name)
        else:
            # Create a brand new index
            logger.info(
                'Creating new index %s with settings' % config.index_name
            )
            indices.create_index(config.index_name, config.settings)

        cls.write([records], {'settings_updated': True})

    @classmethod
    @ModelView.button
    def refresh_index(cls, records):
        """
        Refresh the index on Elastic Search
        """
        configuration, = records

        conn = cls.get_es_connection()
        conn.indices.refresh(configuration.index_name)

    @classmethod
    def make_type_name(cls, name):
        """
        Given a name (suually model name), this method converts it into a
        name that can be used for the type in an index. Having "." in the
        type name (like the model name party.party) makes it difficult to write
        safe search expressions.
        """
        return name.replace('.', '_')

    @classmethod
    def write(cls, records, values):
        """
        If the settings is changed, then set the settings_updated as False
        """
        if isinstance(values, (list, tuple)):
            for value_set in values:
                if 'settings' in value_set:
                    value_set['settings_updated'] = False
        else:
            if 'settings' in values:
                values['settings_updated'] = False

        return super(Configuration, cls).write(records, values)
