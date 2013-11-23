# -*- coding: utf-8 -*-
"""
    test_elastic_search

    Test Funnel

    :copyright: (c) 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
import sys
import time
import os
DIR = os.path.abspath(os.path.normpath(os.path.join(
    __file__,
    '..', '..', '..', '..', '..', 'trytond'))
)
if os.path.isdir(DIR):
    sys.path.insert(0, os.path.dirname(DIR))

import unittest
from pyes import TermQuery
import trytond.tests.test_tryton
from trytond.tests.test_tryton import POOL, DB_NAME, USER, CONTEXT, test_view,\
    test_depends
from trytond.transaction import Transaction
from trytond.config import CONFIG

CONFIG['elastic_search_server'] = "http://localhost:9200"


class IndexBacklogTestCase(unittest.TestCase):
    """
    Tests Index Backlog
    """
    def setUp(self):
        trytond.tests.test_tryton.install_module('elastic_search')
        self.IndexBacklog = POOL.get('elasticsearch.index_backlog')
        self.User = POOL.get('res.user')

    def test0005views(self):
        '''
        Test views.
        '''
        test_view('elastic_search')

    def test0006depends(self):
        '''
        Test depends.
        '''
        test_depends()

    def test_0010_create_IndexBacklog(self):
        """
        Creates index backlog and updates remote elastic search index
        """
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            users = self.User.create([{
                'name': 'user1', 'login': 'user1'
            }, {
                'name': 'user2', 'login': 'user2'
            }])
            # Adds list of active records to IndexBacklog
            self.IndexBacklog.create_from_records(users)
            self.assertEqual(len(self.IndexBacklog.search([])), 2)
            # Updates the remote elastic search index from backlog and deletes
            # the backlog entries
            self.IndexBacklog.update_index()
            self.assertEqual(len(self.IndexBacklog.search([])), 0)


class DocumentTypeTestCase(unittest.TestCase):
    """
    Tests Elastic Search Manage
    """

    def setUp(self):
        trytond.tests.test_tryton.install_module('elastic_search')
        self.IndexBacklog = POOL.get('elasticsearch.index_backlog')
        self.DocumentType = POOL.get('elasticsearch.document.type')
        self.User = POOL.get('res.user')
        self.Model = POOL.get('ir.model')
        self.Trigger = POOL.get('ir.trigger')

    def test0005views(self):
        '''
        Test views.
        '''
        test_view('elastic_search')

    def test0006depends(self):
        '''
        Test depends.
        '''
        test_depends()

    def create_defaults(self):
        user_model, = self.Model.search([('model', '=', 'res.user')])
        dt1, = self.DocumentType.create([{
            'name': 'TestDoc',
            'model': user_model.id,
        }])
        self.assertEqual(dt1.trigger.name, 'elasticsearch_TestDoc')

        dt2, = self.DocumentType.create([{
            'name': 'TestDoc2',
            'model': user_model.id,
        }])
        self.assertEqual(dt2.trigger.name, 'elasticsearch_TestDoc2')

        return {
            'document_type1': dt1,
            'document_type2': dt2,
        }

    def create_users(self):
        return self.User.create([
            {
                'name': 'testuser',
                'login': 'testuser',
                'password': 'testuser',
            },
            {
                'name': 'testuser2',
                'login': 'testuser2',
                'password': 'testuser2',
            }
        ])

    def test_create_update(self):
        '''
        Test registering/unregistering of models for indexing
        '''

        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            defaults = self.create_defaults()
            dt1 = defaults['document_type1']
            dt2 = defaults['document_type2']

            # update document and check if new trigger is created
            trigger1_id = dt1.trigger.id
            self.DocumentType.write([dt1], {'name': 'testdoc'})
            triggers = self.Trigger.search([
                ('id', '=', trigger1_id)
            ])
            self.assertEqual(len(triggers), 0)
            triggers = self.Trigger.search([
                ('name', '=', 'elasticsearch_testdoc')
            ])
            self.assertEqual(len(triggers), 1)

            # remove the model and check trigger
            trigger2_id = dt2.trigger.id
            self.DocumentType.delete([dt2])
            triggers = self.Trigger.search([('id', '=', trigger2_id)])
            self.assertEqual(len(triggers), 0)

    def test_trigger(self):
        '''
        Test if trigger is invoked and do call handler appropriately
        '''
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            self.create_defaults()
            backlog_old_len = self.IndexBacklog.search([], count=True)
            self.create_users()
            backlog_new_len = self.IndexBacklog.search([], count=True)
            self.assertEqual(backlog_old_len + 2, backlog_new_len)

    def test_delete(self):
        '''
        Test if records are deleted from remove elastic server
        '''
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            self.create_defaults()
            users = self.create_users()
            self.assertEqual(len(self.IndexBacklog.search([])), 2)
            self.IndexBacklog.update_index()
            self.assertEqual(len(self.IndexBacklog.search([])), 0)

            time.sleep(2)  # wait for changes to reach search server
            conn = self.IndexBacklog._get_es_connection()
            result = conn.search(query=TermQuery('rec_name', 'testuser'))
            self.assertEqual(len(result), 1)

            self.User.delete(users)
            self.assertEqual(len(self.IndexBacklog.search([])), 2)
            self.IndexBacklog.update_index()
            time.sleep(2)  # wait for changes to reach search server
            result = conn.search(query=TermQuery('rec_name', 'testuser'))
            self.assertEqual(len(result), 0)


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(IndexBacklogTestCase)
    )
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(DocumentTypeTestCase)
    )
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
