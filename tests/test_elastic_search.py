# -*- coding: utf-8 -*-
"""
    test_elastic_search

    Test Funnel

    :copyright: (c) 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
import sys
import os
DIR = os.path.abspath(os.path.normpath(os.path.join(
    __file__,
    '..', '..', '..', '..', '..', 'trytond'))
)
if os.path.isdir(DIR):
    sys.path.insert(0, os.path.dirname(DIR))

import unittest
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


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(IndexBacklogTestCase)
    )
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
