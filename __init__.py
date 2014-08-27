# -*- coding: utf-8 -*-
"""
    __init__

    :copyright: © 2013-2014 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
from trytond.pool import Pool
from index import IndexBacklog, DocumentType
from configuration import Configuration


def register():
    "Register models to pool"
    Pool.register(
        Configuration,
        IndexBacklog,
        DocumentType,
        module="elastic_search", type_="model"
    )
