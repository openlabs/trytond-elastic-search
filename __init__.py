# -*- coding: utf-8 -*-
"""
    __init__

    :copyright: © 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
from trytond.pool import Pool
from .index import IndexBacklog, DocumentType


def register():
    "Register models to pool"
    Pool.register(
        IndexBacklog,
        DocumentType,
        module="elastic_search", type_="model"
    )
