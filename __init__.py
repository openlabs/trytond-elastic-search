# -*- coding: utf-8 -*-
"""
    __init__


    :copyright: © 2013 by Openlabs Technologies & Consulting (P) Limited
    :license: BSD, see LICENSE for more details.
"""
from trytond.pool import Pool
from .index import *


def register():
    Pool.register(
        IndexBacklog,
        DocumentType,
        Trigger,
        module="elastic_search", type_="model"
    )
