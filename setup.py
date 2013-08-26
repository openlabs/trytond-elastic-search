#!/usr/bin/env python
#This file is part of Tryton.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

from setuptools import setup
import re
import os
import ConfigParser


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

config = ConfigParser.ConfigParser()
config.readfp(open('tryton.cfg'))
info = dict(config.items('tryton'))
for key in ('depends', 'extras_depend', 'xml'):
    if key in info:
        info[key] = info[key].strip().splitlines()
major_version, minor_version, _ = info.get('version', '0.0.1').split('.', 2)
major_version = int(major_version)
minor_version = int(minor_version)

requires = [
    'pyes',
]
for dep in info.get('depends', []):
    if not re.match(r'(ir|res|webdav)(\W|$)', dep):
        requires.append(
            'trytond_%s >= %s.%s, < %s.%s' %
            (dep, major_version, minor_version, major_version,
                minor_version + 1)
        )
requires.append(
    'trytond >= %s.%s, < %s.%s' %
        (major_version, minor_version, major_version, minor_version + 1)
)

setup(
    name='trytond_elastic_search',
    version=info.get('version', '0.0.1'),
    description='Tryton module for Elastic Search',
    long_description=read('README.rst'),
    author='Tryton',
    author_email='info@openlabs.co.in',
    url='https://github.com/openlabs/trytond-elastic-search',
    package_dir={'trytond.modules.elastic_search': '.'},
    packages=[
        'trytond.modules.elastic_search',
        'trytond.modules.elastic_search.tests',
    ],
    package_data={
        'trytond.modules.elastic_search': info.get('xml', [])
            + ['tryton.cfg', 'locale/*.po', 'views/*.xml'],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Legal Industry',
        'Intended Audience :: Manufacturing',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Office/Business',
    ],
    license='GPL-3',
    install_requires=requires,
    zip_safe=False,
    entry_points="""
    [trytond.modules]
    elastic_search = trytond.modules.elastic_search
    """,
    test_suite='tests',
    test_loader='trytond.test_loader:Loader',
)
