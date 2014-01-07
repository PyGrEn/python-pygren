#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Approved for Public Release; Distribution Unlimited. 13-3052
# ©2014 - The MITRE Corporation. All rights reserved.

import os
import sys

from setuptools import setup, find_packages

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

#readme = open('README.txt').read()
requirements = open('requirements.txt').read()
#history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='pygren',
    version='0.1',
#    description='Python Graph Engine -- distributed graph database.',
#    long_description=readme + '\n\n' + history,
#    author='MITRE Corporation',
#    author_email='freckleton@mitre.org',
#    url='http://TBD',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points = {'console_scripts':['pygren = pygren:main']},
#    license="BSD",
    zip_safe=False,
#    keywords='TBD',
#    classifiers=[
#        'Development Status :: 2 - Pre-Alpha',
#        'Intended Audience :: Developers',
#        'License :: OSI Approved :: BSD License',
#        'Natural Language :: English',
#        "Programming Language :: Python :: 2",
#        'Programming Language :: Python :: 2.6',
#        'Programming Language :: Python :: 2.7',
#        'Programming Language :: Python :: 3',
#        'Programming Language :: Python :: 3.3',
#    ],
#    test_suite='tests',
)
