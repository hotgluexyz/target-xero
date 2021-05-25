#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-xero',
    version='1.0.0',
    description='hotglue target for exporting data to Xero API',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_xero'],
    install_requires=[
        'requests==2.20.0',
        'pandas==1.1.3',
        'argparse==1.4.0',
        "singer-python==5.9.0"
    ],
    entry_points='''
        [console_scripts]
        target-xero=target_xero:main
    ''',
    packages=['target_xero']
)
