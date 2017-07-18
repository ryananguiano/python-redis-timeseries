#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'redis',
]

test_requirements = [
    'pytest'
]

setup(
    name='redis_timeseries',
    version='0.1.5',
    description="Timeseries API built on top of Redis",
    long_description=readme + '\n\n' + history,
    author="Ryan Anguiano",
    author_email='ryan.anguiano@gmail.com',
    url='https://github.com/ryananguiano/python-redis-timeseries',
    py_modules=['redis_timeseries'],
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='redis_timeseries',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
