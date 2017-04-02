===============================
Redis Timeseries
===============================


Time series API built on top of Redis that can be used to store and query
time series statistics. Multiple time granularities can be used to keep
track of different time intervals.


.. image:: https://img.shields.io/pypi/v/redis_timeseries.svg
        :target: https://pypi.python.org/pypi/redis_timeseries

.. image:: https://api.travis-ci.org/ryananguiano/python-redis-timeseries.svg?branch=master
        :target: https://travis-ci.org/ryananguiano/python-redis-timeseries

.. image:: https://readthedocs.org/projects/redis-timeseries/badge/?version=latest
        :target: https://redis-timeseries.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/ryananguiano/python-redis-timeseries/shield.svg
     :target: https://pyup.io/repos/github/ryananguiano/python-redis-timeseries/
     :alt: Updates


* Free software: MIT license
* Documentation: https://redis-timeseries.readthedocs.io.

Install
-------

To install Redis Timeseries, run this command in your terminal:

.. code-block:: console

    $ pip install redis_timeseries


Usage
-----

To initialize the TimeSeries class, you must pass a Redis client to
access the database. You may also override the base key for the time series.

    >>> import redis
    >>> client = redis.StrictRedis()
    >>> ts = TimeSeries(client, base_key='my_timeseries')

To customize the granularities, make sure each granularity has a ``ttl``
and ``duration`` in seconds. You can use the helper functions for
easier definitions.

    >>> my_granularities = {
    ...     '1minute': {'ttl': hours(1), 'duration': minutes(1)},
    ...     '1hour': {'ttl': days(7), 'duration': hours(1)}
    ... }
    >>> ts = TimeSeries(client, granularities=my_granularities)

``.record_hit()`` accepts a key and an optional timestamp and increment
count. It will record the data in all defined granularities.

    >>> ts.record_hit('event:123')
    >>> ts.record_hit('event:123', datetime(2017, 1, 1, 13, 5))
    >>> ts.record_hit('event:123', count=5)

``.record_hit()`` will automatically execute when ``execute=True``. If you
set ``execute=False``, you can chain the commands into a single redis
pipeline. You must then execute the pipeline with ``.execute()``.

    >>> ts.record_hit('event:123', execute=False)
    >>> ts.record_hit('enter:123', execute=False)
    >>> ts.record_hit('exit:123', execute=False)
    >>> ts.execute()

``.get_hits()`` will query the database for the latest data in the
selected granularity. If you want to query the last 3 minutes, you
would query the ``1minute`` granularity with a count of 3. This will return
a list of tuples ``(bucket, count)`` where the bucket is the rounded timestamp.

    >>> ts.get_hits('event:123', '1minute', 3)
    [(datetime(2017, 1, 1, 13, 5), 1), (datetime(2017, 1, 1, 13, 6), 0), (datetime(2017, 1, 1, 13, 7), 3)]

``.get_total_hits()`` will query the database and return only a sum of all
the buckets in the query.

    >>> ts.get_total_hits('event:123', '1minute', 3)
    4

``.scan_keys()`` will return a list of keys that could exist in the
selected range. You can pass a search string to limit the keys returned.
The search string should always have a ``*`` to define the wildcard.

    >>> ts.scan_keys('1minute', 10, 'event:*')
    ['event:123', 'event:456']


Features
--------

* Multiple granularity tracking
* Redis pipeline chaining
* Key scanner
* Easy to integrate with charting packages

Credits
-------

Algorithm copied from `tonyskn/node-redis-timeseries`_

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _`tonyskn/node-redis-timeseries`: https://github.com/tonyskn/node-redis-timeseries
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

