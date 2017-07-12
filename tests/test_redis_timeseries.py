#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_redis_timeseries
----------------------------------

Tests for `redis_timeseries` module.
"""

from datetime import datetime, timedelta
import pytest

import redis_timeseries as timeseries

TEST_GRANULARITIES = {'1m': {'ttl': timeseries.hours(1), 'duration': timeseries.minutes(1)}}


@pytest.fixture
def redis_client():
    import redis
    client = redis.StrictRedis(db=9)
    client.flushdb()
    return client


@pytest.fixture
def ts(redis_client):
    return timeseries.TimeSeries(redis_client, 'tests', granularities=TEST_GRANULARITIES)


@pytest.fixture
def ts_float(redis_client):
    return timeseries.TimeSeries(redis_client, 'tests', use_float=True, granularities=TEST_GRANULARITIES)


def test_client_connection(ts):
    assert ts.client.ping()


def test_record_hit_basic(ts):
    ts.record_hit('event:123')
    assert ts.get_total_hits('event:123', '1m', 1) == 1


def test_record_hit_count(ts):
    ts.record_hit('event:123', count=5)
    assert ts.get_total_hits('event:123', '1m', 1) == 5


def test_record_hit_datetime(ts):
    now = timeseries.tz_now()
    ts.record_hit('event:123', now - timedelta(minutes=1))
    assert ts.get_total_hits('event:123', '1m', 1, now) == 0
    assert ts.get_total_hits('event:123', '1m', 2, now) == 1


def test_record_hit_chain(ts):
    ts.record_hit('event:123', execute=False)
    ts.record_hit('enter:123', execute=False)
    ts.execute()
    assert ts.get_total_hits('event:123', '1m', 1) == 1
    assert ts.get_total_hits('enter:123', '1m', 1) == 1


def test_get_hits(ts):
    now = timeseries.tz_now()
    ts.record_hit('event:123', now - timedelta(minutes=4))
    ts.record_hit('event:123', now - timedelta(minutes=2))
    ts.record_hit('event:123', now - timedelta(minutes=1))
    ts.record_hit('event:123')
    hits = ts.get_hits('event:123', '1m', 5)
    assert len(hits) == 5
    assert len(hits[0]) == 2
    assert isinstance(hits[0][0], datetime)
    assert isinstance(hits[0][1], int)

    first_event = timeseries.unix_to_dt(timeseries.round_time(now - timedelta(minutes=4), timeseries.minutes(1)))
    assert hits[0][0] == first_event

    assert hits[0][1] == 1
    assert hits[1][1] == 0
    assert hits[2][1] == 1
    assert hits[3][1] == 1
    assert hits[4][1] == 1


def test_get_hits_invalid_count(ts):
    with pytest.raises(ValueError):
        ts.get_hits('event:123', '1m', 100)


def test_get_total_hits(ts):
    now = timeseries.tz_now()
    ts.record_hit('event:123', now - timedelta(minutes=4))
    ts.record_hit('event:123', now - timedelta(minutes=2))
    ts.record_hit('event:123', now - timedelta(minutes=1))
    ts.record_hit('event:123')
    ts.get_total_hits('event:123', '1m', 5) == 4


def test_get_total_hits_no_pytz(ts):
    timeseries.pytz = None
    now = timeseries.tz_now()
    ts.record_hit('event:123', now - timedelta(minutes=4))
    ts.record_hit('event:123', now - timedelta(minutes=2))
    ts.record_hit('event:123', now - timedelta(minutes=1))
    ts.record_hit('event:123')
    ts.get_total_hits('event:123', '1m', 5) == 4


def test_scan_keys(ts):
    ts.record_hit('event:123')
    ts.record_hit('event:456')
    assert ts.scan_keys('1m', 1) == ['event:123', 'event:456']


def test_scan_keys_invalid_count(ts):
    with pytest.raises(ValueError):
        ts.scan_keys('1m', 100)


def test_scan_keys_search(ts):
    ts.record_hit('event:123')
    ts.record_hit('event:456')
    ts.record_hit('enter:123')
    assert ts.scan_keys('1m', 1, 'event:*') == ['event:123', 'event:456']


def test_float_increase(ts_float):
    ts_float.increase('account:123', 1.23)
    assert ts_float.get_total_hits('account:123', '1m', 1) == 1.23


def test_float_decrease(ts_float):
    ts_float.increase('account:123', 5)
    ts_float.decrease('account:123', 2.5)
    assert ts_float.get_total_hits('account:123', '1m', 1) == 2.5
