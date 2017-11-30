#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_redis_timeseries
----------------------------------

Tests for `redis_timeseries` module.
"""

from collections import OrderedDict
from datetime import datetime, timedelta
import pytest
import pytz

import redis_timeseries as timeseries

TEST_GRANULARITIES = OrderedDict([
    ('1m', {'duration': timeseries.minutes(1), 'ttl': timeseries.hours(1)}),
    ('1h', {'duration': timeseries.hours(1), 'ttl': timeseries.days(7)}),
    ('1d', {'duration': timeseries.days(1), 'ttl': timeseries.days(31)}),
])

eastern = pytz.timezone('US/Eastern')


@pytest.fixture
def redis_client():
    import redis
    client = redis.StrictRedis(db=9)
    client.flushdb()
    return client


# Run all baseline tests with and without timezone
@pytest.fixture(params=[None, eastern])
def ts(request, redis_client):
    return timeseries.TimeSeries(redis_client, 'tests', timezone=request.param, granularities=TEST_GRANULARITIES)


@pytest.fixture
def ts_float(redis_client):
    return timeseries.TimeSeries(redis_client, 'float_tests', use_float=True, granularities=TEST_GRANULARITIES)


@pytest.fixture
def ts_timezone(redis_client):
    return timeseries.TimeSeries(redis_client, 'timezone_tests', timezone=eastern, granularities=TEST_GRANULARITIES)


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
    assert ts.get_total_hits('event:123', '1m', 5) == 4


def test_get_total_hits_no_pytz(ts):
    timeseries.pytz, _pytz = None, timeseries.pytz
    now = timeseries.tz_now()
    ts.record_hit('event:123', now - timedelta(minutes=4))
    ts.record_hit('event:123', now - timedelta(minutes=2))
    ts.record_hit('event:123', now - timedelta(minutes=1))
    ts.record_hit('event:123')
    assert ts.get_total_hits('event:123', '1m', 5) == 4
    timeseries.pytz = _pytz


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
    ts = ts_float
    ts.increase('account:123', 1.23)
    assert ts.get_total_hits('account:123', '1m', 1) == 1.23


def test_float_decrease(ts_float):
    ts = ts_float
    ts.increase('account:123', 5)
    ts.decrease('account:123', 2.5)
    assert ts.get_total_hits('account:123', '1m', 1) == 2.5


test_timezone_round_days = [
    (datetime(2017, 1, 16, 4, 59, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 1, 15, 5, tzinfo=pytz.utc)),
    (datetime(2017, 1, 16, 5, 0, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 1, 16, 5, tzinfo=pytz.utc)),
    (datetime(2017, 1, 16, 5, 59, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 1, 16, 5, tzinfo=pytz.utc)),
    (datetime(2017, 7, 16, 3, 59, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 7, 15, 4, tzinfo=pytz.utc)),
    (datetime(2017, 7, 16, 4, 0, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 7, 16, 4, tzinfo=pytz.utc)),
    (datetime(2017, 7, 16, 4, 59, tzinfo=pytz.utc), timeseries.days(1), datetime(2017, 7, 16, 4, tzinfo=pytz.utc)),
    (datetime(2017, 6, 1, 10, tzinfo=pytz.utc), timeseries.hours(6), datetime(2017, 6, 1, 6, tzinfo=pytz.utc)),
    (datetime(2017, 8, 4, 12, 58, tzinfo=pytz.utc), timeseries.hours(1), datetime(2017, 8, 4, 12, tzinfo=pytz.utc)),
    (datetime(2017, 11, 23, 18, 59, 59, tzinfo=pytz.utc), timeseries.minutes(1), datetime(2017, 11, 23, 18, 59, tzinfo=pytz.utc)),
]


@pytest.mark.parametrize('dt, precision, expected', test_timezone_round_days)
def test_round_time_with_tz(dt, precision, expected):
    tz_rounded = timeseries.round_time_with_tz(dt, precision, eastern)
    assert timeseries.unix_to_dt(tz_rounded) == expected


def test_get_total_hits_days(ts_timezone):
    ts = ts_timezone
    ts.record_hit('event:123', datetime(2017, 7, 12, 4, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 13, 4, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 15, 3, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 15, 4, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 15, 5, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 16, 3, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 16, 4, tzinfo=pytz.utc))
    ts.record_hit('event:123', datetime(2017, 7, 16, 5, tzinfo=pytz.utc))

    buckets = ts.get_buckets('event:123', '1d', 5, timestamp=datetime(2017, 7, 17, 0, tzinfo=pytz.utc))

    assert len(buckets) == 5
    assert buckets[0][0] == datetime(2017, 7, 12, 4, tzinfo=pytz.utc)
    assert buckets[1][0] == datetime(2017, 7, 13, 4, tzinfo=pytz.utc)
    assert buckets[2][0] == datetime(2017, 7, 14, 4, tzinfo=pytz.utc)
    assert buckets[3][0] == datetime(2017, 7, 15, 4, tzinfo=pytz.utc)
    assert buckets[4][0] == datetime(2017, 7, 16, 4, tzinfo=pytz.utc)

    assert buckets[0][1] == 1
    assert buckets[1][1] == 1
    assert buckets[2][1] == 1
    assert buckets[3][1] == 3
    assert buckets[4][1] == 2
