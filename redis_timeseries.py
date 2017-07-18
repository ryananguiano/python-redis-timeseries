# -*- coding: utf-8 -*-

__author__ = 'Ryan Anguiano'
__email__ = 'ryan.anguiano@gmail.com'
__version__ = '0.1.5'


from collections import OrderedDict
from datetime import datetime
import functools
import operator
import time

try:
    import pytz
except ImportError:  # pragma: no cover
    pytz = None


seconds = lambda i: i
minutes = lambda i: i * seconds(60)
hours = lambda i: i * minutes(60)
days = lambda i: i * hours(24)


class TimeSeries(object):
    granularities = OrderedDict([
        ('1minute', {'duration': minutes(1), 'ttl': hours(1)}),
        ('5minute', {'duration': minutes(5), 'ttl': hours(6)}),
        ('10minute', {'duration': minutes(10), 'ttl': hours(12)}),
        ('1hour', {'duration': hours(1), 'ttl': days(7)}),
        ('1day', {'duration': days(1), 'ttl': days(31)}),
    ])

    def __init__(self, client, base_key='stats', use_float=False, granularities=None):
        self.client = client
        self.base_key = base_key
        self.use_float = use_float
        self.granularities = granularities or self.granularities
        self.chain = self.client.pipeline()

    def get_key(self, key, timestamp, granularity):
        ttl = self.granularities[granularity]['ttl']
        timestamp_key = round_time(timestamp, ttl)
        return ':'.join([self.base_key, granularity, str(timestamp_key), str(key)])

    def increase(self, key, amount, timestamp=None, execute=True):
        pipe = self.client.pipeline() if execute else self.chain

        for granularity, props in self.granularities.items():
            hkey = self.get_key(key, timestamp, granularity)
            bucket = round_time(timestamp, props['duration'])

            self._hincrby(pipe, hkey, bucket, amount)
            pipe.expire(hkey, props['ttl'])

        if execute:
            pipe.execute()

    def decrease(self, key, amount, timestamp=None, execute=True):
        self.increase(key, -1 * amount, timestamp, execute)

    def execute(self):
        results = self.chain.execute()
        self.chain = self.client.pipeline()
        return results

    def get_buckets(self, key, granularity, count, timestamp=None):
        props = self.granularities[granularity]
        if count > (props['ttl'] / props['duration']):
            raise ValueError('Count exceeds granularity limit')

        pipe = self.client.pipeline()
        buckets = []
        bucket = round_time(timestamp, props['duration']) - (count * props['duration'])
        for _ in range(count):
            bucket += props['duration']
            buckets.append(unix_to_dt(bucket))
            pipe.hget(self.get_key(key, bucket, granularity), bucket)

        results = map(self._parse_result, pipe.execute())
        return list(zip(buckets, results))

    def get_total(self, *args, **kwargs):
        return sum([amount for bucket, amount in self.get_buckets(*args, **kwargs)])

    def scan_keys(self, granularity, count, search='*', timestamp=None):
        props = self.granularities[granularity]
        if count > (props['ttl'] / props['duration']):
            raise ValueError('Count exceeds granularity limit')

        hkeys = set()
        prefixes = set()
        bucket = round_time(timestamp, props['duration']) - (count * props['duration'])
        for _ in range(count):
            bucket += props['duration']
            hkeys.add(self.get_key(search, bucket, granularity))
            prefixes.add(self.get_key('', bucket, granularity))

        pipe = self.client.pipeline()
        for key in hkeys:
            pipe.keys(key)
        results = functools.reduce(operator.add, pipe.execute())

        parsed = set()
        for result in results:
            result = result.decode('utf-8')
            for prefix in prefixes:
                result = result.replace(prefix, '')
            parsed.add(result)

        return sorted(parsed)

    def _hincrby(self, pipe, hkey, bucket, amount):
        if self.use_float:
            pipe.hincrbyfloat(hkey, bucket, amount)
        else:
            pipe.hincrby(hkey, bucket, amount)

    def _parse_result(self, val):
        if self.use_float:
            return float(val or 0)
        else:
            return int(val or 0)

    # Deprecated

    def record_hit(self, key, timestamp=None, count=1, execute=True):
        self.increase(key, count, timestamp, execute)

    def remove_hit(self, key, timestamp=None, count=1, execute=True):
        self.decrease(key, count, timestamp, execute)

    get_hits = get_buckets
    get_total_hits = get_total


def round_time(dt, precision):
    seconds = dt_to_unix(dt or tz_now())
    return int((seconds // precision) * precision)


def tz_now():
    if pytz:
        return datetime.utcnow().replace(tzinfo=pytz.utc)
    else:
        return datetime.now()


def dt_to_unix(dt):
    if isinstance(dt, datetime):
        dt = time.mktime(dt.timetuple())
    return dt


def unix_to_dt(dt):
    if isinstance(dt, (int, float)):
        utc = pytz.utc if pytz else None
        dt = datetime.fromtimestamp(dt, utc)
    return dt
