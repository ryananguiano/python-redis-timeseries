"""Redis Timeseries

Time series API built on top of Redis that can be used to store and query
time series statistics. Multiple time granularities can be used to keep
track of different time intervals.

To initialize the TimeSeries class, you must pass a Redis client to
access the database. You may also override the base key for the time series.

    >>> import redis
    >>> client = redis.StrictRedis()
    >>> ts = TimeSeries(client, base_key='my_timeseries')

To customize the granularities, make sure each granularity has a `ttl`
and `duration` in seconds. You can use the helper functions for
easier definitions.

    >>> my_granularities = {
    ...     '1minute': {'ttl': hours(1), 'duration': minutes(1)},
    ...     '1hour': {'ttl': days(7), 'duration': hours(1)}
    ... }
    >>> ts = TimeSeries(client, granularities=my_granularities)

`.record_hit()` accepts a key and an optional timestamp and increment
count. It will record the data in all defined granularities.

    >>> ts.record_hit('event:123')
    >>> ts.record_hit('event:123', datetime(2017, 1, 1, 13, 5))
    >>> ts.record_hit('event:123', count=5)

`.record_hit()` will automatically execute when `execute=True`. If you
set `execute=False`, you can chain the commands into a single redis
pipeline. You must then execute the pipeline with `.execute()`.

    >>> ts.record_hit('event:123', execute=False)
    >>> ts.record_hit('enter:123', execute=False)
    >>> ts.record_hit('exit:123', execute=False)
    >>> ts.execute()

`.get_hits()` will query the database for the latest data in the
selected granularity. If you want to query the last 3 minutes, you
would query the `1minute` granularity with a count of 3. This will return
a list of tuples `(bucket, count)` where the bucket is the rounded timestamp.

    >>> ts.get_hits('event:123', '1minute', 3)
    [(datetime(2017, 1, 1, 13, 5), 1), (datetime(2017, 1, 1, 13, 6), 0), (datetime(2017, 1, 1, 13, 7), 3)]

`.get_total_hits()` will query the database and return only a sum of all
the buckets in the query.

    >>> ts.get_total_hits('event:123', '1minute', 3)
    4

`.scan_keys()` will return a list of keys that could exist in the
selected range. You can pass a search string to limit the keys returned.
The search string should always have a `*` to define the wildcard.

    >>> ts.scan_keys('1minute', 10, 'event:*')
    ['event:123', 'event:456']

"""

__author__ = 'Ryan Anguiano'
__email__ = 'ryan.anguiano@gmail.com'
__version__ = '0.1.1'


from collections import OrderedDict
from datetime import datetime
from time import mktime
import operator

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
        ('1minute', {'ttl': hours(1), 'duration': minutes(1)}),
        ('5minute', {'ttl': hours(6), 'duration': minutes(5)}),
        ('10minute', {'ttl': hours(12), 'duration': minutes(10)}),
        ('1hour', {'ttl': days(7), 'duration': hours(1)}),
        ('1day', {'ttl': days(7), 'duration': days(1)}),
    ])

    def __init__(self, client, base_key='stats', granularities=None):
        self.client = client
        self.base_key = base_key
        self.granularities = granularities or self.granularities
        self.chain = self.client.pipeline()

    def get_key(self, key, timestamp, granularity):
        ttl = self.granularities[granularity]['ttl']
        timestamp_key = round_time(timestamp, ttl)
        return ':'.join([self.base_key, granularity, str(timestamp_key), str(key)])

    def record_hit(self, key, timestamp=None, count=1, execute=True):
        pipe = self.client.pipeline() if execute else self.chain

        for granularity, props in self.granularities.iteritems():
            hkey = self.get_key(key, timestamp, granularity)
            bucket = round_time(timestamp, props['duration'])

            pipe.hincrby(hkey, bucket, count)
            pipe.expire(hkey, props['ttl'])

        if execute:
            pipe.execute()

    def execute(self):
        self.chain.execute()
        self.chain = self.client.pipeline()

    def get_hits(self, key, granularity, count, timestamp=None):
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

        results = map(parse_result, pipe.execute())
        return zip(buckets, results)

    def get_total_hits(self, *args, **kwargs):
        return sum([hits for bucket, hits in self.get_hits(*args, **kwargs)])

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
        results = reduce(operator.add, pipe.execute())

        parsed = set()
        for result in results:
            for prefix in prefixes:
                result = result.replace(prefix, '')
            parsed.add(result)

        return sorted(parsed)


def parse_result(val):
    if val is None:
        val = 0
    return int(val)


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
        dt = mktime(dt.timetuple())
    return dt


def unix_to_dt(dt):
    if isinstance(dt, (int, float)):
        utc = pytz.utc if pytz else None
        dt = datetime.fromtimestamp(dt, utc)
    return dt
