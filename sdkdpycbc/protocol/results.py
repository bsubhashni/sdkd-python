from collections import defaultdict
from time import time

from couchbase import exceptions as CBExc

from sdkdpycbc.protocol.constants import StatusCodes

class Status(StatusCodes):
    def __init__(self, code=0, msg=None):
        self.raw = code
        self.msg = msg

    @property
    def code(self):
        return self.raw

    def add_to_dict(self, target):
        target['Status'] = self.raw
        if self.msg:
            target['ErrorString'] = self.msg

    @classmethod
    def new_ok(cls):
        return cls()

    @classmethod
    def from_cbexc(self, exc):
        if isinstance(exc, CBExc.NotFoundError):
            code =  self.SUBSYSf_MEMD|Status.MEMD_ENOENT

        elif isinstance(exc, CBExc.TimeoutError):
            code = self.SUBSYSf_CLIENT|Status.CLIENT_ETMO

        elif exc.__class__ in (CBExc.NetworkError,
                               CBExc.ConnectError,
                               CBExc.CouchbaseNetworkError,
                               CBExc.UnknownHostError):
            code = self.SUBSYSf_NETWORK|Status.ERROR_GENERIC

        elif isinstance(exc, CBExc.NotMyVbucketError):
            code = self.SUBSYSf_MEMD|Status.MEMD_EVBUCKET

        elif isinstance(exc, CBExc.AuthError):
            code = self.SUBSYSf_CLUSTER|Status.CLUSTER_EAUTH

        elif isinstance(exc, CBExc.HTTPError):
            code = self.SUBSYSf_VIEWS|Status.VIEWS_HTTP_ERROR

        else:
            code = self.SUBSYSf_CLIENT|Status.ERROR_GENERIC

        return self(code)


class TimeWindow(object):
    def __init__(self):
        self.min = None
        self.max = None
        self.total_time = 0
        self.count = 0
        self.errors = defaultdict(lambda: 1)

    def mark(self, duration, status):
        self.errors[str(status.raw)] +=  1
        if self.min is None:
            self.min = duration
            self.max = duration

        else:
            if self.min > duration:
                self.min = duration
            if self.max < duration:
                self.max = duration

        self.total_time += duration


class ResultInfo(object):
    def __init__(self, interval):
        self.first_window_begin = 0
        self.last_window_begin = 0
        self.windows = []
        self.interval = interval

    def get_window(self, now):
        if not self.first_window_begin:
            self.first_window_begin = now
            self.last_window_begin = now
            window = TimeWindow()
            self.windows.append(window)
            return window

        if now - self.last_window_begin > self.interval:
            self.last_window_begin = now
            win = TimeWindow()
            self.windows.append(win)

        return self.windows[-1]

    def make_dict(self):
        timings_ret = {
            'Base': int(self.first_window_begin),
            'Step': self.interval,
            'Windows': []
        }

        for win in self.windows:
            windict = {
                'Min': win.min,
                'Max': win.max,
                'Avg': win.count / win.total_time,
                'Count': win.count,
                'Errors': win.errors
            }
            timings_ret['Windows'].append(windict)

        ret = {
            'Timings': timings_ret,
            'Summary': defaultdict(lambda :1)
        }

        for w in self.windows:
            for code, value in w.errors.items():
                ret['Summary'][code] += value

        return ret

    def mark(self, duration, status, now=None, nops=1):
        duration *= 1000 # Sec -> Msec
        if not now:
            now = time()
        win = self.get_window(now)
        for _ in xrange(nops):
            win.mark(duration, status)
