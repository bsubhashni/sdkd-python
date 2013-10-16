from threading import Lock
from time import time, sleep

from couchbase.connection import Connection
from couchbase.exceptions import CouchbaseError
from couchbase import FMT_BYTES, FMT_UTF8

from sdkdpycbc.protocol.ds_seed import DSSeeded
from sdkdpycbc.protocol.results import ResultInfo, TimeWindow, Status
from sdkdpycbc.protocol.message import Response
from sdkdpycbc.server import Server
from sdkdpycbc.pool import ConnectionPool


class CommandRunner(object):
    def __init__(self, pool, meth, cmd, dsiter, batchsize=1, kv_is_dict=True):
        self.meth = meth
        self.dsiter = dsiter
        self.cancelled = False
        self.done = False
        self.kv_is_dict = kv_is_dict
        self.batchsize = 0
        self.timeres = 0
        self.delay = 0
        self.pool = pool
        self._extract_options(cmd.payload.get('Options'))
        self.results = ResultInfo(self.timeres)

    def _extract_options(self, options):
        if not options:
            return

        self.batchsize = options.get('IterWait', 1)
        self.delay = options.get('DelayMsec', 0)
        self.timeres = options.get('TimeRes', 2)

        if self.delay:
            self.delay /= 1000

    def cancel(self):
        self.cancelled = True

    def run(self):
        while not self.cancelled and not self.done and self.dsiter.next():
            self._run_one()
            if self.delay:
                sleep(self.delay)

    def _run_one(self):
        t_begin = time()
        kviter = self.dsiter.batch_iter(nbatch=self.batchsize,
                                        use_values=self.kv_is_dict)

        cb = self.pool.get()
        try:
            self.meth(cb, kviter)
            status = Status()

        except CouchbaseError as e:
            status = Status.from_cbexc(e)

        finally:
            self.pool.put(cb)

        t_now = time()
        t_duration = t_now - t_begin
        self.results.mark(t_duration, status, nops=self.batchsize)

        if status.raw == 0 and self.delay:
            sleep(self.delay)

    def get_payload_dict(self):
        return self.results.make_dict()


class Handle(Server):
    """
    Default timeout for operations
    """
    DEFAULT_TIMEOUT = 2.5


    def __init__(self, parent, sock):
        super(Handle, self).__init__(sock)
        self.pool = None
        self._cur_runner = None
        self._lock = Lock()
        self.parent = parent
        self.hid = None

    def cancel(self):
        self._lock.acquire()
        if self._cur_runner:
            self._cur_runner.cancel()
        self._lock.release()

    def _make_handle(self, request):
        opts = request.payload
        host = opts['Hostname']

        if opts.get('OtherNodes'):
            host = [ host ] + opts['OtherNodes']

        kwargs = {
            'bucket': opts.get('Bucket', 'default'),
            'password': opts.get('Password', None),
            'host': host,
            'unlock_gil': True,
            'timeout': self.DEFAULT_TIMEOUT,
            'default_format': FMT_UTF8
        }

        self.pool = ConnectionPool.allocate_instance(**kwargs)

    def dispatch_cb_command(self, request):
        s = request.cmdname
        meth = None
        kv_is_dict = False

        dsiter = DSSeeded(request.payload['DS']).mkiter()

        if s == 'MC_DS_MUTATE_SET':
            meth = Connection.set_multi
            kv_is_dict = True
        elif s == 'MC_DS_GET':
            meth = Connection.get_multi
        else:
            raise NotImplementedError()

        runner = CommandRunner(self.pool,
                               meth, request, dsiter, kv_is_dict=kv_is_dict)
        self._lock.acquire()
        self._cur_runner = runner
        self._lock.release()

        self._cur_runner.run()

        self._lock.acquire()
        self._cur_runner = None
        self._lock.release()

        payload = runner.get_payload_dict()
        resp = Response(request, payload)
        self.send_response(resp)

    def stop(self):
        if self.pool:
            ConnectionPool.release_instance(self.pool)

        if self.hid is not None:
            self.parent.unregister_handle(self.hid)

        super(Handle, self).stop()

    def _do_bootstrap(self, request):
        """
        The first command for any new handle connection should be a request
        for a new handle
        """
        if request.cmdname != 'NEWHANDLE':
            status = Status(Status.SUBSYSf_SDKD|Status.SDKD_EINVAL,
                            "No handle yet")
            resp = Response.create_err(request, status)
            self.send_response(self.send_response(resp))
            return

        try:
            self._make_handle(request)
            self.send_response(Response(request))
            self.parent.register_handle(request.handle_id, self)

        except CouchbaseError as e:
            status = Status.from_cbexc(e)
            self.send_response(Response.create_err(status))
            self.stop()


    def handle_request(self, request):
        if not self.pool:
            self._do_bootstrap(request)
            return

        if request.cmdname == 'CLOSEHANDLE':
            self.stop()

        else:
            self.dispatch_cb_command(request)
