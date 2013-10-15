from threading import Lock
from time import time, sleep
from collections import defaultdict

from couchbase import Couchbase, FMT_BYTES
from couchbase.exceptions import CouchbaseError, NotFoundError

from sdkdpycbc.protocol.ds_seed import DSSeeded
from sdkdpycbc.protocol.results import ResultInfo, TimeWindow, Status
from sdkdpycbc.server import Server
from sdkdpycbc.protocol.message import Response

class CommandRunner(object):
    def __init__(self, meth, cmd, dsiter, batchsize=1, kv_is_dict=True):
        self.meth = meth
        self.dsiter = dsiter
        self.cancelled = False
        self.done = False
        self.kv_is_dict = kv_is_dict

        options = cmd.payload.get('Options')

        self._extract_options(options)
        self.results = ResultInfo(self.timeres)

    def _extract_options(self, options):
        if not options:
            self.batchsize = 0
            self.timeres = 0
            self.delay = 0
            return

        self.batchsize = options.get('IterWait', 1)
        self.delay = options.get('DelayMsec', 0)
        if self.delay:
            self.delay /= 1000
        self.timeres = options.get('TimeRes', 2)

    def cancel(self):
        self.cancelled = True

    def run(self):
        while not self.cancelled and not self.done and self.dsiter.next():
            t_begin = time()
            try:
                self._run_one()
                status = Status()

            except NotFoundError:
                status = Status(Status.SUBSYSf_MEMD|Status.MEMD_ENOENT)

            except CouchbaseError as e:
                print e
                status = Status(Status.SUBSYSf_CLIENT|Status.ERROR_GENERIC)

            t_now = time()
            t_duration = t_now - t_begin
            self.results.mark(t_duration, status, nops=self.batchsize)

            if status.raw == 0 and self.delay:
                sleep(self.delay)

    def _run_one(self):
        kviter = self.dsiter.batch_iter(nbatch=self.batchsize,
                                        use_values=self.kv_is_dict)
        self.meth(kviter)
        return True

    def get_payload_dict(self):
        return self.results.make_dict()


class Handle(Server):
    def __init__(self, parent, sock):
        super(Handle, self).__init__(sock)
        self.cb = None
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
            'unlock_gil': True
        }

        self.cb = Couchbase.connect(**kwargs)

    def dispatch_cb_command(self, request):
        s = request.cmdname
        meth = None
        kv_is_dict = False

        dsiter = DSSeeded(request.payload['DS']).mkiter()

        if s == 'MC_DS_MUTATE_SET':
            meth = self.cb.set_multi
            kv_is_dict = True
        elif s == 'MC_DS_GET':
            meth = self.cb.get_multi
        else:
            raise NotImplementedError()

        runner = CommandRunner(meth, request, dsiter, kv_is_dict=kv_is_dict)
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
        if self.hid is not None:
            self.parent.unregister_handle(self.hid)
        super(Handle, self).stop()

    def handle_request(self, request):
        if not self.cb:
            if request.cmdname != 'NEWHANDLE':
                resp = Response.create_err(request,
                                           Status(Status.SUBSYSf_SDKD|
                                                  Status.SDKD_EINVAL,
                                                  "No Handle yet"))
                self.send_response(self.send_response(resp))

            try:
                self._make_handle(request)
                self.send_response(Response(request, {}, Status.new_ok()))
                self.parent.register_handle(request.handle_id, self)

            except CouchbaseError as e:
                self.send_response(Response.create_err(
                    Status(Status.SUBSYSf_CLIENT|Status.ERROR_GENERIC)))

                self.stop()

            return

        if request.cmdname == 'CLOSEHANDLE':
            self.stop()

        else:
            self.dispatch_cb_command(request)
