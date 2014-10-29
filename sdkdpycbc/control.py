from threading import Lock

from couchbase.connection import Connection
from couchbase import __version__ as CB_VERSION

from sdkdpycbc.server import Server, HandleClosed
from sdkdpycbc.handle import Handle
from sdkdpycbc.protocol.message import Response
from sdkdpycbc.protocol.results import Status
from sdkdpycbc.pool import ConnectionPool

def gen_info_dict():
    return {
        "CAPS" : {
            "VIEWS": True,
            "CANCEL": True,
            "CONTINUOUS": True
        },

        "COMPONENTS" : {
            "LCB": Connection.lcb_version(),
            "SDK": CB_VERSION
        },
        "CONFIG": {
            "CONNCACHE": ConnectionPool.CONNCACHE,
            "POOL_SIZE": ConnectionPool.POOL_SIZE,
            "TIMEOUT": Handle.DEFAULT_TIMEOUT
        }
    }


class Control(Server):
    def __init__(self, *args, **kwargs):
        super(Control, self).__init__(*args)
        self.handles = {}
        self._lock = Lock()
        self.name = "SDKD-CONTROL"

    def register_handle(self, hid, obj):
        self._lock.acquire()
        self.handles[hid] = obj
        self._lock.release()

    def get_handle(self, hid):
        self._lock.acquire()
        try:
            return self.handles[hid]
        except KeyError:
            return None
        finally:
            self._lock.release()

    def unregister_handle(self, hid, handle):
        self._lock.acquire()
        self.handles.pop(hid)
        self._lock.release()

    def handle_request(self, request):
        if request.cmdname == 'INFO':
            r = Response(request, gen_info_dict())
            self.send_response(r)

        elif request.cmdname == 'GOODBYE':
            self.stop()
            raise HandleClosed("Handle closed with GOODBYE")

        elif request.cmdname == 'CANCEL':
            handle = self.get_handle(request.handle_id)
            if handle:
                handle.cancel()
                status = Status(0)
            else:
                status = Status(Status.SUBSYSf_SDKD|Status.SDKD_ENOHANDLE)

            self.send_response(Response(request, {}, status))
        else:
            raise Exception("No such message..")
