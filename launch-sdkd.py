#!/usr/bin/env python

import argparse

ap = argparse.ArgumentParser()

ap.add_argument('-l', '--listen',
                help="Listening port", type=int,
                default=0)

ap.add_argument('-L', '--libpath',
                help="Library path for client (PYTHONPATH)",
                action='append')

ap.add_argument('-P', '--persistent',
                help="Rerun for multiple control connections",
                action='store_true')

ap.add_argument('--client-timeout', type=float,
                help="Default client timeout for operations",
                default=2.5)

ap.add_argument('--pool-size', type=int, default=-1,
                help="Connection pool size to use")



import sys
options = ap.parse_args()
if options.libpath:
    sys.path += options.libpath

from sdkdpycbc.sdkd import SDKD
from sdkdpycbc.handle import Handle
from sdkdpycbc.pool import ConnectionPool

Handle.DEFAULT_TIMEOUT = options.client_timeout
ConnectionPool.POOL_SIZE = options.pool_size

def run_sdkd():
    sdkd = SDKD(options.listen)
    sdkd.start()
    msg = ("SDKD Listening on port {0} "
           "Use -C localhost:{0} on harness").format(sdkd.addr)

    print msg
    sdkd.join()

print "SDKD Running. Use SIGQUIT (^\\) to exit"

run_sdkd()
if options.persistent:
    while True:
        print "Re-running SDKD Server"
        run_sdkd()
