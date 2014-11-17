"""
This module defines some constants and utility classes,
and various other static functions and data
"""

MUTATE_SET     = "MC_DS_MUTATE_SET"
MUTATE_APPEND  = "MC_DS_MUTATE_APPEND"
MUTATE_PREPEND = "MC_DS_MUTATE_PREPEND"
MUTATE_REPLACE = "MC_DS_MUTATE_REPLACE"
VIEW_LOAD = "CB_VIEW_LOAD"
VIEW_QUERY = "CB_VIEW_QUERY"
CBSDKD_MSGFLD_V_INFLATEBASE="InflateContent"
CBSDKD_MSGFLD_V_INFLATECOUNT="InflateLevel"
CBSDKD_MSGFLD_V_KIDENT="KIdent"
CBSDKD_MSGFLD_V_KSEQ="KVSequence"
CBSDKD_MSGFLD_V_DESNAME="DesignName"
CBSDKD_MSGFLD_V_MRNAME="ViewName"


KOP_DELETE = "DELETE"
KOP_TOUCH  = "TOUCH"

DSTYPE_FILE         = "DSTYPE_FILE"
DSTYPE_INLINE       = "DSTYPE_INLINE"
DSTYPE_SEEDED       = "DSTYPE_SEEDED"
DSTYPE_REFERENCE    = "DSTYPE_REFERENCE"

class StatusCodes(object):
    error_types = {
        'SUBSYSf_UNKNOWN'    : 0x1,
        'SUBSYSf_CLUSTER'    : 0x2,
        'SUBSYSf_CLIENT'     : 0x4,
        'SUBSYSf_MEMD'       : 0x8,
        'SUBSYSf_NETWORK'    : 0x10,
        'SUBSYSf_SDKD'       : 0x20,
        'SUBSYSf_KVOPS'      : 0x40,
        'SUBSYSf_VIEWS'      : 0x41,
    }

    error_codes = {
        'SDKD_EINVAL'        : 0x200,
        'SDKD_ENOIMPL'       : 0x300,
        'SDKD_ENOHANDLE'     : 0x400,
        'SDKD_ENODS'         : 0x500,
        'SDKD_ENOREQ'        : 0x600,

        'ERROR_GENERIC'      : 0x100,

        'CLIENT_ETMO'        : 0x200,
        'CLIENT_WOULDHANG'   : 0x300,
        'CLIENT_EDURSPEC'    : 0x400,

        'CLUSTER_EAUTH'      : 0x200,
        'CLUSTER_ENOENT'     : 0x300,

        'MEMD_ENOENT'        : 0x200,
        'MEMD_ECAS'          : 0x300,
        'MEMD_ESET'          : 0x400,
        'MEMD_EVBUCKET'      : 0x500,

        'KVOPS_EMATCH'       : 0x200,

        'VIEWS_MALFORMED'    : 0x200,
        'VIEWS_MISMATCH'     : 0x300,
        'VIEWS_HTTP_ERROR'   : 0x400,
        'VIEWS_HTTP_3XX'     : 0x500,
        'VIEWS_HTTP_4XX'     : 0x600,
        'VIEWS_HTTP_5XX'     : 0x700,
        'VIEWS_EXC_UNEXPECTED' : 0x800
    }

    locals().update(error_codes)
    locals().update(error_types)
