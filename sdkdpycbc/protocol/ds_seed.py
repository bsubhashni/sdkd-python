from couchbase.user_constants import (FMT_BYTES, FMT_UTF8, FMT_PICKLE, FMT_JSON)
from couchbase.items import Item, ItemOptionDict
import json

# cb.set("key", "value")
# cb.set_multi("key", "value")
#
# item1 = Item(....)
# item2 = Item(...)
# itemlist = ItemOptionDict(...)
# itemlist.add(item1, options_for_item1)
# itemlist.add(item2, options_for_item2)
# ...
# cb.set_multi(itemlist)

ALL_FORMATS = (FMT_BYTES, FMT_UTF8, FMT_PICKLE, FMT_JSON)

class DatatypeItem(Item):
    def __init__(self, ix=-1, use_datatypes=True, **kwargs):
        super(DatatypeItem, self).__init__(**kwargs)
        self.expected_datatype = ALL_FORMATS[(ix % len(ALL_FORMATS)) - 1]
        self.use_datatypes = use_datatypes

    def set_value(self, vstr):
        if not self.use_datatypes:
            self.value = vstr
            return

        if self.expected_datatype == FMT_BYTES:
            self.value = vstr.encode("utf-8")
        else:
            self.value = vstr

    def verify_value(self):
        if not self.use_datatypes:
            return True # No verification

        return self.expected_datatype == self.flags



class DSSeeded(object):

    def __init__(self, spec, schema, use_datatypes=True,  use_schema=True): # Use Datatypes should be false for view population
        self.ksize = spec['KSize']
        self.vsize = spec['VSize']
        self.repeat = spec['Repeat']
        self.count = spec['Count']
        self.kseed = spec['KSeed']
        self.vseed = spec['VSeed']
        self.continuous = spec['Continuous']
        self.use_datatypes = use_datatypes
        if use_schema:
            self.InflateContent = schema[CBSDKD_MSGFLD_V_INFLATEBASE]
            self.InflateLevel = schema[CBSDKD_MSGFLD_V_INFLATELEVEL]

    def gen_str(self, seed, size, ix):
        curlen = len(seed)
        multiplier = 1
        rep = "{0}{1}".format(self.repeat, ix)
        replen = len(rep)

        while curlen + replen * multiplier < size:
            multiplier += 1

        ret = seed + (rep * multiplier)
        return ret

    def gen_json(self, key, ix):
        value = dict()
        value[CBSDKD_MSGFLD_V_KIDENT] = key
        value[CBSDKD_MSGFLD_V_INFLATEBASE] = self.InflateContent
        value[CBSDKD_MSGFLD_V_INFLATELEVEL] = self.InflateLevel
        value[CBSDKD_MSGFLD_V_KSEQ] = ix
        return json.dump(value)

    def batch_iter(self, startval, nitems, use_values=False, view_load=False):
        itmcoll = ItemOptionDict()
        for x in xrange(startval, startval + nitems):
            key = self.gen_str(self.kseed, self.ksize, x)
            itm = DatatypeItem(key=self.gen_str(self.kseed, self.ksize, x),
                               ix=x,
                               use_datatypes=self.use_datatypes)
            options = {}
            if use_values and not(view_load):
                itm.set_value(self.gen_str(self.vseed, self.vsize, x))
                options['format'] = itm.expected_datatype

            elif use_values and view_load:
                itm.set_value(self.gen_json(key, x))

            itmcoll.add(itm, **options)

        return itmcoll

    def mkiter(self):
        return DSIterator(self)


class DSIterator(object):
    def __init__(self, parent):
        self._parent = parent
        self.cur_iter = 0
        self.do_iter = True

    def _on_next(self):
        if not self._parent.continuous and self.cur_iter > self._parent.count:
            self.do_iter = False

        if self._parent.count and self.cur_iter > self._parent.count:
            self.cur_iter = 0

    def batch_iter(self, nbatch=1, use_values=False):
        it = self._parent.batch_iter(self.cur_iter, nbatch, use_values)
        self.cur_iter += nbatch
        return it

    def next(self):
        self._on_next()
        return self.do_iter
