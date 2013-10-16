class DSSeeded(object):
    def __init__(self, spec):
        self.ksize = spec['KSize']
        self.vsize = spec['VSize']
        self.repeat = spec['Repeat']
        self.count = spec['Count']
        self.kseed = spec['KSeed']
        self.vseed = spec['VSeed']
        self.continuous = spec['Continuous']

    def gen_str(self, seed, size, ix):
        curlen = len(seed)
        multiplier = 1
        rep = "{0}{1}".format(self.repeat, ix)
        replen = len(rep)

        while curlen + replen * multiplier < size:
            multiplier += 1

        ret = seed + (rep * multiplier)
        return ret

    def batch_iter(self, startval, nitems, use_values=False):
        if not use_values:
            it = tuple(
                self.gen_str(self.kseed, self.ksize, startval + x)
                             for x in xrange(0, nitems))

        else:
            it = {}
            for x in xrange(startval, startval + nitems):
                key = self.gen_str(self.kseed, self.ksize, x)
                val = self.gen_str(self.vseed, self.vsize, x)
                it[key] = val

        return it

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
