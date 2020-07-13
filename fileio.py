def load_hashtable(path):
    result = {}
    with open(path, 'r') as f:
        for line in f:
            hashval, filename = line.split('  ', 1)
            result[filename] = hashval

    return result


class Sha256File:
    def __init__(self, path, hashvalue):
        self.path = path
        self.hashvalue = hashvalue

    def __enter__(self):
        self._f = open(self.path, 'br')
        self._m = hashlib.sha256()
        return self

    def read(self, size=-1):
        b = self._f.read(size)
        self._m.update(b)
        return b

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            assert self._m.hexdigest() == self.hashvalue
        self._f.close()
