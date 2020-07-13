import hashlib


def load_hashtable(path):
    """
    Reads a list of hash digests as generated by the linux tool `sha256sum`.
    Returns a `dict` object with filenames as keys and digests as values.
    """
    result = {}
    with open(path, 'r') as f:
        for line in f:
            hashval, filename = line.split('  ', 1)
            result[filename.rstrip()] = hashval

    return result


class sha256_open:
    """
    Replacement for the `open` function which transparently checks the message
    digest. If there is a mismatch, an exception is raised.

    Currently, limitations are as follows:
    - only binary file reading is supported;
    - only SHA256 digests are supported; and
    - the file must be read fully and sequentially.
    """
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
            assert self._m.hexdigest() == self.hashvalue, 'sha256 checksum mismatch'
        self._f.close()
