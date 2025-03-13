# See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
class Echo:
    """A class that implements just the write method of a file-like interface.

    This is intended for use by a csv writer.
    """

    def write(self, value):
        return value


class ZipBuffer:
    """A class that implements a zip-file-like interface.

    This is intended for use by a zipfile writer.
    """

    def __init__(self):
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)
        return len(data)

    def flush(self):
        pass

    def take(self):
        buf = self.buf
        self.buf = bytearray()
        return bytes(buf)

    def end(self):
        buf = self.buf
        self.buf = None
        return bytes(buf)
