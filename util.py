import os
from contextlib import contextmanager
from tempfile import NamedTemporaryFile


@contextmanager
def FaultTolerantFile(name):
    dirpath, filename = os.path.split(name)
    with NamedTemporaryFile(dir=dirpath, prefix=filename, suffix='.tmp', mode="w", delete=False) as f:
        yield f
        f.flush()
        os.fsync(f)
        f.close()
        os.replace(f.name, name)


def base36encode(number, alphabet='0123456789abcdefghijklmnopqrstuvwxyz'):
    base36 = ''

    if 0 <= number < len(alphabet):
        return alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return base36


def reddit_ids(prefix, start_id):
    cursor = int(start_id, 36)
    while True:
        yield prefix + base36encode(cursor)
        cursor += 1


def update_cursor(cursor, thing_type):
    with FaultTolerantFile("cursor_" + thing_type) as f:
        f.write(cursor)


def read_cursor(thing_type):
    if not os.path.exists("cursor_" + thing_type):
        with open("cursor_" + thing_type, "w"):
            pass
        return "0"
    with open("cursor_" + thing_type, "r") as f:
        return f.read()
