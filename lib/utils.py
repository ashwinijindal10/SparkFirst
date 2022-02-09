import base64
import string


def encode_value(val: string):
    ascii_val = val.encode("ascii")
    base64_bytes = base64.b64encode(ascii_val)
    return base64_bytes.decode("ascii")


def decode_value(val: string):
    ascii_val = val.encode("ascii")
    base64_bytes = base64.b64decode(ascii_val)
    return base64_bytes.decode("ascii")