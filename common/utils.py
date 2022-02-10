import base64
import string
import re


def encode_value(val: string):
    ascii_val = val.encode("ascii")
    base64_bytes = base64.b64encode(ascii_val)
    return base64_bytes.decode("ascii")


def decode_value(val: string):
    ascii_val = val.encode("ascii")
    base64_bytes = base64.b64decode(ascii_val)
    return base64_bytes.decode("ascii")


def get_decodes_string(val: string, fn_decode):
    find_block_pattern = r"\<([\S ]*?)\>"
    # m = re.findall(find_block_pattern, val)
    m = re.sub(find_block_pattern, fn_decode, val)
    return m


def decode_pattern(match_obj, key="Encrypted"):
    if match_obj.group() is not None and \
            len(match_obj.group()[1:-1]) > 0 and \
            len(match_obj.group()[1:-1].split()) > 0 and \
            match_obj.group()[1:-1].split()[0] == key:
        encrypted_val = match_obj.group()[1:-1].split()[1]
        return decode_value(encrypted_val)
