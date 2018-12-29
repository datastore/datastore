
import hashlib


def fhash(tohash):
    '''fast, deterministic hash function'''
    hashstr = str(tohash).encode('utf-8')
    return int(hashlib.sha1(hashstr).hexdigest(), 16)
