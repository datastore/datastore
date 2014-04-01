
import hashlib

def hash(tohash):
  '''fast, deterministic hash function'''
  return int(hashlib.sha1(str(tohash)).hexdigest(), 16)
