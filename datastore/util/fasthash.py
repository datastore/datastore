
import smhasher

def hash(tohash):
  '''fast, deterministic hash function'''
  return smhasher.murmur3_x86_64(str(tohash))
