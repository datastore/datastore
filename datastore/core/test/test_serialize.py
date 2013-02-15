
import unittest

from ..key import Key
from ..basic import DictDatastore
from ..serialize import *
from test_basic import TestDatastore

import pickle
import bson

monkey_patch_bson(bson)



class TestSerialize(TestDatastore):


  def test_basic(self):

    value = 'test_value_%s' % self
    values_raw = [{'value': i} for i in xrange(0, 1000)]
    values_json = map(json.dumps, values_raw)

    # test protocol
    self.assertRaises(NotImplementedError, Serializer.loads, value)
    self.assertRaises(NotImplementedError, Serializer.dumps, value)

    # test non serializer
    self.assertEqual(NonSerializer.loads(value), value)
    self.assertEqual(NonSerializer.dumps(value), value)
    self.assertTrue(NonSerializer.loads(value) is value)
    self.assertTrue(NonSerializer.dumps(value) is value)

    # test generators
    values_serialized = list(serialized_gen(json, values_raw))
    values_deserialized = list(deserialized_gen(json, values_serialized))
    self.assertEqual(values_serialized, values_json)
    self.assertEqual(values_deserialized, values_raw)

    # test stack
    stack = Stack([json, map_serializer, bson])
    values_serialized = map(stack.dumps, values_raw)
    values_deserialized = map(stack.loads, values_serialized)
    self.assertEqual(values_deserialized, values_raw)


  def subtest_serializer_shim(self, serializer, numelems=100):

    child = DictDatastore()
    shim = SerializerShimDatastore(child, serializer=serializer)

    values_raw = [{'value': i} for i in xrange(0, numelems)]

    values_serial = [serializer.dumps(v) for v in values_raw]
    values_deserial = [serializer.loads(v) for v in values_serial]
    self.assertEqual(values_deserial, values_raw)

    for value in values_raw:
      key = Key(value['value'])
      value_serialized = serializer.dumps(value)

      # should not be there yet
      self.assertFalse(shim.contains(key))
      self.assertEqual(shim.get(key), None)

      # put (should be there)
      shim.put(key, value)
      self.assertTrue(shim.contains(key))
      self.assertEqual(shim.get(key), value)

      # make sure underlying DictDatastore is storing the serialized value.
      self.assertEqual(shim.child_datastore.get(key), value_serialized)

      # delete (should not be there)
      shim.delete(key)
      self.assertFalse(shim.contains(key))
      self.assertEqual(shim.get(key), None)

      # make sure manipulating underlying DictDatastore works equally well.
      shim.child_datastore.put(key, value_serialized)
      self.assertTrue(shim.contains(key))
      self.assertEqual(shim.get(key), value)

      shim.child_datastore.delete(key)
      self.assertFalse(shim.contains(key))
      self.assertEqual(shim.get(key), None)

    if serializer is not bson: # bson can't handle non mapping types
      self.subtest_simple([shim], numelems)


  def test_serializer_shim(self):

    self.subtest_serializer_shim(json)
    self.subtest_serializer_shim(prettyjson)
    self.subtest_serializer_shim(pickle)
    self.subtest_serializer_shim(map_serializer)
    self.subtest_serializer_shim(bson)
    self.subtest_serializer_shim(default_serializer) # module default

    self.subtest_serializer_shim(Stack([map_serializer]))
    self.subtest_serializer_shim(Stack([map_serializer, bson]))
    self.subtest_serializer_shim(Stack([json, map_serializer, bson]))
    self.subtest_serializer_shim(Stack([json, map_serializer, bson, pickle]))


  def test_has_interface_check(self):
    self.assertTrue(hasattr(Serializer, 'implements_serializer_interface'))


  def test_interface_check_returns_true_for_valid_serializers(self):
    class S(object):
      def loads(self, foo): return foo
      def dumps(self, foo): return foo

    self.assertTrue(Serializer.implements_serializer_interface(S))
    self.assertTrue(Serializer.implements_serializer_interface(json))
    self.assertTrue(Serializer.implements_serializer_interface(pickle))
    self.assertTrue(Serializer.implements_serializer_interface(Serializer))


  def test_interface_check_returns_false_for_invalid_serializers(self):
    class S1(object):
      pass

    class S2(object):
      def loads(self, foo):
        return foo

    class S3(object):
      def dumps(self, foo):
        return foo

    class S4(object):
      def dumps(self, foo):
        return foo

    class S5(object):
      loads = 'loads'
      dumps = 'dumps'

    self.assertFalse(Serializer.implements_serializer_interface(S1))
    self.assertFalse(Serializer.implements_serializer_interface(S2))
    self.assertFalse(Serializer.implements_serializer_interface(S3))
    self.assertFalse(Serializer.implements_serializer_interface(S4))
    self.assertFalse(Serializer.implements_serializer_interface(S5))


if __name__ == '__main__':
  unittest.main()
