
import unittest
import random

from ..key import Key
from ..key import Namespace


def randomString():
  string = ''
  length = random.randint(0, 50)
  for i in range(0, length):
    string += chr(random.randint(ord('0'), ord('Z')))
  return string


class KeyTests(unittest.TestCase):

  def __subtest_basic(self, string):
    fixedString = Key.removeDuplicateSlashes(string)
    lastNamespace = fixedString.rsplit('/')[-1].split(':')
    ktype = lastNamespace[0] if len(lastNamespace) > 1 else ''
    name = lastNamespace[-1]
    path = fixedString.rsplit('/', 1)[0] + '/' + ktype
    instance = fixedString + ':' + 'c'

    self.assertEqual(Key(string)._string, fixedString)
    self.assertEqual(Key(string), Key(string))
    self.assertEqual(str(Key(string)), fixedString)
    self.assertEqual(repr(Key(string)), "Key('%s')" % fixedString)
    self.assertEqual(Key(string).name, name)
    self.assertEqual(Key(string).type, ktype)
    self.assertEqual(Key(string).instance('c'), Key(instance))
    self.assertEqual(Key(string).path, Key(path))
    self.assertEqual(Key(string), eval(repr(Key(string))))

    self.assertTrue(Key(string).child('a') > Key(string))
    self.assertTrue(Key(string).child('a') < Key(string).child('b'))
    self.assertTrue(Key(string) == Key(string))

    self.assertRaises(TypeError, cmp, Key(string), string)

    split = fixedString.split('/')
    if len(split) > 1:
      self.assertEqual(Key('/'.join(split[:-1])), Key(string).parent)
    else:
      self.assertRaises(ValueError, lambda: Key(string).parent)

    namespace = split[-1].split(':')
    if len(namespace) > 1:
      self.assertEqual(namespace[0], Key(string).type)
    else:
      self.assertEqual('', Key(string).type)


  def test_basic(self):
    self.__subtest_basic('')
    self.__subtest_basic('abcde')
    self.__subtest_basic('disahfidsalfhduisaufidsail')
    self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/')
    self.__subtest_basic(u'4215432143214321432143214321')
    self.__subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/')
    self.__subtest_basic('abcde:fdsfd')
    self.__subtest_basic('disahfidsalfhduisaufidsail:fdsa')
    self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/:')
    self.__subtest_basic(u'4215432143214321432143214321:')
    self.__subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/f:fdaf')


  def test_ancestry(self):
    k1 = Key('/A/B/C')
    k2 = Key('/A/B/C/D')

    self.assertEqual(k1._string, '/A/B/C')
    self.assertEqual(k2._string, '/A/B/C/D')
    self.assertTrue(k1.isAncestorOf(k2))
    self.assertTrue(k2.isDescendantOf(k1))
    self.assertTrue(Key('/A').isAncestorOf(k2))
    self.assertTrue(Key('/A').isAncestorOf(k1))
    self.assertFalse(Key('/A').isDescendantOf(k2))
    self.assertFalse(Key('/A').isDescendantOf(k1))
    self.assertTrue(k2.isDescendantOf(Key('/A')))
    self.assertTrue(k1.isDescendantOf(Key('/A')))
    self.assertFalse(k2.isAncestorOf(Key('/A')))
    self.assertFalse(k1.isAncestorOf(Key('/A')))
    self.assertFalse(k2.isAncestorOf(k2))
    self.assertFalse(k1.isAncestorOf(k1))
    self.assertEqual(k1.child('D'), k2)
    self.assertEqual(k1, k2.parent)
    self.assertEqual(k1.path, k2.parent.path)

  def test_type(self):
    k1 = Key('/A/B/C:c')
    k2 = Key('/A/B/C:c/D:d')

    self.assertRaises(TypeError, k1.isAncestorOf, str(k2))
    self.assertTrue(k1.isAncestorOf(k2))
    self.assertTrue(k2.isDescendantOf(k1))
    self.assertEqual(k1.type, 'C')
    self.assertEqual(k2.type, 'D')
    self.assertEqual(k1.type, k2.parent.type)

  def test_hashing(self):

    def randomKey():
      return Key('/herp/' + randomString() + '/derp')

    keys = {}

    for i in range(0, 200):
      key = randomKey()
      while key in keys.values():
        key = randomKey()

      hstr = str(hash(key))
      self.assertFalse(hstr in keys)
      keys[hstr] = key

    for key in keys.values():
      hstr = str(hash(key))
      self.assertTrue(hstr in keys)
      self.assertEqual(key, keys[hstr])

  def test_random(self):
    keys = set()
    for i in range(0, 1000):
      random = Key.randomKey()
      self.assertFalse(random in keys)
      keys.add(random)
    self.assertEqual(len(keys), 1000)


if __name__ == '__main__':
  unittest.main()
