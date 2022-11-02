import unittest
import mock
import StringIO
import mpq
import sys
import os


class Req(object):
    def __init__(self, size, priority):
        self.size = size
        self.priority = priority

    def __repr__(self):
        return "<size=%s, priority=%s>" % (self.size, self.priority)


def compare_priority(r1, r2):
    return -cmp(r1.priority, r2.priority)


def compare_size(r1, r2):
    return cmp(r1.size, r2.size)


class TestMPQ(unittest.TestCase):

    def setUp(self):
        self.reqs = []
        r1 = Req(size=1, priority=1)
        r2 = Req(size=2, priority=2)
        r3 = Req(size=3, priority=3)
        r4 = Req(size=4, priority=4)
        self.reqs.append(r1)
        self.reqs.append(r2)
        self.reqs.append(r3)
        self.reqs.append(r4)
        self.prio_mpq = mpq.MPQ(compare_priority)
        self.size_mpq = mpq.MPQ(compare_size)

    def test___init__(self):
        self.assertTrue(isinstance(self.prio_mpq, mpq.MPQ))
        self.assertTrue(isinstance(self.size_mpq, mpq.MPQ))

    def perform_insort(self):
        for itm in self.reqs:
            self.prio_mpq.insort(itm)
            self.size_mpq.insort(itm)

    def test_insort(self):
        self.perform_insort()
        if os.getenv('DEBUG'):
            print "\nafter insort:\n"
            print "self.reqs: %s\n" % self.reqs
            print "self.prio_mpq: %s\n" % self.prio_mpq
            print "self.size_mpq: %s\n" % self.size_mpq
        self.assertNotEqual(self.prio_mpq[0], self.size_mpq[0])

    def test_bisect(self):
        self.perform_insort()
        req = Req(size=5, priority=5)
        bndx = self.prio_mpq.bisect(req)
        self.assertEqual(bndx, 0)
        bndx = self.size_mpq.bisect(req)
        self.assertEqual(bndx, len(self.size_mpq) - 1)

    def test_remove(self):
        self.perform_insort()
        req = Req(size=5, priority=5)

        # try removing something not in list
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            self.prio_mpq.remove(req)
            self.assertTrue(
                'exceptions.ValueError' in std_out.getvalue(),
                std_out.getvalue())

        # try removing something that is in list
        itm = self.reqs[0]
        self.prio_mpq.remove(itm)
        self.assertNotEqual(len(self.reqs), len(self.prio_mpq))

    def test___nonzero__(self):
        self.assertFalse(self.prio_mpq.__nonzero__())
        self.perform_insort()
        self.assertTrue(self.prio_mpq.__nonzero__())

    def test___repr__(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as std_out:
            self.perform_insort()
            print self.prio_mpq
            self.assertTrue(
                '<size=4, priority=4>' in std_out.getvalue(),
                std_out.getvalue())


if __name__ == "__main__":
    unittest.main()
