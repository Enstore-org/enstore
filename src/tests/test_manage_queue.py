import unittest
import sys
import string
import threading
import time
import mock
import StringIO
import mpq  # binary search / put
import Trace
import e_errors
import hostaddr
from manage_queue import Request, SortedList, Queue, Atomic_Request_Queue, Request_Queue, compare_priority, compare_value

# utility functions to generate test tickets
CID = 10


def new_id():
    global CID
    CID += 1
    return CID


def cid():
    global CID
    return CID


def mk_ticket(unique_id=0, basepri=0, adminpri=0, work="read_from_hsm",
              external_label="external_label_0", location_cookie=5, storage_group='storage_group_0',
              volume_family="volume_family_0", file_family="file_family_0", wrapper="wrapper_0",
              size_bytes=0, callback_addr=None, comment=None):

    global CID
    if not unique_id:
        unique_id = new_id()
    if unique_id > CID:
        CID = unique_id

    if not basepri:
        basepri = CID

    if not adminpri:
        adminpri = CID

    if not size_bytes:
        size_bytes = CID * 1024

    t1 = {}
    t1["encp"] = {}
    t1['fc'] = {}
    t1['vc'] = {}
    t1['wrapper'] = {}
    t1["times"] = {}
    t1["unique_id"] = unique_id
    t1["encp"]["basepri"] = basepri
    t1['encp']['adminpri'] = adminpri
    t1["times"]["t0"] = time.time()
    t1['work'] = work
    t1['fc']['external_label'] = external_label
    t1['fc']['location_cookie'] = location_cookie
    t1['vc']['storage_group'] = storage_group
    t1['vc']['volume_family'] = volume_family
    t1['vc']['file_family'] = file_family
    t1['vc']['wrapper'] = wrapper
    t1["wrapper"]["size_bytes"] = size_bytes
    t1['callback_addr'] = callback_addr
    t1['comment'] = comment
    return t1


class TestRequest(unittest.TestCase):

    def setUp(self):
        t = mk_ticket()
        self.req = Request(1, 1000, t)

    def test___init__(self):
        self.assertTrue(isinstance(self.req, Request))

    def test___cmp__(self):
        t2 = mk_ticket(2, 2, 2)
        r2 = Request(2, 1000, t2)
        val = self.req.__cmp__(r2)
        self.assertEqual(val, -1)
        val2 = r2.__cmp__(self.req)
        self.assertEqual(val2, 1)
        val3 = r2.__cmp__(r2)
        self.assertEqual(val3, 0)

    def test___repr__(self):
        rep = self.req.__repr__()
        self.assertTrue('<priority 1 value 1000 id ' in rep, rep)

    def test_change_pri(self):
        orig = self.req.pri
        new = orig + 10
        self.req.change_pri(new)
        self.assertEqual(self.req.pri, new)
        self.assertEqual(self.req.ticket['encp']['curpri'], new)


class TestSortedList(unittest.TestCase):

    def setUp(self):
        self.slp = SortedList(compare_priority, 1, 'slp')
        self.slv = SortedList(compare_value, 0, 1, 'slv')
        self.r1 = Request(1, 1000, mk_ticket(1, 1, 1))
        self.r2 = Request(2, 2000, mk_ticket(2, 2, 2))
        self.slp.put(self.r1)
        self.slp.put(self.r2)

    def test___init__(self):
        self.assertTrue(isinstance(self.slp, SortedList))
        self.assertTrue(isinstance(self.slv, SortedList))

    def test_test(self):
        rid, stat = self.slp.test(1)
        self.assertEqual(rid, 1)
        self.assertEqual(stat, e_errors.OK)
        rid, stat = self.slp.test(99)
        self.assertEqual(rid, 0)
        self.assertTrue(stat is None)

    def test_find(self):
        req, stat = self.slp.find(1)
        self.assertEqual(req.unique_id, 1)
        self.assertEqual(stat, e_errors.OK)
        req, stat = self.slp.find(99)
        self.assertTrue(req is None)
        self.assertTrue(stat is None)

    def test_get_tickets(self):
        tickets = self.slp.get_tickets()
        self.assertTrue(len(tickets) > 0)

    def test_put(self):
        tickets = self.slp.get_tickets()
        for rid in self.slp.ids:
            req, stat = self.slp.find(rid)
            self.slv.put(req)
        ptickets = self.slv.get_tickets()
        self.assertTrue(len(tickets) == len(ptickets))

    def test_get_get_next(self):
        r1 = self.slp.get()
        r2 = self.slp.get_next()
        self.assertTrue(isinstance(r1, Request))
        self.assertTrue(isinstance(r2, Request))
        self.assertTrue(r1.__cmp__(r2) != 0)

    def test_update(self):
        old_highest_pri = self.slp.highest_pri
        self.slp.update(True)
        new_highest_pri = self.slp.highest_pri
        self.assertTrue(new_highest_pri > old_highest_pri)

    def test_rm(self):
        r1 = self.slp.get()
        rid = r1.unique_id
        self.slp.rm(r1)
        r1, stat = self.slp.find(rid)
        self.assertTrue(r1 is None)

    def test_delete(self):
        r1 = self.slp.get()
        rid = r1.unique_id
        self.slp.delete(r1)
        r1, stat = self.slp.find(rid)
        self.assertTrue(r1 is None)

    def test_change_pri(self):
        r1 = self.slp.get()
        r1_pri = r1.pri
        r1_id = r1.unique_id
        self.slp.change_pri(r1, r1_pri + 100)
        r2, stat = self.slp.find(r1_id)
        r2_pri = r2.pri
        self.assertTrue(r2_pri == (r1_pri + 100))

    def test_wprint(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.slp.wprint()
            self.assertTrue('LIST LENGTH' in _out.getvalue(), _out.getvalue())
            self.assertTrue('<priority ' in _out.getvalue(), _out.getvalue())

    def test_sprint(self):
        strm = self.slp.sprint()
        self.assertTrue('LIST LENGTH' in strm, strm)
        self.assertTrue('<priority ' in strm, strm)


class TestQueue(unittest.TestCase):

    def setUp(self):
        self.q = Queue()
        self.r_list = []
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='write_to_hsm'))
        self.r_list.append(rq)
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='write_to_hsm'))
        self.r_list.append(rq)
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='write_to_hsm'))
        self.r_list.append(rq)
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='read_from_hsm'))
        self.r_list.append(rq)
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='read_from_hsm'))
        self.r_list.append(rq)
        rq, stat = self.q.put(new_id(), mk_ticket(cid(), work='read_from_hsm'))
        self.r_list.append(rq)

    def test___init__(self):
        self.assertTrue(isinstance(self.q, Queue))

    def test_put_test_test(self):

        # test basic put() and test()
        t0 = mk_ticket(1, work='write_to_hsm')
        r0, stat = self.q.put(1, t0)
        self.assertTrue(isinstance(r0, Request))
        tst_rslt = self.q.test(t0)
        self.assertEqual(
            t0['unique_id'], tst_rslt[0], "%s|%s" %
            (t0['unique_id'], tst_rslt[0]))
        t1 = mk_ticket(work='read_from_hsm')
        r1, stat = self.q.put(1, t1)
        self.assertTrue(isinstance(r1, Request))
        tst_rslt = self.q.test(t1)
        self.assertEqual(
            t1['unique_id'], tst_rslt[0], "%s|%s" %
            (t1['unique_id'], tst_rslt[0]))

        # test putting same ticket twice
        r2, stat = self.q.put(1, t0)
        self.assertTrue(r2 is None)

        # test putting ticket with wrong work type
        r3, stat = self.q.put(1, mk_ticket(work='do_nothing'))
        self.assertTrue(r3 is None)
        self.assertEqual(stat, e_errors.WRONGPARAMETER)

        # verify that test() on ticket not in Queue behaves correctly
        tx = mk_ticket()
        tst_rslt = self.q.test(tx)
        self.assertFalse(tst_rslt[0], tst_rslt)

    def test_wprint(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.q.wprint()
            self.assertTrue(
                'KEY volume_family_0' in _out.getvalue(),
                _out.getvalue())
            self.assertTrue(
                'KEY external_label_0' in _out.getvalue(),
                _out.getvalue())

    def test_sprint(self):
        spr = self.q.sprint()
        self.assertTrue('KEY volume_family_0' in spr, spr)
        self.assertTrue('KEY external_label_0' in spr, spr)

    def test_get_queue(self):
        q1 = self.q.get_queue()
        self.assertTrue(isinstance(q1, list))
        q2 = self.q.get_queue(queue_key='opt')
        self.assertTrue(isinstance(q2, list))
        # test a bad key
        with mock.patch('sys.stderr', new=StringIO.StringIO()) as _out:
            try:
                q3 = self.q.get(queue_key='bad_key')
                self.assertTrue(False)
            except TypeError as KeyError:
                pass

    def test_get(self):
        rslt = self.q.get('volume_family_0')
        self.assertTrue(isinstance(rslt, Request))
        rslt = self.q.get('external_label_0')
        self.assertTrue(isinstance(rslt, Request))
        rslt = self.q.get('non_existent_key')
        self.assertTrue(rslt is None)

    def test_get_next(self):
        rslt = self.q.get_next('volume_family_0')
        self.assertTrue(isinstance(rslt, Request))
        rslt = self.q.get_next('external_label_0')
        self.assertTrue(isinstance(rslt, Request))
        rslt = self.q.get_next('non_existent_key')
        self.assertTrue(rslt is None)

    def test_what_key(self):
        for rq in self.r_list[:3]:
            akey = self.q.what_key(rq)
            self.assertEqual('volume_family_0', akey, akey)
        for rq in self.r_list[3:]:
            akey = self.q.what_key(rq)
            self.assertEqual('external_label_0', akey, akey)
        # note that what_key() doesnt care if request is in Queue
        rq = Request(new_id(), cid(), mk_ticket(cid()))
        akey = self.q.what_key(rq)
        self.assertEqual('external_label_0', akey, akey)

    def test_delete(self):
        # find a req in queue
        req = self.q.find(self.r_list[0].unique_id)
        self.assertTrue(req is not None)
        # delete
        self.q.delete(req)
        # test if still in queue
        req2 = self.q.find(self.r_list[0].unique_id)
        self.assertTrue(req2 is None)

    def test_change_pri(self):
        req = self.r_list[0]
        pri = req.pri
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            req2 = self.q.change_pri(req, pri + 100)
            self.assertTrue(req2.pri == pri + 100)

    def test_find(self):
        req1 = self.q.find(self.r_list[0].unique_id)
        self.assertEqual(req1, self.r_list[0])
        req2 = Request(new_id(), cid(), mk_ticket(cid()))
        req3 = self.q.find(req2.unique_id)
        self.assertTrue(req3 is None)

    def test_update_priority(self):
        u1 = self.q.update_priority()
        self.assertNotEqual({}, u1, u1)


class TestAtomic_Request_Queue(unittest.TestCase):
    def setUp(self):
        self.arq = Atomic_Request_Queue()
        self.r_list = []
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='write_to_hsm'))
        self.r_list.append(rq)
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='write_to_hsm'))
        self.r_list.append(rq)
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='write_to_hsm', storage_group='storage_group_1'))
        self.r_list.append(rq)
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='read_from_hsm', external_label='123', storage_group='123'))
        self.r_list.append(rq)
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='read_from_hsm', external_label='456', storage_group='456'))
        self.r_list.append(rq)
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='read_from_hsm', external_label='456', storage_group='456'))
        self.r_list.append(rq)

    def test___init__(self):
        self.assertTrue(isinstance(self.arq, Atomic_Request_Queue))
        # self.arq.wprint()

    def test_update(self):
        keylist = self.arq.tags.keys.union(self.arq.ref.keys())
        # leaving the debug stuff in here for now
        # to remind me that there may be a bunch of unneeded
        # inserts and deletes in update()
        #import pdb; pdb.set_trace()
        for k in keylist:
            #print "k=%s" % k
            for req in self.r_list:
                #print "req=%s" % req
                ret = self.arq.update(req, k)
                #print "ret output = %s" % ret

    def test_get_tags(self):
        tags = self.arq.get_tags()
        self.assertEqual(tags, self.arq.tags.keys)

    def test_get_sg(self):
        tags = self.arq.get_tags()
        for tag in tags:
            sg = self.arq.get_sg(tag)
            self.assertTrue(sg in tag)

    def test_put_test_find(self):
        rq, stat = self.arq.put(
            new_id(), mk_ticket(
                cid(), work='write_to_hsm'))
        rq2 = self.arq.find(rq.unique_id)
        self.assertEqual(rq, rq2)

    def test_test(self):
        for req in self.r_list:
            rid, stat = self.arq.test(req.ticket)
            self.assertNotEqual(rid, 0)
        tik = mk_ticket()
        rid, stat = self.arq.test(tik)
        self.assertEqual(rid, 0)

    def test_delete(self):
        req = self.arq.find(self.r_list[0].unique_id)
        self.assertTrue(req is not None)
        self.arq.delete(req)
        req2 = self.arq.find(req.unique_id)
        self.assertTrue(req2 is None)

    def test_get(self):
        req = self.arq.get()
        self.assertTrue(isinstance(req, Request))
        req2 = self.arq.get(next=1)
        self.assertNotEqual(req, req2)

    def test_get_queue(self):
        que = self.arq.get_queue()
        # returns a tuple of two lists of dicts
        self.assertTrue(isinstance(que, tuple))
        self.assertTrue(len(que) == 2)
        self.assertTrue(isinstance(que[0], list))
        self.assertTrue(isinstance(que[0][0], dict))

    def test_change_pri(self):
        req = self.arq.find(self.r_list[0].unique_id)
        self.assertTrue(req.pri is not None)
        r2 = req.pri + 100
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.arq.change_pri(req, r2)
        req = self.arq.find(self.r_list[0].unique_id)
        self.assertEqual(req.pri, r2)

    def test_wprint(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.arq.wprint()
            self.assertTrue("LIST LENGTH" in _out.getvalue())
            self.assertTrue("WRITE QUEUE" in _out.getvalue())

    def test_sprint(self):
        spr = self.arq.sprint()
        self.assertTrue("LIST LENGTH" in spr)
        self.assertTrue("WRITE QUEUE" in spr)


class TestRequest_Queue(unittest.TestCase):

    def setUp(self):
        self.rq = Request_Queue()
        self.r_list = []
        req = self.rq.put(mk_ticket(work='write_to_hsm'))
        self.r_list.append(req)
        req = self.rq.put(mk_ticket(work='write_to_hsm'))
        self.r_list.append(req)
        req = self.rq.put(mk_ticket(work='write_to_hsm'))
        self.r_list.append(req)
        req = self.rq.put(mk_ticket(work='read_from_hsm'))
        self.r_list.append(req)
        req = self.rq.put(mk_ticket(work='read_from_hsm'))
        self.r_list.append(req)
        req = self.rq.put(mk_ticket(work='read_from_hsm'))
        self.r_list.append(req)

    def test___init__(self):
        self.assertTrue(isinstance(self.rq, Request_Queue))

    def test_get_tags_and_sg(self):
        tags = self.rq.get_tags()
        self.assertTrue(len(tags) > 0)
        for t in tags:
            sg = self.rq.get_sg(t)
            self.assertNotEqual('', sg)

    def test_put_and_test(self):
        tk = mk_ticket(work='read_from_hsm')
        req = self.rq.put(tk)
        rslt = self.rq.test(tk)
        self.assertEqual(rslt[0], req[0].unique_id)

    def test_find_and_delete_and_change_pri(self):
        req = self.rq.find(self.r_list[0][0].unique_id)
        self.assertTrue(isinstance(req, Request))
        self.rq.delete(req)
        req = self.rq.find(self.r_list[0][0].unique_id)
        self.assertTrue(req is None)
        # test alternate pathway through find()
        req = self.rq.find(self.r_list[-1][0].unique_id)
        self.assertTrue(isinstance(req, Request))
        self.rq.delete(req)
        req = self.rq.find(self.r_list[-1][0].unique_id)
        self.assertTrue(req is None)
        req = self.rq.find(self.r_list[1][0].unique_id)
        self.assertTrue(isinstance(req, Request))
        pri = req.pri
        newpri = pri + 100
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.rq.change_pri(req, newpri)
        req = self.rq.find(self.r_list[1][0].unique_id)
        self.assertEqual(req.pri, newpri)

    def test_start_cycle_and_get_admin_request(self):
        try:
            req = self.rq.get_admin_request()
        except AttributeError:
            pass  # need to start_cycle or get_admin_request fails
        self.rq.start_cycle()
        req = self.rq.get_admin_request()
        self.assertTrue(isinstance(req, Request))

    def test_get(self):
        try:
            req = self.rq.get()
        except AttributeError:
            pass  # need to start_cycle or get()  fails
        self.rq.start_cycle()
        req = self.rq.get()
        req2 = self.rq.get(next=1)
        self.assertNotEqual(req, req2)

    def test_get_queue(self):
        # used by library_manager
        # adm_queue, write_queue, read_queue
        q = self.rq.get_queue()
        self.assertTrue(isinstance(q, tuple))
        self.assertTrue(len(q) == 3)
        self.assertTrue(isinstance(q[0], list))
        self.assertTrue(isinstance(q[1], list))
        self.assertTrue(isinstance(q[2], list))

    def test_wprint(self):
        with mock.patch('sys.stdout', new=StringIO.StringIO()) as _out:
            self.rq.wprint()
            self.assertTrue('ADMIN QUEUE' in _out.getvalue(), _out.getvalue())
            self.assertTrue('WRITE QUEUE' in _out.getvalue(), _out.getvalue())
            self.assertTrue(
                'REGULAR QUEUE' in _out.getvalue(),
                _out.getvalue())

    def test_sprint(self):
        s = self.rq.sprint()
        self.assertTrue('ADMIN QUEUE' in s)
        self.assertTrue('WRITE QUEUE' in s)
        self.assertTrue('REGULAR QUEUE' in s)


if __name__ == "__main__":
    unittest.main()
