#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import datetime
import string
import time

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

import Trace
import e_errors

MAX_NUMBER_OF_RETRIES=10
TIME_TO_SLEEP=2

#
# this function converts datetime.datetime key in a list of dictionaries
# to string date representation. Input argument : list of dictionaries
# (e.g. returned by query_dictresult())
#
def sanitize_datetime_values(dictionaries) :
    for item in dictionaries:
        if isinstance(item,psycopg2.extras.RealDictRow):
            for key in item.keys():
                if isinstance(item[key],datetime.datetime):
                    item[key] = item[key].isoformat(' ')
        elif isinstance(item,psycopg2.extras.DictRow):
            for i,v in enumerate(item):
                if isinstance(v,datetime.datetime):
                    item[i] = v.isoformat(' ')
    return dictionaries

def generate_insert_query(table_name,keys):
    """
    Generate insert query given table name
    and list of fields

    :type table_name: :obj:`str`
    :arg table_name: Name of the table to insert into

    :keys: :obj:`list`
    :arg keys: List of column names

    :rtype: :obj:`str` - insert query

    """
    query = """
    INSERT INTO {} ({}) VALUES ({})
    """
    query=query.format(table_name,string.join(keys, ","),(("%s,")*len(keys))[:-1])
    return query

def generate_update_query(table_name,keys):
    """
    Generate update query query given table name
    and list of fields

    :type table_name: :obj:`str`
    :arg table_name: Name of the table to insert into

    :keys: :obj:`list`
    :arg keys: List of column names

    :rtype: :obj:`str` - update query

    """
    query = """
    UPDATE {} SET {}
    """
    query=query.format(table_name,string.join(keys, "=%s,")+"=%s")
    return query

class DatabaseAccess:
    #
    # class provides basic DB access methods. Owns db connection pool
    #
    def __init__(self,**kwargs):
        self.pool = PooledDB(psycopg2,**kwargs)
        self.retries = MAX_NUMBER_OF_RETRIES
        self.timeout = TIME_TO_SLEEP

    def get_connection(self):
        i=self.retries+1
        t=self.timeout
        while i :
            try:
                return self.pool.connection()
            except Exception, msg:
                i -= 1
                if not i:
                    Trace.alarm(e_errors.WARNING, "CONNECTION FAILURE", str(msg))
                    raise e_errors.EnstoreError(None,
                                                "Number of retries {}  reached\n {}".format(self.retries,str(msg)),
                                                e_errors.DATABASE_ERROR)
                else:
                    time.sleep(t)
                    t *= self.timeout

    def close(self):
        self.pool.close()

    def set_retries(self,retries=10):
        """
        Set number of retries in case of database connection failure

        :type retries: :obj:`int`
        :arg retries: Number of retries

        """
        if retries < 0 :
            raise ValueError("Argument to set_retries must be positive integer")

        self.retries=retries

    def get_retries(self):
        """
        Return number of retries in case of database connection failure

        :rtype: :obj:`int` - number of retries
        """
        return self.retries

    def set_timeout(self,timeout=2):
        """
        Set base for exponential timeout in second
        for how long to sleep between databse connection retries

        :type timeout: :obj:`int`
        :arg timeout: Number of seconds

        """
        if timeout < 0 :
            raise ValueError("Argument to set_timeout must be positive integer")
        self.timeout=timeout

    def get_timeout(self):
        """
        Return base for exponential timeout in seconds
        for how long to sleep between databse connection retries

        :rtype: :obj:`int` - value of base timeout in second

        """
        return self.timeout

    def query(self,s,values=None,cursor_factory=None) :
        colnames,res=self.__query(s,values,cursor_factory)
        return res

    def query_with_columns(self,s,values=None,cursor_factory=None) :
        colnames,res=self.__query(s,values,cursor_factory)
        return colnames,res

    def __query(self,s,values=None,cursor_factory=None) :
        db,cursor=None,None
        try:
            db=self.get_connection();
            if cursor_factory :
                cursor=db.cursor(cursor_factory=cursor_factory)
            else:
                cursor=db.cursor()
            if values:
                cursor.execute(s,values)
            else:
                cursor.execute(s)
            colnames=[desc[0] for desc in cursor.description]
            res=cursor.fetchall()
            cursor.close()
            db.close()
            db,cursor=None,None
            return colnames,res
        except psycopg2.Error, msg:
            try:
                for c in (cursor,db):
                    if c:
                        c.close()
            except:
                # if we failed to close just silently ignore the exception
                pass
            db,cursor=None,None
            #
            # propagate exception to caller
            #
            raise e_errors.EnstoreError(None,
                                        str(msg),
                                        e_errors.DATABASE_ERROR)
        except:
            try:
                for c in (cursor,db):
                    if c:
                        c.close()
            except:
                # if we failed to close just silently ignore the exception
                pass
            #
            # propagate exception to caller
            #
            raise


    def update(self,s,values=None):
        db,cursor=None,None
        try:
            db=self.pool.connection();
            cursor=db.cursor()
            if values:
                cursor.execute(s,values)
            else:
                cursor.execute(s)
            db.commit()
            cursor.close()
            db.close()
        except psycopg2.Error, msg:
            try:
                if db:
                    db.rollback()
                for c in (cursor,db):
                    if c:
                        c.close()
            except:
                # if we failed to close just silently ignore the exception
                pass
            db,cursor=None,None
            #
            # propagate exception to caller
            #
            raise e_errors.EnstoreError(None,
                                        str(msg),
                                        e_errors.DATABASE_ERROR)
        except:
            if db:
                db.rollback()
                for c in (cursor,db):
                    if c:
                        c.close()
            #
            # propagate exception to caller
            #
            raise

    def update_returning_result(self,s,values=None):
        db,cursor=None,None
        try:
            db=self.pool.connection();
            cursor=db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            s += " RETURNING *"
            if values:
                cursor.execute(s,values)
            else:
                cursor.execute(s)
            res=cursor.fetchone()
            db.commit()
            cursor.close()
            db.close()
            return res
        except psycopg2.Error, msg:
            try:
                if db:
                    db.rollback()
                for c in (cursor,db):
                    if c:
                        c.close()
            except:
                # if we failed to close just silently ignore the exception
                pass
            db,cursor=None,None
            #
            # propagate exception to caller
            #
            raise e_errors.EnstoreError(None,
                                        str(msg),
                                        e_errors.DATABASE_ERROR)
        except:
            if db:
                db.rollback()
                for c in (cursor,db):
                    if c:
                        c.close()
            #
            # propagate exception to caller
            #
            raise

    def insert(self,s,record=None):
        if record:
            q=generate_insert_query(s,record.keys())
            return self.update(q,record.values())
        else:
            return self.update(s)

    def insert_returning_result(self,s,record=None):
        if record:
            q=generate_insert_query(s,record.keys())
            return self.update_returning_result(q,record.values())
        else:
            return self.update_returning_result(s)

    def remove(self,s,values=None):
        return self.update(s,values)

    def delete(self,s,values=None):
        return self.remove(s,values)

    def query_dictresult(self,s,values=None):
        if values:
            result=self.query(s,values,cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            result=self.query(s,cursor_factory=psycopg2.extras.RealDictCursor)
        #
        # code below converts the result, which is
        # psycopg2.extras.RealDictCursor object into ordinary
        # dictionary. We need it b/c some parts of volume_clerk, file_clerk
        # send the result over the wire to the client, and client
        # chokes on psycopg2.extras.RealDictCursor is psycopg2.extras is not
        # installed on the client side
        #
        res=[]
        for row in result:
            r={}
            for key in row.keys():
                if isinstance(row[key],datetime.datetime):
                    r[key] = row[key].isoformat(' ')
                else:
                    r[key] = row[key]
            res.append(r)
        return res

    def query_getresult(self,s,values=None):
        if values:
            result=self.query(s,values,cursor_factory=psycopg2.extras.DictCursor)
        else:
            result=self.query(s,cursor_factory=psycopg2.extras.DictCursor)
        #
        # code below converts the result, which is
        # psycopg2.extras.DictCursor object into list lists
        # We need it b/c some parts of volume_clerk, file_clerk
        # send the result over the wire to the client, and client
        # chokes on psycopg2.extras.DictCursor is psycopg2.extras is not
        # installed on the client side
        #
        res=[]
        for row in result:
            r=[]
            for item in row:
                if isinstance(item,datetime.datetime):
                    r.append(item.isoformat(' '))
                else:
                    r.append(item)
            res.append(r)
        return res

    def query_tuple(self,s,vlaues):
        return self.query(s,values)



if __name__ == "__main__":
    dbaccess =  DatabaseAccess(maxconnections=100,
                               maxcached=10,
                               blocking=True,
                               host="localhost",
                               port=9999,
                               user="enstore",
                               database="enstoredb")
    #res=dbaccess.query("select count(*) from encp_xfer")

    dbaccess.remove("delete from a")

    a={ "name" : "aaa", "id" : 10}

    dbaccess.insert("a",a)

    res=dbaccess.query("select * from a  where id=%s",(10,))
    print res

    dbaccess.update("update a set name=%s where id=%s",('bbb',10))

    res=dbaccess.query_dictresult("select * from a  where id=%s",(10,))

    print res

    res=dbaccess.remove("delete from a where id=%s",(10,))

    print res



