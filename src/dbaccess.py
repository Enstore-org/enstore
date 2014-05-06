#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import time
import e_errors
import datetime
import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

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


class DatabaseAccess:
    #
    # class provides basic DB access methods. Owns db connection pool
    #
    def __init__(self,**kwargs):
        self.pool = PooledDB(psycopg2,**kwargs)

    def get_connection(self,timeout=2,retry=10):
        for i in range(retry):
            try:
                return self.pool.connection()
            except Exception, msg:
                if i>=retry-1:
                    raise e_errors.EnstoreError(None,
                                                "Number of retries reached\n" + str(msg),
                                                e_errors.DATABASE_ERROR)
                else:
                    time.sleep(timeout)

    def close(self):
        self.pool.close()

    def query(self,s,cursor_factory=None) :
        colnames,res=self.__query(s,cursor_factory)
        return res

    def query_with_columns(self,s,cursor_factory=None) :
        colnames,res=self.__query(s,cursor_factory)
        return colnames,res

    def __query(self,s,cursor_factory=None) :
        db,cursor=None,None
        try:
            db=self.pool.connection();
            if cursor_factory :
                cursor=db.cursor(cursor_factory=cursor_factory)
            else:
                cursor=db.cursor()
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


    def update(self,s):
        db,cursor=None,None
        try:
            db=self.pool.connection();
            cursor=db.cursor()
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

    def insert(self,s):
        return self.update(s)

    def remove(self,s):
        return self.update(s)

    def delete(self,s):
        return self.remove(s)

    def query_dictresult(self,s):
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

    def query_getresult(self,s):
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


    def query_tuple(self,s):
        return self.query(s)


