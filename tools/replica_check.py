#!/usr/bin/env python


from   DBUtils import PooledDB
import  psycopg2


import e_errors
import enstore_functions2
import Trace
import alarm_client
import configuration_client


Q="""
SELECT CASE
         WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0
         ELSE EXTRACT (EPOCH
                       FROM age(now(),pg_last_xact_replay_timestamp()))
       END AS log_delay
"""


if __name__ == "__main__":
    alarm_client.Trace.init("REPLICA_CHECK")
    connectionPool = None
    cursor = None
    db     = None
    try:

        csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                        enstore_functions2.default_port()))
        ac = alarm_client.AlarmClient(csc)

        connectionPool = PooledDB.PooledDB(psycopg2,
                                           maxconnections=1,
                                           blocking=True,
                                           host='localhost',
                                           user='enstore',
                                           database='chimera')
        db = connectionPool.connection()
        cursor = db.cursor()
        cursor.execute(Q)
        res=cursor.fetchall()
        delay = 0
        if res[0][0]:
            delay = int(res[0][0])
        if delay > 18000:
            Trace.alarm(e_errors.ALARM, "Replica is behind by %d seconds"%(delay,))
    except Exception as e:
        Trace.alarm(e_errors.ALARM, "Replica check failed :",e.message)
    finally:
        for item in [cursor, db, connectionPool]:
            if item :
                item.close()



