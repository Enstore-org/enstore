
import os
import pg
import time

import accounting
import enstore_functions2

SELECT = "select"
FROM = " from "
ALL_COLS = "*"
WHERE = " where "
AND = " and "
OR = ' or '
BETWEEN = ' between '
ORDERBY = ' order by '
DESC = ' desc '
EQUALS = '='
NOTEQUALS = '!='
GREATER = ">"
LESS = "<"
LESSOREQUAL = "<="
GREATEROREQUAL = ">="
OPENCLAUSE = ' ('
CLOSECLAUSE = ') '

class accountingQuery(accounting.accDB):
	
    	def __init__(self, host, dbname, logname='ACC_QUERY'):
	    accounting.accDB.__init__(self, host, dbname, logname)
            self.tables_cache = self.db.get_tables()

        def query(self, qstr):
            return self.db.query(qstr)

        def setup_query(self, tableName, cols):
            # make sure this is a supported table
            if not tableName in self.tables_cache:
                return None
            return "%s %s %s %s"%(SELECT, cols, FROM, tableName)
            
        # support a simple  query like -
        #    select * from <tablename> where a='b' and c='d'...
        def simple_query(self, tableName, keyVals_d=None, cols=ALL_COLS):
            qstr = self.setup_query(tableName, cols)
            if qstr:
                if keyVals_d:
                    keys = keyVals_d.keys()
                    connector = WHERE
                    for key in keys:
                        qstr = "%s%s%s='%s'"%(qstr, connector, key, keyVals_d[key])
                        connector = AND
                return self.query(qstr)
            else:
                return None
            
        # support advanced queries
        #    select * from <tablename> where (a='b' and c>'d') or e='f'...
        def advanced_query(self, tableName, keyVals_l=None, cols=ALL_COLS):
            qstr = self.setup_query(tableName, cols)
            if qstr:
                qstr = "%s%s"%(qstr, WHERE)
                if keyVals_l:
                    last_str = ""
                    for str in keyVals_l:
                        if last_str == EQUALS:
                            qstr = "%s'%s'"%(qstr, str)
                        else:
                            qstr = "%s%s"%(qstr, str)
                        last_str = str
                return self.query(qstr)
            else:
                return None

        # return the data 30 days before today
        def days_ago(self, days):
            func = "date_mii"
            today = time.strftime("%s"%(enstore_functions2.PLOTYEARFMT,),
                                  time.localtime(time.time()))
            qstr = "%s %s('%s', %s)"%(SELECT, func, today, days)
            result = self.query(qstr)
	    if len(result.dictresult()) > 0:
	        return result.dictresult()[0].get(func, None)
	    else:
	        return None

        def close(self):
            self.db.close()
