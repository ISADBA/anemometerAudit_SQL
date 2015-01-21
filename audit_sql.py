#!/usr/bin/python
'''auto audit sql task
'''
import MySQLdb
db = MySQLdb.connect("192.168.1.1","anemometer","anemometerpass","slow_query_log_192_168_11_17")

def run():
	cursor = db.cursor()
	sql = '''UPDATE `global_query_review` aa,( 
	SELECT 
	  a.`checksum`,
	  MAX(b.`Query_time_max`),
	  AVG(b.`Query_time_pct_95`),
	  AVG(b.`Query_time_median`),
	  AVG(b.`Lock_time_pct_95`),
	  MAX(b.`Rows_sent_max`),
	  AVG(b.`Rows_sent_pct_95`),
	  AVG(b.`Rows_examined_pct_95`),
	  MAX(b.`Rows_examined_max`),
	  AVG(b.`Bytes_median`) 
	FROM
	  `global_query_review` a,
	  global_query_review_history b
	WHERE a.`checksum` = b.`checksum` 
	  and a.audit_status = 'refuse'
	  GROUP BY a.`checksum`
	  HAVING 
	   (
	    MAX(b.`Query_time_max`) < %s
	    OR AVG(b.`Query_time_pct_95`) < %s
	    OR AVG(b.`Query_time_median`) < %s
	  ) 
	  AND AVG(b.`Lock_time_pct_95`) < %s
	  AND (
	    MAX(b.`Rows_sent_max`) < 1000 
	    OR AVG(b.`Rows_sent_pct_95`) < 100
	  ) 
	  AND (
	    AVG(b.`Rows_examined_pct_95`) < 500 
	    OR MAX(b.`Rows_examined_max`) < 2000
	  ) 
	  AND AVG(b.`Bytes_median`) < 1000 
	) bb
	SET aa.`audit_status` = 'pass'
	WHERE aa.`checksum` = bb.checksum
	''' % (0.1,0.08,0.06,0.01)

	sql2 = '''UPDATE 
	  `global_query_review` a 
	SET
	  a.`audit_status` = 'pass' 
	WHERE a.`audit_status` = 'refuse' 
	  AND (a.`sample` LIKE '%show create table%' OR a.`sample` LIKE '%/*!40001 SQL_NO_CACHE */%' OR a.sample LIKE 'explain%');
	'''
	try:
	        cursor.execute(sql)
	        cursor.execute(sql2)	        
	        db.commit()
	        print 'Exec sucessful'
	except:
	        db.rollback()
	        print 'Exec fail'

	db.close()

if __name__=='__main__':
        run()
