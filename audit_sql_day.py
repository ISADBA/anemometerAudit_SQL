#!/usr/bin/python
'''auto audit sql task
'''
import MySQLdb
db = MySQLdb.connect("192.168.11.28","anemometer","anemometerpass","slow_query_log_192_168_11_17")

def run():
	cursor = db.cursor()
	sql = '''UPDATE `global_query_review` aa,( 
	SELECT 
	  a.`checksum`,
	  COUNT(b.`checksum`) AS coun 
	FROM
	  `global_query_review` a,
	  global_query_review_history b 
	WHERE a.`checksum` = b.`checksum` 
	  AND a.audit_status = 'refuse' 
	GROUP BY a.`checksum` 
	HAVING MAX(b.`Query_time_max`) < %s 
	  AND AVG(b.`Lock_time_max`) < %s
	  AND (MAX(b.`Rows_sent_max`) <= 1000) 
	  AND MAX(b.`Rows_examined_max`) < 5000 
	  AND AVG(b.`Bytes_median`) < 5000 
	  AND coun < 5 
	) bb
	SET aa.`audit_status` = 'pass'
	WHERE aa.`checksum` = bb.checksum
	''' % (1,0.01)
	cursor.execute(sql)
        try:
                cursor.execute(sql)
                db.commit()
                print 'Exec sucessful'
        except:
                db.rollback()
                print 'Exec fail'
        db.close()

if __name__=='__main__':
	run()
