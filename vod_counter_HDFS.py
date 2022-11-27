# coding=utf-8
#!/opt/py27/bin/python
import sys,os,time,json,pymssql
sys.path.append(os.path.join(os.path.dirname(__file__),'../..','config'))
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import Row,IntegerType,FloatType
from pyspark.sql.functions import udf,date_format,upper,col
from dateutil.parser import *
from datetime import datetime,timedelta
from config2 import Config 
Conf = Config()


# VOD FUNCTIONs COUNTER With Parameter
# Author: Thuy.Duong
# Update: 2017.02.27
# Update Most View by Unit Customers

# CONSTANTs ==========================

sc = Conf.create_sc()
sqlContext = SQLContext(sc)
_params = sys.argv[1:]
# Structure: number of jobs need to run (0:Run all); From DateTime ('now' if getdate, the other: %Y.%M.%D %H); <To DateTime> (optional)

# FUNCTIONs ==========================

def Process_log(res):

    rdd = res.filter(lambda p: 'fields' in p
                        and 'received_at' in p
                        and 'LogId' in p['fields']
                        and 'AppId' in p['fields']
                        and 'AppName' in p['fields']
                        and p['fields']['AppName'] in ('VOD','CHILD','RELAX')
                        and 'Session' in p['fields']
                        and 'CustomerID' in p['fields']
                        and 'Contract' in p['fields']
                        and 'ItemId' in p['fields']
                        and not p['fields']['CustomerID'] == ""
                        and not p['fields']['Contract'] == ""
                        and not p['fields']['Session'] == ""
                        and (
                                p['fields']['LogId'] == '20'
                                or(
                                    'SubMenuId' in p['fields']
                                    and not p['fields']['SubMenuId'] == ""
                                    and not p['fields']['SubMenuId'] is None
                                    and unicode(p['fields']['SubMenuId']).isdigit() == True
                                    and unicode(p['fields']['ItemId']).isdigit() == True
                                    and (
                                        p['fields']['LogId'] == '52'
                                        and 'Duration' in p['fields']
                                        and 'ElapsedTimePlaying' in p['fields']
                                        and 'RealTimePlaying' in p['fields']
                                        and 'ListOnFolder' in p['fields']
                                        and 'ChapterID' in p['fields']
                                        and not str(p['fields']['ListOnFolder']) == 'undefined'
                                        and str(p['fields']['Duration']).replace('.','').isdigit() == True
                                        and not p['fields']['ElapsedTimePlaying'] == None
                                        and str(p['fields']['RealTimePlaying']).replace('.','').replace(',','').isdigit() == True
                                        and p['fields']['ChapterID'].isdigit() == True
                                    )
                                    or
                                    (
                                        p['fields']['LogId'] in ('51','512','55','56','57')
                                    )
                                )
                            )
                        ).map(lambda p: (p['received_at'],p['fields']))

    return sqlContext.createDataFrame(rdd,['Date','fields'])

def Process_log_Kafka(res):

    result = res.where("""  received_at is not null
                                and fields.Session is not null
                                and fields.CustomerID is not null
                                and fields.Contract is not null
                                and fields.AppId is not null
                                and fields.AppName is not null
                                and (fields.AppName in ('VOD','CHILD','RELAX') or fields.AppId in ('21','23','24'))
                                and fields.LogId is not null
                                and fields.ItemId is not null
                                and not fields.CustomerID = ''
                                and not fields.CustomerID = '0'
                                and not fields.Contract = ''
                                and not fields.Contract = '0'
                                and not fields.Mac = ''
                                and ( fields.LogId in ('51','512','55','56','57')
                                      or (fields.LogId = '20')
                                      or (fields.LogId = '52'
                                           and fields.ListOnFolder is not null
                                           and not fields.ListOnFolder = 'undefined'
                                           and not fields.RealTimePlaying = ''
                                           and fields.SubMenuId is not null
                                           and not fields.SubMenuId = ''
                                           and fields.RealTimePlaying is not null
                                           and fields.RealTimePlaying > 0
                                           and fields.ElapsedTimePlaying is not null
                                           and fields.ElapsedTimePlaying > 0
                                           and fields.Duration is not null
                                           and fields.Duration > 0
                                           and fields.ChapterID is not null
                                           )
                                   )
                                """)\
                        .select(date_format('received_at','yyyy-MM-dd HH:mm:ss').alias("Date"),'fields.LogId','fields')
    return result

def folder_rdd(sc):
    
    list_menuid = "3,4,5,6,7,8,15"

    sql_fol = """SELECT MenuID,FolderID FROM Menu_Detail WHERE MenuID in (%s)"""%list_menuid
    fol_rdd = sc.parallelize(Conf.select110(sql_fol), 4)\
                .map(lambda p: Row(int(p[u'MenuID']),int(p[u'FolderID'])
                                ))
    return sqlContext.createDataFrame(fol_rdd,["MenuID","FolderID"])

def GetListFolder(sc):

    sql_list = """SELECT DISTINCT ID,Folder from Mv_PropertiesShowVN UNION ALL SELECT DISTINCT ID,Folder from Tv_PropertiesShowVN """
    fol_rdd = sc.parallelize(Conf.select110(sql_list), 4)\
                .map(lambda p: Row(int(p[u'ID']),int(p[u'Folder'])
                                ))
    return sqlContext.createDataFrame(fol_rdd,["MovieID","Folder"])


def GetAVT(a,b,c):

    # a: RealTimePlaying
    # b: ElapsedTimePlaying
    # c: Duration
    a = float(a)
    b = float(b)
    c = float(c)

    if(b == 0): # Elapsed = 0
        if(a == 0): # Real = 0
            return a
        else:
            if(a > c): # Real > Dura
                return c # Dura
            else:
                return a # Real
    else: # Elap true
        return b #Elapsed

    # try:
    #     if(a >= b):
    #         return float(b)
    #     else:
    #         return float(a)
    # except Exception, e:
    #     return 0

def level(a,c,b):

    # a: RealTimePlaying
    # b: Duration
    # c: ElapsedTimePlaying

    a = GetAVT(a,c,b)

    try:
        b = float(b)
        if b==0:
            return 1
        if 0<a/b<=0.25:
            return 1
        elif 0.25<a/b<=0.5:
            return 2
        elif 0.5<a/b<=0.75:
            return 3
        elif 0.75<a/b:
            return 4
        else:
            return 0
    except Exception, e:
        return 0

def GetPosition(pos):

    try:
        item = int(pos.split(':')[0])
        page = int(pos.split(':')[1])
        if(page == 0):
            return int(pos.split(':')[0])
        else:
            try:
                return int(pos.split(':')[0]) * int(pos.split(':')[1])
            except Exception, e:
                return 0
    except Exception, e:
        try:
            return int(pos)
        except Exception, e:
            return 0

# ===============================================

# ================== FOLDER =====================

def vod_count_by_folder(logs_table):

    print "Start VOD COUNT BY FOLDER"

    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date
                                    ,c.Area 
                                    ,c.UType 
                                    ,l.Folder
                                    ,COUNT(1) AS TotalViews 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                    ,SUM(l.RealTimePlaying) AS TotalDuration 
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,CASE WHEN fields.LogId = 52 THEN 
                                                 CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                           ELSE fields.RealTimePlaying END 
                                                 ELSE 0 END RealTimePlaying
                                            ,v.Folder 
                                        FROM logs r LEFT JOIN _listfolder v ON fields.ItemId = v.MovieID
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date
                                        ,c.Area 
                                        ,c.UType 
                                        ,l.Folder
                                ORDER BY l.Date
                                        ,c.Area 
                                        ,c.UType 
                                        ,l.Folder
                            """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.Area,p.UType,p.Folder,p.TotalViews,p.TotalCustomers,p.TotalDuration))
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_CountByFolder (Date,Area,UType,Folder,TotalView,TotalCustomer,TotalDuration) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

def vod_count_by_folder_level(logs_table):

    print "Start VOD COUNT BY FOLDER LEVEL"

    # # Log from Elasticsearch
    
    f = udf(level, IntegerType())
    logs = logs_table.where("fields.LogId = 52")\
                     .withColumn('Level', f(logs_table.fields.RealTimePlaying,logs_table.fields.ElapsedTimePlaying,logs_table.fields.Duration))\
                     .registerTempTable('logs')

    result = sqlContext.sql("""SELECT l.Date 
                                    ,c.Area 
                                    ,c.UType 
                                    ,l.Level 
                                    ,l.Folder 
                                    ,COUNT(1) AS TotalViews 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                    ,SUM(l.RealTimePlaying) AS TotalDuration 
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                ELSE fields.RealTimePlaying END RealTimePlaying
                                            ,v.Folder 
                                            ,Level
                                        FROM logs r LEFT JOIN _listfolder v ON fields.ItemId = v.MovieID
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date 
                                        ,c.Area 
                                        ,c.UType 
                                        ,l.Level 
                                        ,l.Folder
                                ORDER BY l.Date 
                                        ,c.Area 
                                        ,c.UType 
                                        ,l.Level 
                                        ,l.Folder
                            """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.Area,p.UType,p.Level,p.Folder,p.TotalViews,p.TotalCustomers,round(p.TotalDuration),2))

    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_CountByFolderLevel
                            (Date,Area,UType,Level,Folder,TotalView,TotalCustomer,TotalDuration)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()    

# ================= MOVIEID =====================

def vod_count_by_movieid_chapter(logs_table,_date):

    print "Start VOD COUNT BY MOVIEID AND CHAPTER"

    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date 
                                    ,c.UType 
                                    ,l.ItemId 
                                    ,l.ChapterID 
                                    ,COUNT(1) AS TotalViews 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                    ,SUM(l.RealTimePlaying) AS TotalDuration 
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,fields.ItemId
                                            ,fields.ChapterID
                                            ,CASE WHEN fields.LogId = 52 THEN 
                                                 CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                           ELSE fields.RealTimePlaying END 
                                                 ELSE 0 END RealTimePlaying
                                              --,CASE WHEN CAST(fields.ElapsedTimePlaying AS float)>0 THEN fields.ElapsedTimePlaying ELSE 
                                              --    CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                              --         ELSE fields.RealTimePlaying END 
                                              --END RealTimePlaying
                                        FROM logs 
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date 
                                        ,c.UType 
                                        ,l.ItemId 
                                        ,l.ChapterID 
                                ORDER BY l.Date 
                                        ,c.UType 
                                        ,l.ItemId
                                        ,l.ChapterID 
                            """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.UType,p.ItemId,p.ChapterID,p.TotalViews,p.TotalCustomers,round(p.TotalDuration),2))
    
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_CountByMovieIDChapter
                                (Date,UType,MovieID,ChapterID,TotalView,TotalCustomer,TotalDuration) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()

        print "INSERT SUCCESSFULLY"

        #cur.execute("exec [dbo].[Recommend_TopVOD_TVOD_Daily_Job] '%s'"%_date)
        #conn.commit()

        print "RECOMMEND TOP CONTENT INSERTED SUCCESSFULLY"

    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

def vod_count_by_movieid(logs_table,_date):
    
    print "Start VOD COUNT BY MOVIEID"

    logs_table.registerTempTable("logs")

    # Join table, calculte TotalView by ItemId+Location

    result = sqlContext.sql("""SELECT l.Date 
                                    ,c.UType 
                                    ,l.ItemId 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,fields.ItemId
                                        FROM logs 
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID 
                                GROUP BY l.Date
                                        ,c.UType 
                                        ,l.ItemId
                                """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.UType,p.ItemId,p.TotalCustomers))
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_CountByMovieID (Date,UType,MovieID,TotalCustomer)
                            VALUES (%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"

        #cur.execute("exec [dbo].[Recommend_TopVOD_TVOD_Customer_Daily_Job] '%s'"%_date)
        #conn.commit()

        print "RECOMMEND TOP CONTENT BY UNIT CUSTOMER INSERTED SUCCESSFULLY"

    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

# ================= LOCATION ====================

def vod_count_cus_by_location(logs_table):

    print "Start VOD COUNT CUS BY LOCATION."
    
    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date 
                                    ,c.Area 
                                    ,c.UType 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                        FROM logs 
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date 
                                        ,c.Area 
                                        ,c.UType 
                                """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.Area,p.UType,p.TotalCustomers))
    
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_Count_Cus_By_Location (Date,Location,UType,TotalCustomer)
                             VALUES (%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

# ================  MAIN - MENUID ===============

def vod_count_by_MainMenu(logs_table):

    print "Start VOD COUNT BY FOLDER LEVEL"

    # # Log from Elasticsearch
    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date 
                                    ,c.Area 
                                    ,l.Folder
                                    ,COUNT(1) AS TotalViews 
                                    ,COUNT(DISTINCT l.CustomerID) AS TotalCustomers 
                                    ,COUNT(DISTINCT l.Session) AS TotalSession
                                    ,SUM(l.RealTimePlaying) AS TotalDuration 
                                FROM (SELECT CAST(Date as date)Date
                                            ,CASE WHEN UPPER(fields.AppName) = 'VOD' THEN 2
                                                  WHEN UPPER(fields.AppName) = 'RELAX' THEN 3
                                                  WHEN UPPER(fields.AppName) = 'CHILD' THEN 4
                                                END Folder
                                            ,fields.CustomerID
                                            ,fields.Session
                                            ,CASE WHEN fields.LogId = 52 THEN 
                                                 CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                           ELSE fields.RealTimePlaying END 
                                                 ELSE 0 END RealTimePlaying
                                        FROM logs 
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date 
                                        ,c.Area 
                                        ,l.Folder
                                ORDER BY l.Date 
                                        ,c.Area 
                                        ,l.Folder
                            """)
    # print result.collect()
    rs = result.rdd.map(lambda p:(p.Date,p.Area,p.Folder,p.TotalViews,p.TotalCustomers,round(p.TotalDuration),p.TotalSession,2))
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO an_CustomerTake_MainMenu
                            (DateStamp,Area,Folder,Counter,NumberCustomer,Duration,Session)
                             VALUES (%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

def vod_count_by_menuid(logs_table):
    
    print "Start VOD COUNT BY MENU ID"

    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date 
                                     ,f.MenuID
                                     ,COUNT(DISTINCT l.CustomerID) AS TotalCustomer
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,v.Folder
                                        FROM logs r LEFT JOIN _listfolder v ON fields.ItemId = v.MovieID
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,fol f
                                WHERE l.Folder = f.FolderID
                                GROUP BY l.Date 
                                        ,f.MenuID
                                ORDER BY l.Date 
                                        ,f.MenuID
                            """)
    # print result.collect()
    rs = result.rdd.map(lambda p: (p.Date,p.MenuID,p.TotalCustomer))
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_Channel_By_GroupMenu (Date,MenuID,TotalCustomer) 
                            VALUES (%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT Log_VOD_Channel_By_GroupMenu SUCCESS"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT Log_VOD_Channel_By_GroupMenu FAIL"
    conn.close()

# ============= COUNT CUSTOMER PER APP ============

def vod_count_by_Customers_MainApps(logs_table):

    print "Start VOD COUNT FOR MAIN APPs"

    # # Log from Elasticsearch
    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT Date
                                     ,Contract
                                     ,CustomerID
                                     ,AppId
                                     ,COUNT(1) AS counting
                                FROM ( SELECT CAST(Date AS date)Date
                                             ,UPPER(fields.Contract)Contract
                                             ,fields.CustomerID
                                             ,UPPER(fields.AppName) AS AppId
                                        FROM logs
                                        WHERE fields.LogId = 52
                                        )l
                                GROUP BY Date
                                        ,Contract
                                        ,CustomerID
                                        ,AppId
                                ORDER BY Date
                                        ,Contract
                                        ,CustomerID
                                        ,AppId
                            """)
    # print result.collect()
    pivoted = result.groupBy('Date','Contract','CustomerID').pivot('AppId', ['VOD','CHILD','RELAX']).sum('counting').na.fill(0)

    rs = pivoted.rdd.map(lambda p:"""'<T Date="%s" Contract="%s" CustomerID="%s" VOD="%s" CHILD="%s" RELAX="%s"/>'"""
                            %(p[0],p[1],p[2],p[3],p[4],p[5]))
    xmlstr = ""
    xmlstr = xmlstr + ''.join(rs.collect()).replace('\n',' ').replace('  ','')
    #print xmlstr
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        if(len(xmlstr)>0):
            cur.execute("exec [dbo].[Log_Update_TopCustomerAccess_MainApps] %s"%xmlstr)
            conn.commit()
            print "INSERT & UPDATE SUCCESSFULLY!"
        else:
            print "EMPTY LOGs"
    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT & UPDATE FAILED!"
    conn.close()

# ================= SESSION VOD =================

def vod_count_total_session_by_location(logs_table):
    
    print "Start VOD COUNT CUS BY LOCATION."
    
    logs_table.registerTempTable("logs")

    result = sqlContext.sql("""SELECT l.Date
                                     ,c.Area
                                     ,c.UType
                                     ,l.Folder 
                                     ,COUNT(DISTINCT l.Session) AS TotalSession
                                FROM (SELECT CAST(Date as date)Date
                                            ,fields.CustomerID
                                            ,v.Folder
                                            ,fields.Session
                                        FROM logs r LEFT JOIN _listfolder v ON fields.ItemId = v.MovieID
                                        WHERE fields.LogId = 52
                                        ) l
                                    ,cus c 
                                WHERE c.ID = l.CustomerID 
                                GROUP BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.Folder 
                                ORDER BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.Folder 
                                """)
    # print result.collect()

    rs = result.rdd.map(lambda p: (p.Date,p.Area,p.UType,p.Folder,p.TotalSession))

    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        if(rs.count()>0):
            cur.executemany("""INSERT INTO Log_Session_VOD_ByFolderArea (Date,Area,UType,Folder,TotalSession) VALUES (%s,%s,%s,%s,%s)""", rs.collect())
            conn.commit()
            print "vod_count_total_session_by_location succesful!"
        else:
            print "EMPTY LOGs..."
    except Exception, e:
         conn.rollback()
         print str(e)
         print "vod_count_total_session_by_location fail!"
    conn.close()

def vod_unique_pageview(log_table):
    
    print "Start VOD page view and unique page view"

    log_table.where("fields.LogId in (52,512) and fields.SubMenuId IS NOT NULL and fields.SubMenuId not in ('undefined','null','')").registerTempTable("log")

    result = sqlContext.sql(""" SELECT Date
                                      ,Area
                                      ,UType
                                      ,SubMenuId
                                      ,ItemId
                                      ,SUM(PV)PV
                                      ,SUM(TotalViews)TotalViews
                                      ,COUNT(DISTINCT CustomerID)TotalCustomers
                                      ,SUM(Duration)TotalDuration
                                      ,COUNT(DISTINCT Session)UniquePV
                                FROM (
                                        SELECT CAST(l.Date AS date) Date
                                              ,c.Area
                                              ,c.UType
                                              ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END SubMenuId
                                              ,fields.ItemId
                                              ,CASE WHEN fields.LogId = 512 THEN 1 ELSE 0 END PV
                                              ,CASE WHEN fields.LogId = 52 THEN 1 ELSE 0 END TotalViews
                                              ,CASE WHEN fields.LogId = 52 THEN fields.CustomerID ELSE NULL END CustomerID
                                              ,CASE WHEN fields.LogId = 52 THEN 
                                                 CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                           ELSE fields.RealTimePlaying END 
                                                 ELSE 0 END Duration
                                              ,CASE WHEN fields.LogId = 52 THEN fields.Session ELSE NULL END Session
                                        FROM log l
                                            ,cus c
                                        WHERE fields.CustomerID = c.ID
                                        )A
                                GROUP BY Date
                                        ,Area
                                        ,UType
                                        ,ItemId
                                        ,SubMenuId
                                ORDER BY Date
                                        ,Area
                                        ,UType
                                        ,ItemId
                                        ,SubMenuId
                            """)
    # print result.collect()
    
    res = result.rdd.map(lambda p: (p.Date 
                              ,p.Area 
                              ,p.UType 
                              ,p.SubMenuId
                              ,p.ItemId 
                              ,p.TotalViews 
                              ,p.TotalCustomers 
                              ,p.TotalDuration 
                              ,p.UniquePV 
                              ,p.PV))

    rs2 = result.rdd.filter(lambda p: p.SubMenuId in ('48','49','50'))\
                    .map(lambda p: (p.Date
                              ,p.Area
                              ,p.UType
                              ,p.SubMenuId
                              ,p.ItemId
                              ,p.TotalViews
                              ,p.TotalCustomers
                              ,p.TotalDuration
                              ,p.UniquePV
                              ,p.PV))

    conn = Conf.connect110()
    cur = conn.cursor()

    try:
        if(res.count()>0):
            cur.executemany("""INSERT INTO Log_Session_VOD_UniquePV
                                            (Date,Area,UType,SubMenuId,MovieID,TotalView,TotalCustomer,TotalDuration,UniquePV,PV) 
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", res.collect())
            conn.commit()
            print "vod_unique_pageview successfully!"
        else:
            print "EMPTY LOGs..."
    except Exception, e:
        conn.rollback()
        print str(e)
        print "vod_unique_pageview fail!"

    try:
        if(rs2.count()>0):
            cur.executemany("""INSERT INTO Log_Session_VOD_UniquePV_LEAVINGSOON
                                            (Date,Area,UType,SubMenuId,MovieID,TotalView,TotalCustomer,TotalDuration,UniquePV,PV)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rs2.collect())
            conn.commit()
            print "vod_unique_LEAVINGSOON_pageview successfully!"
        else:
            print "EMPTY LOGs..."
    except Exception, e:
        conn.rollback()
        print str(e)
        print "vod_unique_LEAVINGSOON_pageview fail!"

    conn.close()

def VOD_Series_Movies_Counter(log_table):

    print "VOD COUNTER..."

    log_table.registerTempTable('logs')

    result = sqlContext.sql(""" SELECT Date
                                      ,Contract
                                      ,CustomerID
                                      ,SUM(ViewSeries)ViewSeries
                                      ,SUM(ViewMovies)ViewMovies
                                FROM (SELECT CAST(Date AS date)Date
                                                ,UPPER(fields.Contract)Contract
                                                ,fields.CustomerID
                                                ,CASE WHEN (fields.ListOnFolder LIKE '%19%' OR fields.ListOnFolder LIKE '%91%') THEN 1 ELSE 0 END ViewSeries
                                                ,CASE WHEN (fields.ListOnFolder NOT LIKE '%19%' AND fields.ListOnFolder NOT LIKE '%91%') THEN 1 ELSE 0 END ViewMovies
                                        FROM logs
                                        )A
                                GROUP BY Date
                                        ,Contract
                                        ,CustomerID
                                ORDER BY Date
                                        ,Contract
                                        ,CustomerID
                            """)
    # print result.count()
    rs = result.rdd.map(lambda p:(p.Date,p.Contract,p.CustomerID,p.ViewSeries,p.ViewMovies))

    conn = Conf.connect110()
    cur = conn.cursor()

    try:
        cur.executemany("""INSERT INTO Log_TopCustomerView_Movies_Daily
                            (Date,Contract,CustomerID,ViewSeries,ViewMovies)
                             VALUES (%s,%s,%s,%s,%s)""", result.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        print str(e)
        print "INSERT FAILED"
        conn.rollback()
        
    conn.close()

def RecSys_IMP_Counter(logs_table):

    print "IMPRESSION COUNTER..."

    logs = logs_table.where("fields.LogId = 512 or (fields.LogId = 57 and (fields.ItemId <> 'undefined' or fields.ItemId is not null)) or fields.LogId = 20").registerTempTable("logs")

    res = sqlContext.sql(""" SELECT CAST(Date AS date) Date
                                    ,CASE WHEN fields.AppId = '21' THEN 'VOD'
                                          WHEN fields.AppId = '23' THEN 'RELAX'
                                          WHEN fields.AppId = '24' THEN 'CHILD'
                                          ELSE fields.AppName
                                          END AppId
                                    ,fields.CustomerID
                                    ,CASE WHEN fields.SubMenuId = 0 AND fields.LogId <> '20' THEN 1000
                                          WHEN fields.LogId = '20' THEN 1600 
                                          WHEN fields.LogId = '57' THEN fields.ItemId 
                                          ELSE fields.SubMenuId 
                                        END SubMenuId
                                    ,CASE WHEN (fields.SubMenuId = 1600 AND fields.Screen like '%Home%') OR fields.LogId = '20' THEN 1 ELSE 0 END isHome
                                    ,CASE WHEN fields.LogId = 20 OR fields.LogId = 57 OR (fields.LogId = '512' AND NOT fields.Screen like '%Home%') THEN 1 ELSE 0 END IMP
                                    ,CASE WHEN fields.LogId = '512' THEN 1 ELSE 0 END CLK
                                    ,CASE WHEN fields.LogId = '512' AND (fields.Position IS NOT NULL AND fields.Position <> 'undefined')
                                        THEN fields.Position ELSE 0 END Position
                                FROM logs
                            """)
    
    f = udf(GetPosition, IntegerType())
    res = res.withColumn('_pos',f(res.Position)).registerTempTable('res')

    result = sqlContext.sql(""" SELECT l.Date 
                                       ,c.Area 
                                       ,c.UType 
                                       ,l.AppId 
                                       ,l.SubMenuId
                                       ,l.isHome
                                       ,COUNT(1) AS IMP
                                       ,SUM(l.CLK) AS CLK
                                       ,SUM(l._pos) AS POS
                                FROM res l
                                    ,cus c
                                WHERE l.CustomerID = c.ID
                                    AND l.SubMenuId <> 'undefined'
                                GROUP BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.AppId
                                        ,l.SubMenuId
                                        ,l.isHome
                                ORDER BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.AppId
                                        ,l.SubMenuId
                                        ,l.isHome
                            """)
    # print result.collect()

    rs = result.rdd.map(lambda p: (p.Date,p.Area,p.UType,p.AppId,p.SubMenuId,p.isHome,p.IMP,p.CLK,p.POS))

    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO RecSys_Impact_Interaction
                            (DateStamp,Area,UType,App,Folder,isHome,IMP,CLK,POS)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        print str(e)
        print "INSERT FAILED"
        conn.rollback()
    conn.close()

def RecSys_Duration_Counter(logs_table):

    print "RECSYS DURATION COUNTER..."

    logs_table.where("fields.LogId in ('51','52')").registerTempTable("logs")

    rdd = sqlContext.sql( """ SELECT CAST(Date AS date) Date
                                    ,fields.AppName AS AppId
                                    ,fields.CustomerID
                                    ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END SubMenuId
                                    ,CASE WHEN fields.SubMenuId = 1600 AND (fields.Screen like '%Home%' OR fields.MenuSession IS NULL) THEN 1 ELSE 0 END isHome
                                    ,fields.ItemId AS MovieID
                                    ,fields.ChapterID
                                    ,fields.LogId
                                    ,CASE WHEN fields.LogId = '52' THEN 
                                          CASE WHEN CAST(fields.ElapsedTimePlaying AS float)>0 THEN fields.ElapsedTimePlaying ELSE 
                                               CASE WHEN CAST(fields.RealTimePlaying AS float) >= CAST(fields.Duration AS float) THEN fields.Duration 
                                                    ELSE fields.RealTimePlaying END END
                                          ELSE 0 END ElapsedTimePlaying 
                                    ,CASE WHEN fields.LogId = '52' THEN fields.RealTimePlaying ELSE 0 END RealTimePlaying
                                    ,CASE WHEN fields.LogId = '52' THEN fields.Duration ELSE 0 END Duration
                                FROM logs
                            """)
    f = udf(GetAVT, FloatType())
    e = udf(level, IntegerType())
    rdd = rdd.withColumn('PlayingTime', f(rdd.RealTimePlaying,rdd.ElapsedTimePlaying,rdd.Duration))\
             .withColumn('Level', e(rdd.RealTimePlaying,rdd.ElapsedTimePlaying,rdd.Duration))\
             .registerTempTable("rdd")
    
    res = sqlContext.sql(""" SELECT Date
                                    ,Area
                                    ,UType
                                    ,AppId
                                    ,SubMenuId AS Folder
                                    ,isHome
                                    ,MovieID
                                    ,ChapterID
                                    ,CASE WHEN SUM(MPLY) > SUM(Q1+MPT+Q3+CMPL) THEN SUM(Q1+MPT+Q3+CMPL)
                                        ELSE SUM(MPLY)
                                        END MPLY
                                    ,SUM(Q1+MPT+Q3+CMPL) AS TotalPlay
                                    ,SUM(PlayingTime)PlayingTime
                                    ,AVG(APR) AS APR
                                    ,SUM(Q1) AS Q1
                                    ,SUM(MPT) AS MPT
                                    ,SUM(Q3) AS Q3
                                    ,SUM(CMPL) AS CMPL
                            FROM (SELECT r.Date
                                        ,c.Area
                                        ,c.UType
                                        ,r.AppId
                                        ,r.SubMenuId
                                        ,r.isHome
                                        ,r.MovieID
                                        ,r.ChapterID
                                        ,r.PlayingTime
                                        ,CASE WHEN r.LogId = 51 THEN 1 ELSE 0 END MPLY
                                        ,CASE WHEN r.LogId = 52 THEN (r.PlayingTime / CAST(r.Duration AS float) * 100) ELSE 0 END APR
                                        ,CASE WHEN r.LogId = 52 AND Level<=1 THEN 1 ELSE 0 END Q1
                                        ,CASE WHEN r.LogId = 52 AND Level=2 THEN 1 ELSE 0 END MPT
                                        ,CASE WHEN r.LogId = 52 AND Level=3 THEN 1 ELSE 0 END Q3
                                        ,CASE WHEN r.LogId = 52 AND Level=4 THEN 1 ELSE 0 END CMPL
                                    FROM rdd r
                                        ,cus c 
                                    WHERE r.CustomerID = c.ID
                                    )A
                            GROUP BY Date
                                    ,Area
                                    ,UType
                                    ,AppId
                                    ,SubMenuId
                                    ,isHome
                                    ,MovieID
                                    ,ChapterID
                            ORDER BY (SUM(Q1+MPT+Q3+CMPL)) DESC
                        """).registerTempTable('res')
    
    result = sqlContext.sql(""" SELECT Date
                                      ,Area
                                      ,UType
                                      ,AppId
                                      ,Folder
                                      ,isHome
                                      ,SUM(MPLY) AS MPLY
                                      ,SUM(TotalPlay) - SUM(MPLY) AS APLY
                                      ,ROUND(CAST(SUM(PlayingTime) as float)/CAST(SUM(TotalPlay) as float),0) AS AVT
                                      ,SUM(PlayingTime) AS PlayingTime
                                      ,ROUND(AVG(APR),2) AS APR
                                      ,SUM(Q1) AS Q1
                                      ,SUM(MPT) AS MPT
                                      ,SUM(Q3) AS Q3
                                      ,SUM(CMPL) AS CMPL
                                 FROM res
                                 GROUP BY Date
                                         ,Area
                                         ,UType
                                         ,AppId
                                         ,Folder
                                         ,isHome
                                 HAVING AVT IS NOT NULL
                                 ORDER BY Date
                                         ,Area
                                         ,UType
                                         ,AppId
                                         ,Folder
                                         ,isHome
                            """)
    #print result.collect()

    rs = result.rdd.map(lambda p: (p.Date,p.Area,p.UType,p.AppId,p.Folder,p.isHome,p.MPLY,p.APLY,p.AVT,p.APR,p.Q1,p.MPT,p.Q3,p.CMPL))
    try:
        conn = Conf.connect110()
        cur = conn.cursor()
        cur.executemany("""INSERT INTO RecSys_Conversion
                            (DateStamp,Area,UType,App,Folder,isHome,MPLY,APLY,AVT,APR,Q1,MPT,Q3,CMPL)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
        conn.close()
    except Exception, e:
        print str(e)
        print "INSERT FAILED"
        conn.rollback()
        conn.close()

def VOD_SubMenu_Behaviours(logs_table):

    print "VOD SUBMENU BEHAVIOURs is COUNTING..."

    logs = logs_table.where("(fields.LogId = 512 or fields.LogId = 57) and fields.SubMenuId <> '' and fields.SubMenuId not in ('undefined','null') and fields.SubMenuId is not null").registerTempTable("logs")

    result = sqlContext.sql(""" SELECT l.Date 
                                       ,c.Area 
                                       ,c.UType 
                                       ,l.AppId 
                                       ,l.SubMenuId
                                       ,COUNT(l.SessionSubMenu)Access
                                       ,COUNT(h.SessionSubMenu)Hit
                                       ,COUNT(DISTINCT l.Session)Session
                                FROM (SELECT CAST(Date AS date) Date
                                            ,fields.AppName AS AppId
                                            ,fields.CustomerID
                                            ,fields.Session
                                            ,CASE WHEN fields.MenuSession IS NOT NULL THEN fields.MenuSession ELSE NULL END SessionSubMenu
                                            ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END SubMenuId
                                            ,COUNT(1) Access
                                        FROM logs
                                        WHERE fields.LogId = 57
                                        GROUP BY CAST(Date AS date)
                                                ,fields.AppName
                                                ,fields.CustomerID
                                                ,fields.Session
                                                ,CASE WHEN fields.MenuSession IS NOT NULL THEN fields.MenuSession ELSE NULL END
                                                --,fields.SessionSubMenu
                                                ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END
                                        )l
                                    LEFT JOIN (SELECT CAST(Date AS date) Date
                                            ,fields.AppName AS AppId
                                            ,fields.CustomerID
                                            ,fields.Session
                                            ,CASE WHEN fields.MenuSession IS NOT NULL THEN fields.MenuSession ELSE NULL END SessionSubMenu
                                            --,fields.SessionSubMenu
                                            ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END SubMenuId
                                            ,COUNT(1) Hit
                                        FROM logs
                                        WHERE fields.LogId = 512
                                        GROUP BY CAST(Date AS date)
                                                ,fields.AppName
                                                ,fields.CustomerID
                                                ,fields.Session
                                                ,CASE WHEN fields.MenuSession IS NOT NULL THEN fields.MenuSession ELSE NULL END
                                                --,fields.SessionSubMenu
                                                ,CASE WHEN fields.SubMenuId = 0 THEN 1000 ELSE fields.SubMenuId END
                                        )h
                                        ON l.Date = h.Date
                                        AND l.AppId = h.AppId
                                        AND l.CustomerID = h.CustomerID
                                        AND l.Session = h.Session
                                        AND l.SessionSubMenu = h.SessionSubMenu
                                        AND l.SubMenuId = h.SubMenuId
                                    ,cus c
                                WHERE l.CustomerID = c.ID
                                GROUP BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.AppId
                                        ,l.SubMenuId
                                ORDER BY l.Date
                                        ,c.Area
                                        ,c.UType
                                        ,l.AppId
                                        ,l.SubMenuId
                            """)
    #print result.collect()

    rs = result.rdd.map(lambda p: (p.Date,p.Area,p.UType,p.AppId,p.SubMenuId,p.Access,p.Access-p.Hit,p.Session))

    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.executemany("""INSERT INTO Log_VOD_SubMenu_Behaviours
                            (DateStamp,Area,UType,AppId,SubMenuId,Access,Miss,Session)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", rs.collect())
        conn.commit()
        print "INSERT SUCCESSFULLY"
    except Exception, e:
        print str(e)
        print "INSERT FAILED"
        conn.rollback()
    conn.close()

def RUN_TopView(_date):
    
    conn = Conf.connect110()
    cur = conn.cursor()
    try:
        cur.execute("exec Recommend_TopVOD_TVOD_Customer_Daily_Job '%s'"%_date)
        conn.commit()

        print "RECOMMEND TOP CONTENT BY UNIT CUSTOMER INSERTED SUCCESSFULLY"

        cur.execute("exec Recommend_TopVOD_TVOD_Daily_Job '%s'"%_date)
        conn.commit()

        print "RECOMMEND TOP CONTENT INSERTED SUCCESSFULLY"


    except Exception, e:
        conn.rollback()
        print str(e)
        print "INSERT FAILED"
    conn.close()

def vod_count_by_folder_TopicLand(logs_rdd):

    #rdd = logs_rdd.where("LogId IN (512,51,52) AND fields.PlayingSession IS NOT NULL")\
    rdd = logs_rdd.where("LogId IN (512,51,55,56,52) AND fields.PlayingSession IS NOT NULL")\
                  .select(date_format('Date','yyyy-MM-dd HH:mm:ss').alias('Date'),'fields.Screen','fields.SubMenuId','fields.CustomerID','fields.Contract','fields.ItemId',upper(col('fields.AppName')),'fields.PlayingSession','fields.LogId','fields.Folder','fields.RealTimePlaying').cache()

    _tablename = 'Log_VOD_CountByFolder_TopicLane'
    _colstr = 'Date,Screen,SubMenuId,CustomerID,Contract,ItemId,AppId,PlayingSession,LogId,Folder,RealTimePlaying'
    _arr = _colstr.split(',')

    _insquery = """INSERT INTO %s(%s) VALUES (%s)"""%(_tablename,_colstr,','.join('%s' for x in _arr))

    print InsertSQL(rdd,_insquery)

    rdd.unpersist()
    #listSess = rdd.where("LogId = 512")\
    #              .select(date_format('Date','yyyy-MM-dd').alias('Date'),'fields.Screen','fields.SubMenuId','fields.CustomerID','fields.Contract','fields.ItemId','fields.AppId','fields.PlayingSession','fields.LogId','fields.Folder').registerTempTable('session')

    #listView = rdd.where("LogId = 52")\
    #              .select(date_format('Date','yyyy-MM-dd').alias('Date'),'fields.CustomerID','fields.Contract','fields.ItemId','fields.AppId','fields.PlayingSession','fields.ChapterID','fields.LogId','fields.RealTimePlaying').registerTempTable('logs')

    ##res1 = listView.groupBy('Date','PlayingSession').agg({'Date':'count'}).alias('TotalView')

    ##print res1.count()
    ##print res1.take(10)

    #res = sqlContext.sql(""" SELECT CASE WHEN l.Date IS NOT NULL THEN l.Date ELSE s.Date END Date
    #                                ,s.Screen,s.SubMenuId,s.Folder,SUM(l.TotalView),SUM(l.TotalCustomer),SUM(l.TotalDuration),COUNT(DISTINCT s.PlayingSession)TotalSess
    #                        FROM (SELECT DISTINCT Date,Screen,SubMenuId,Folder,PlayingSession FROM session) s
    #                             FULL OUTER JOIN (SELECT Date,PlayingSession,COUNT(1)TotalView,COUNT(DISTINCT CustomerID)TotalCustomer,SUM(RealTimePlaying)TotalDuration FROM logs
    #                                        GROUP BY Date,PlayingSession) l 
    #                                             ON s.Date = l.Date
    #                                             AND s.PlayingSession = l.PlayingSession
    #                        GROUP BY CASE WHEN l.Date IS NOT NULL THEN l.Date ELSE s.Date END,s.Screen,s.SubMenuId,s.Folder
    #                    """)

def InsertSQL(data,query):

    conn = Conf.connect110()
    cur = conn.cursor()

    i = 0
    step = 100000
    n = data.count()

    while (i <= n):

        print i

        try:
            cur.executemany(query,data.collect()[i:i+step])
            conn.commit()
            print "SQL SUCCESS!"
        except Exception, e:
            print "SQl FAILED!"
            print str(e)
            conn.rollback()

        i = i + step

    conn.close()

    return n

def Running(_NO,logs_rdd,logstash_date):

    # Folder
    if(_NO == 1):
        try:
            vod_count_by_folder_level(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 2):
        try:
            vod_count_by_folder(logs_rdd)
        except Exception, e:
            print str(e)
    
    # MovieID
    elif(_NO == 3):
        try:
            vod_count_by_movieid_chapter(logs_rdd,logstash_date)
        except Exception, e:
            print str(e)

    elif(_NO == 4):
        try:
            vod_count_by_movieid(logs_rdd,logstash_date)
        except Exception, e:
            print str(e)

    # Location
    elif(_NO == 5):
        try:
            vod_count_cus_by_location(logs_rdd)
        except Exception, e:
            print str(e)

    # Main - MenuID
    elif(_NO == 6):
        try:
            vod_count_by_menuid(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 7):
        try:
            vod_count_by_MainMenu(logs_rdd)
        except Exception, e:
            print str(e)

    # Count Customer per Apps
    elif(_NO == 8):
        try:
            vod_count_by_Customers_MainApps(logs_rdd)
        except Exception, e:
            print str(e)

    # Session VOD
    elif(_NO == 9):
        try:
            vod_count_total_session_by_location(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 10):
        try:
            vod_unique_pageview(logs_rdd)
        except Exception, e:
            print str(e)
        
    # View Series/Movies
    elif(_NO == 11):
        try:
            VOD_Series_Movies_Counter(logs_rdd)
        except Exception, e:
            print str(e)

    # RecSys
    elif(_NO == 12):
        try:
            RecSys_IMP_Counter(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 13):
        try:
            RecSys_Duration_Counter(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 14):
        try:
            VOD_SubMenu_Behaviours(logs_rdd)
        except Exception, e:
            print str(e)

    elif(_NO == 15):
        try:
            RUN_TopView(logstash_date)
        except Exception, e:
            print str(e)

    elif(_NO == 16):
        try:
            vod_count_by_folder_TopicLand(logs_rdd)
        except Exception, e:
            print str(e)


    else:
        print "Out of Functions"
        logs_rdd.unpersist()
        return -1

    return _NO

def Statement(_NO,sc,_date,_source):

    ## Files
    #data = Conf.GetDataFromFiles(sc,'/opt/bigdata/thuydb15/fbox-%s.txt'%_date.replace('.','-'))
    #logs_rdd = Process_log_Kafka(data).cache()

    if(_source == 'k'): #priority Kafka

        try:
            data = Conf.GetDataFromHDFS_Kafka(sc,_date)
            logs_rdd = Process_log_Kafka(data).cache()
        except Exception,e :
            print unicode(e)
            data = Conf.GetDataFromHDFS(sc,_date)
            logs_rdd = Process_log(data).cache()

    elif(_source == 'h'): #priority HDFS
        try:
            data = Conf.GetDataFromHDFS(sc,_date)
            logs_rdd = Process_log(data).cache()
        except Exception, e:
            print unicode(e)
            data = Conf.GetDataFromHDFS_Kafka(sc,_date)
            logs_rdd = Process_log_Kafka(data).cache()

    elif(_source == 'f'):
        data = Conf.GetDataFromFiles(sc,_date).cache()
        logs_rdd = Process_log_Kafka(data).cache()

    if not(_NO == 0 or _NO[0] == '0'):
        for _f in _NO:
            Running(int(_f),logs_rdd,_date)
    else:
        print 'RUN ALL JOBs'
        i = 1
        while not(i == 0):
            _func = Running(i,logs_rdd,_date)
            i = _func + 1
            time.sleep(30)

    logs_rdd.unpersist()
 
def main(_NO,_from,_to,_source):
    
    # menu = menu_rdd(sc).registerTempTable("menu")
    fol = folder_rdd(sc).registerTempTable("fol")
    _listfolder = GetListFolder(sc).registerTempTable("_listfolder")

    if(_to == 0):
        _date = _from
        logstash_date = Conf.GetLogStash(_date)
        print logstash_date

        cus_rdd = Conf.customers_rdd(sc,logstash_date).cache().registerTempTable('cus')
        Statement(_NO,sc,logstash_date,_source)
        #Statement(_NO,sc,_date,_source) # test 1h
        #time.sleep(30)
    else:
        _fromDate = datetime.strptime(_from,'%Y.%m.%d %H')
        _toDate = datetime.strptime(_to,'%Y.%m.%d %H')
        
        print 'Functions will running from %s to %s'%(_fromDate,_toDate)

        i = _fromDate
        while (i <= _toDate): 
            date = str(datetime.strftime(i,'%Y.%m.%d %H'))
            i = i + timedelta(days = 1)
            logstash_date = Conf.GetLogStash(date)
            print logstash_date

            cus_rdd = Conf.customers_rdd(sc,logstash_date).cache().registerTempTable('cus')
            Statement(_NO,sc,logstash_date,_source)
            # Statement(_NO,sc,_date,_source) # test 1h
            time.sleep(30)
    
if __name__ == '__main__':

    try:
        #_NO = int(_params[0])
        _NO = _params[0].split(',')[1:]
        print _NO
    except Exception, e:
        _NO = 0
    
    try:
        _from = Conf.GetDateforStash(_params[1])
    except Exception, e:
        _from = Conf.GetDateforStash('now')
    
    try:
        _to = Conf.GetDateforStash(_params[2])
    except Exception, e:
        _to = 0

    try:
        _source = _params[0].split(',')[0].lower()
    except Exception, e:
        _source = 'k'

    print _source

    main(_NO,_from,_to,_source)
