import MySQLdb;
import datetime

class MySQLConn:
    
    __Connection__= None;
    
    def __init__(self):
        pass;
    
    #@staticmethod
    def NewConn(self,dbName):
        if(self.__Connection__ is None):
            self.__Connection__=MySQLdb.connect(host="128.196.93.142",user="ming",passwd="password",db=dbName);
            #MySQLConn.__Connection__=MySQLdb.connect(host="localhost",user="root",passwd="password",db="mac_trip");
        return self.__Connection__;
    
    
class MySQLQuery:

    def __init__(self):
        pass;
    
    def InsertMacDataRow(self,Time,Location,MacAddress):
        Connection=MySQLConn.NewConn("mac_trip")
        MySqlCursor=Connection.cursor()
        
        try:
            MySqlCursor.execute("""INSERT INTO MacData VALUES (%s,%s,%s)""",(Time,Location,MacAddress))
            Connection.commit()
        except:
            Connection.rollback()
            
    def SelectMacData(self,fromTime,toTime,temptime):
        year = temptime.year
        month = temptime.month
        tblName = "%d_%02d" % (year,month)
        
        MySQLConn_MacTrip = MySQLConn()
        Connection = MySQLConn_MacTrip.NewConn("mac_trip")
        MySqlCursor=Connection.cursor()
        SqlString = """SELECT * from %s where DetectedTime >= \'%s\' and DetectedTime <= \'%s\' order by MAC, DetectedTime""" % (tblName,fromTime,toTime)        
        #MySqlCursor.execute("""SELECT * from %s where DetectedTime >= %s and DetectedTime <= %s order by MAC, DetectedTime""", (tblName,fromTime,toTime))
        MySqlCursor.execute(SqlString)
        Connection.close()
        return MySqlCursor
    
    def GetIntersectionTable(self):
        MySQLConn_bt = MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor = Connection.cursor()
        MySqlCursor.execute("SELECT * FROM INTERSECTION")
        
        dicINT = {}
        for i in range(MySqlCursor.rowcount):
            Row = MySqlCursor.fetchone() 
            INT_Name = Row[1] #Name of the Intersection
            dicINT[INT_Name] = Row[0] #id of the Intersection
            
        Connection.close()
        return dicINT
    
    def InsertBTStatusRecord(self,INT_id,ReportDate,BTStatus,TotalSamples):
        MySQLConn_bt = MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor=Connection.cursor()
        
        try:            
            MySqlCursor.execute("""INSERT INTO btdevicestatus VALUES (%s,%s,%s,%s)""",(INT_id,ReportDate,BTStatus,TotalSamples))
            Connection.commit()
        except:
            Connection.rollback()
            
        Connection.close()
     
            
    def InsertPairedTTSamples(self,Mac,FROM_INT,TO_INT,F2F_TT,F2F_Timestamp,L2L_TT,L2L_Timestamp):
        MySQLConn_bt = MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor = Connection.cursor()
        
        try:
            MySqlCursor.execute("""INSERT INTO pairedtt_raw (Mac_address,FROM_INT,TO_INT,F2F_TT,F2F_Timestamp,L2L_TT,L2L_Timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (Mac,FROM_INT,TO_INT,F2F_TT,F2F_Timestamp,L2L_TT,L2L_Timestamp))
            Connection.commit()
        except:
            Connection.rollback()
            
        Connection.close()
        
        
        
        
            
            
        
        
    
        
    
        
