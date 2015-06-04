'''
Created on Oct 25, 2013

@author: admshuyang
'''


import MySQLdb;

class Connect2MySQL:
    
    #private attribute
    __Connection__ = None;
    
    def __init__(self):
        pass;
    
    @staticmethod
    def GetInisitance():
        if(Connect2MySQL.__Connection__ is None):
            Connect2MySQL.__Connection__ = MySQLdb.connect(host="128.196.93.142", user="shu", passwd="Sql.slu2012", db="mac_trip");
            
        return Connect2MySQL.__Connection__;



class MacData2MySQL:
    def __init__(self):
        pass;
    
    def InsertTrip(self, mac, previousLocation, previousTime, nextLocation, nextTime):
        Connection = Connect2MySQL.GetInisitance();
        MysqlCursor = Connection.cursor();
        MysqlCursor.callproc( "Insert_MacTrip", (mac, previousLocation, previousTime, nextLocation, nextTime));
        #Connection.commit();
        MysqlCursor.close();
        
    def InsertIndividualMAC(self, year, month, mac, location, detectedTime, macName = None):
        Connection = Connect2MySQL.GetInisitance();
        MysqlCursor = Connection.cursor();
        
        try:
            if(macName is not None):
                MysqlCursor.callproc( "Insert_IndividualMAC_withName", (mac, location, detectedTime, macName));
            else:
                MysqlCursor.callproc( "Insert_IndividualMAC", (year, month, mac, location, detectedTime));
        except:
            print "Error!"
            pass;
                    
        MysqlCursor.close();
        
    def CreateNewTable(self, year, month):
        Connection = Connect2MySQL.GetInisitance();
        MysqlCursor = Connection.cursor();
        MysqlCursor.callproc( "CreateNewTable", (year, month));
        # MysqlCursor.callproc("CreateIndex", (year,month))
        MysqlCursor.close();
        
#===============================================================================
# class ExtractMacData:
#     def __init__(self):
#         pass;
#     
#     def 
#===============================================================================
    
        
