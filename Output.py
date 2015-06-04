'''
Created on Mar 3, 2015

@author: mingchen7
'''

import SqlConnect
import csv

class DataOutput:
    
    def __init__(self):
        pass
    
    def WriteTrajectory2CSV(self,TrajectoryLists, Timestring):

        Year = Timestring[0:4]  #get the year
        Month = Timestring[5:7] #get the month
        Date = Timestring[8:10]  #get the date
        
        FileName = 'TripData\\TripInfo_%s_%s_%s.csv' % (Year,Month,Date)
        
        csvfile = open(FileName,'ab')
        writer = csv.writer(csvfile)
        
        for i in range(len(TrajectoryLists)):
            for j in range(len(TrajectoryLists[i])):
                writer.writerow(TrajectoryLists[i][j])
            
            writer.writerow("\n")
        print "Trajectory data written in the %s." % (FileName) 
                  
    
    # output trip TT to csv     
    def WriteTripTT2CSV(self, F2FTTContainer, L2LTTContainer, F2FMacContainer, L2LMacContainer, F2FTSContainer, L2LTSContainer, crtTime):
        Year = crtTime[0:4]  #get the year
        Month = crtTime[5:7] #get the month
        Date = crtTime[8:10]  #get the date
        Hour = crtTime[11:13] #get the hour
        Minute = crtTime[14:16]
        Second = crtTime[17:19]
        printTable = [] 
                
        FileName = 'TravelTime\\TTSamples_%s_%s_%s.csv' % (Year,Month,Date)
            
        csvfile = open(FileName,'ab')
        writer = csv.writer(csvfile, delimiter=',')
        
        if((Hour == '06') and (Minute == '00') and (Second == '00')):
            writer.writerow(['From','To','F2FArrvlTime','F2FTT','F2FMac','L2LArrvlTime','L2LTT','L2LMac'])
            
        for FromLocation in F2FTTContainer:
            for ToLocation in F2FTTContainer[FromLocation]:
                for i in range(len(F2FTTContainer[FromLocation][ToLocation])):
                    F2F = F2FTTContainer[FromLocation][ToLocation][i]  #F2F travel time
                    L2L = L2LTTContainer[FromLocation][ToLocation][i]  #L2L travel time
                    F2FMac = F2FMacContainer[FromLocation][ToLocation][i]  #F2FMac
                    L2LMac = L2LMacContainer[FromLocation][ToLocation][i]  #L2LMac
                    F2FTimestamp = F2FTSContainer[FromLocation][ToLocation][i]
                    L2LTimestamp = L2LTSContainer[FromLocation][ToLocation][i]
                    #writer.writerow([FromLocation,ToLocation,F2FTimestamp,str(F2F),str(F2FMac),L2LTimestamp,str(L2L),str(L2LMac)])
                    printTable.append([FromLocation,ToLocation,F2FTimestamp,str(F2F),str(F2FMac),L2LTimestamp,str(L2L),str(L2LMac)])
                    
        printTable.sort(key=lambda x:x[2])  #sorted by the third column
        for row in printTable:
            writer.writerow(row)
            
        print "Trip-level travel time written in the %s" % (FileName)
        
    # output trip TT to database    
    def WriteTripTT2DB(self, F2FTTContainer, L2LTTContainer, F2FMacContainer, L2LMacContainer,F2FTSContainer,L2LTSContainer):
        MySQLConn_bt = SqlConnect.MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor = Connection.cursor()
        
        Query = SqlConnect.MySQLQuery()
        dicIntID = Query.GetIntersectionTable()

        count = 0
                    
        for FromLocation in F2FTTContainer:
            for ToLocation in F2FTTContainer[FromLocation]:
                for i in range(len(F2FTTContainer[FromLocation][ToLocation])):
                    FROM_INT = dicIntID[FromLocation]
                    TO_INT = dicIntID[ToLocation]
                    
                    F2F = F2FTTContainer[FromLocation][ToLocation][i]  #F2F travel time
                    L2L = L2LTTContainer[FromLocation][ToLocation][i]  #L2L travel time
                    F2FMac = F2FMacContainer[FromLocation][ToLocation][i]  #F2FMac
                    F2FTimestamp = F2FTSContainer[FromLocation][ToLocation][i]
                    L2LTimestamp = L2LTSContainer[FromLocation][ToLocation][i]
    
                    MySqlCursor.execute("""INSERT INTO triptt (Mac_address,FROM_INT,TO_INT,F2F_TT,F2F_Timestamp,L2L_TT,L2L_Timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (F2FMac,FROM_INT,TO_INT,F2F,F2FTimestamp,L2L,L2LTimestamp))                
                    count = count + 1    
        try:
            Connection.commit()
        except:
            Connection.rollback()       
            
        print "%d Trip-level travel time records inserted in the database." % (count)
    
