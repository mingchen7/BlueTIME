'''
Created on Nov 10, 2014

@author: mingchen7
'''

import datetime
import SqlConnect
#import TravelTime
import csv
from collections import defaultdict

class LinkTravelTimeProcess:
    
    __LinkTT__ = None
    __AggIntvl__ = None;
    __LocationList__ = None;
    #__HardThresholds__ = None;  #define the hardthresholds for links
    __maxKnear__ = None;
    __AliveLocations__ = None;
    __flagSTA__ = None; #dummy for if activating algorithm
    __IntersectionID__ = None;
    
    def __init__(self,Interval,Locations):
        self.__LinkTT__ = []
        self.__AliveLocations__ = {}
        self.__AggIntvl__ = Interval #by default, 15 minutes
        self.__LocationList__ = Locations
        self.__maxKnear__ = 3
        #self.__HardThresholds__ = 30
        self.__flagSTA__ = True
         
        Query = SqlConnect.MySQLQuery()
        self.__IntersectionID__ = Query.GetIntersectionTable()
         
    def getHardThresholds(self,FromLocation, ToLocation):                        
        SpeedwayThresholds = [10, 15, 15, 14, 30, 30, 17, 42, 30, 30, 30, 30, 62, 59, 30, 30, 59, 60]
        FROM_INT = self.__IntersectionID__[FromLocation]
        TO_INT = self.__IntersectionID__[ToLocation]
        
        if(FROM_INT < TO_INT):
            tmpThresholds = SpeedwayThresholds[FROM_INT-1:TO_INT-1]
        else:
            tmpThresholds = SpeedwayThresholds[TO_INT-1:FROM_INT-1]
        
        TotalThreshold = sum(tmpThresholds)
        
        return TotalThreshold
        
    
    def ReshapeData2List(self,F2FTTContainer,L2LTTContainer,MacContainer,F2FTSContainer):
        lstPairedTT = []
        
        for FromLocation in F2FTTContainer:
            for ToLocation in F2FTTContainer[FromLocation]:
                for i in range(len(F2FTTContainer[FromLocation][ToLocation])):
                    F2F = F2FTTContainer[FromLocation][ToLocation][i]  #F2F travel time
                    L2L = L2LTTContainer[FromLocation][ToLocation][i]  #L2L travel time
                    Mac = MacContainer[FromLocation][ToLocation][i]  #F2FMac               
                    F2FTimestamp = F2FTSContainer[FromLocation][ToLocation][i]
                    
                    #Use the mean of F2F and L2L
                    TT = (F2F + L2L) / 2
                    lstPairedTT.append([str(Mac),FromLocation,ToLocation,F2FTimestamp,TT])
                    
        lstPairedTT.sort(key=lambda x:x[3])  #sorted by timestamp
        
        return lstPairedTT
        
    def Convert2TTMatrix(self,lstPairedTT,Time_S,Time_E,LocationList,row_indicator):
        TTMatrix = {}
        
        for location in LocationList:
            TTMatrix[location] = defaultdict(list)  #for each location, it's a structure of lists in dictionary to store travel time results
        
        row = row_indicator
        
        while(row < len(lstPairedTT)):
            if((lstPairedTT[row][3] >= Time_S) and (lstPairedTT[row][3] <= Time_E)):        
                FromLocation = lstPairedTT[row][1]
                ToLocation = lstPairedTT[row][2]
                TT = lstPairedTT[row][4]
                
                if(TTMatrix.has_key(FromLocation)):
                    if(TT >= self.getHardThresholds(FromLocation, ToLocation)):  #check if the TT larger than the hard threshold
                        TTMatrix[FromLocation][ToLocation].append(TT)
                
                row = row + 1
            else:
                break                    
        
        return (row, TTMatrix)
    
    
    def getAverageTT(self,TTMatrix):
        AvgTTMatrix = {}
        
        for location in self.__LocationList__:
            AvgTTMatrix[location] = {};
            
        
        for FromLocation in TTMatrix:
            for ToLocation in TTMatrix[FromLocation]:
                if(AvgTTMatrix.has_key(FromLocation)):
                    AvgTTMatrix[FromLocation][ToLocation] = sum(TTMatrix[FromLocation][ToLocation]) / len(TTMatrix[FromLocation][ToLocation])
        
        return AvgTTMatrix
        
    # The status are determined based on calculated TT Matrix    
    def getDeviceStatus(self,F2FTTContainer):
        BTStatus = {} 
        
        for FromLocation in F2FTTContainer:
            if(len(F2FTTContainer[FromLocation]) != 0):
                BTStatus[FromLocation] = 1
            else:
                BTStatus[FromLocation] = 0
                
        return BTStatus
    
    # ====================================================================
    # important to consider it non-exist if detected samples are very low
    # ====================================================================
    def RemoveCherry(self,BTStatus):
        Location = 'Speedway-Cherry'
        BTStatus[Location] = 0
        
        return BTStatus
        
    def SpatialTraversalAlgorithm(self,AvgTTMatrix,AliveLocations):
        estAvgTT = {}
        #to store the TT results computed from SAL algorithm
        for location in self.__LocationList__:
            estAvgTT[location] = defaultdict(list)
        
        maxidx = len(AliveLocations)
        
        HardThreshold = 1000 
        
        #EB
        tmpList = AliveLocations.keys()[0:maxidx-1]
        for i in tmpList:
            FromLocation = AliveLocations[i]
            if(i <= (len(AliveLocations) - 1)): #not the last location
                ToLocation = AliveLocations[i+1]
            
            HardThreshold = self.getHardThresholds(FromLocation, ToLocation)
            
            #starts the iteration of SAL algorithm
            #case 1, scenario 1 - based on two links, forward
            for n in range(self.__maxKnear__):
                First_i = i - (n + 1)
                First_j = i + 1
                Second_i = i -(n + 1) 
                Second_j = i 
                   
                if(First_i > 0):
                    firstFrom = AliveLocations[First_i]
                    firstTo = AliveLocations[First_j]
                    secondFrom = AliveLocations[Second_i]
                    secondTo = AliveLocations[Second_j]
                    #if TT existed for this pair of from & to
                    if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo)): 
                        #diffTT = AvgTTMatrix[secondFrom][secondTo] - AvgTTMatrix[firstFrom][firstTo]
                        diffTT = AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo]
                        if(diffTT >= HardThreshold):
                            estAvgTT[FromLocation][ToLocation].append(diffTT)
        
                #case 1, scenario 2 - based on two links, backward
                First_i = i
                First_j = i + 1 + (n + 1)
                Second_i = i + 1           
                Second_j = i + 1 + (n + 1)
                
                if(First_j <= maxidx):
                    firstFrom = AliveLocations[First_i]
                    firstTo = AliveLocations[First_j]
                    secondFrom = AliveLocations[Second_i]
                    secondTo = AliveLocations[Second_j]
                    #if TT existed
                    if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo)):
                        diffTT = AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo]
                        if(diffTT >= HardThreshold):
                            estAvgTT[FromLocation][ToLocation].append(diffTT)
            
            #case 2 - based on three links            
            for m in range(self.__maxKnear__):
                for n in range(self.__maxKnear__):
                    First_i = i - (m + 1)
                    First_j = (i + 1) + (n + 1)
                    Second_i = i - (m + 1)
                    Second_j = i
                    Third_i = i + 1
                    Third_j = (i + 1) + (n + 1)
                    
                    if( (i > (m + 1)) and ((i + 1 + n + 1) <= maxidx)):
                        firstFrom = AliveLocations[First_i]
                        firstTo = AliveLocations[First_j]
                        secondFrom = AliveLocations[Second_i]
                        secondTo = AliveLocations[Second_j]
                        thirdFrom = AliveLocations[Third_i]
                        thirdTo = AliveLocations[Third_j]
                        if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo) and AvgTTMatrix[thirdFrom].has_key(thirdTo)):
                            diffTT =  AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo] - AvgTTMatrix[thirdFrom][thirdTo]
                            if(diffTT >= HardThreshold):
                                estAvgTT[FromLocation][ToLocation].append(diffTT)
            
            #case 3? Link AC has data, link AB, BC negative
                      
        #WB
        tmpList = AliveLocations.keys()[1:maxidx]
        tmpList.reverse()
        
        for i in tmpList:
            FromLocation = AliveLocations[i]
            if(i > 1): #not the last location
                ToLocation = AliveLocations[i-1]
            
            HardThreshold = self.getHardThresholds(FromLocation, ToLocation)
            
            #starts the iteration of Spatial Traversal Algorithm
            #case 1, scenario 1 - based on two links, forward
            for n in range(self.__maxKnear__):
                First_i = i + (n + 1)
                First_j = i - 1
                Second_i = i + (n + 1) 
                Second_j = i 
                   
                if(First_i <= maxidx):
                    firstFrom = AliveLocations[First_i]
                    firstTo = AliveLocations[First_j]
                    secondFrom = AliveLocations[Second_i]
                    secondTo = AliveLocations[Second_j]
                    #if TT existed for this pair of from & to
                    if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo)): 
                        diffTT = AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo]
                        if(diffTT >= HardThreshold):
                            estAvgTT[FromLocation][ToLocation].append(diffTT)
        
                #case 1, scenario 2 - based on two links, backward
                First_i = i
                First_j = i - 1 - (n + 1)
                Second_i = i - 1           
                Second_j = i - 1 - (n + 1)
                
                if(First_j > 0):
                    firstFrom = AliveLocations[First_i]
                    firstTo = AliveLocations[First_j]
                    secondFrom = AliveLocations[Second_i]
                    secondTo = AliveLocations[Second_j]
                    #if TT existed
                    if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo)):
                        diffTT = AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo]
                        if(diffTT >= HardThreshold):
                            estAvgTT[FromLocation][ToLocation].append(diffTT)
            
            #case 2 - based on three links            
            for m in range(self.__maxKnear__):
                for n in range(self.__maxKnear__):
                    First_i = i + (m + 1)
                    First_j = (i -1) - (n + 1)
                    Second_i = i + (m + 1)
                    Second_j = i
                    Third_i = i - 1
                    Third_j = (i - 1) - (n + 1)
                    
                    if( (i + m + 1 <= maxidx) and ((i - 1 - (n + 1)) > 0)):
                        firstFrom = AliveLocations[First_i]
                        firstTo = AliveLocations[First_j]
                        secondFrom = AliveLocations[Second_i]
                        secondTo = AliveLocations[Second_j]
                        thirdFrom = AliveLocations[Third_i]
                        thirdTo = AliveLocations[Third_j]
                        if(AvgTTMatrix[firstFrom].has_key(firstTo) and AvgTTMatrix[secondFrom].has_key(secondTo) and AvgTTMatrix[thirdFrom].has_key(thirdTo)):
                            diffTT =  AvgTTMatrix[firstFrom][firstTo] - AvgTTMatrix[secondFrom][secondTo] - AvgTTMatrix[thirdFrom][thirdTo]
                            if(diffTT >= HardThreshold):
                                estAvgTT[FromLocation][ToLocation].append(diffTT)
        return estAvgTT
        
            
    def getEstimatedTTMatrix(self,TTMatrix,AliveLocations):                            
        AvgTTMatrix = self.getAverageTT(TTMatrix)
                        
        if(self.__flagSTA__):    
            #get estimated link TT via Spatial Traversal Algorithm                
            estAvgTTMatrix = self.SpatialTraversalAlgorithm(AvgTTMatrix, AliveLocations)
                        
            #combine the direct and indirect TT
            for FromLocation in estAvgTTMatrix:
                for ToLocation in estAvgTTMatrix[FromLocation]:
                    if(not AvgTTMatrix[FromLocation].has_key(ToLocation)):  #No value in the direct AvgTT
                        tmpAvg = sum(estAvgTTMatrix[FromLocation][ToLocation]) / len(estAvgTTMatrix[FromLocation][ToLocation])
                        AvgTTMatrix[FromLocation][ToLocation] = tmpAvg                                    
                    elif((estAvgTTMatrix[FromLocation].has_key(ToLocation)) and (AvgTTMatrix[FromLocation].has_key(ToLocation))): #both have values
                        #give some weights on the direct AvgTT and estimated TT from SAL algorithm
                        tmpAvg = 0.7 * AvgTTMatrix[FromLocation][ToLocation] + 0.3 * sum(estAvgTTMatrix[FromLocation][ToLocation]) / len(estAvgTTMatrix[FromLocation][ToLocation])
                        AvgTTMatrix[FromLocation][ToLocation] = tmpAvg
                    else: #neither one has value or no estimated value
                        continue        
                                            
        return AvgTTMatrix


    def Save2LinkTTList(self,updTTMatrix, AliveLocations, StartTime, idxTOD):
        tmplist = []
        crtDate = StartTime.date()
        #EB
        maxidx = len(AliveLocations)
        keys = AliveLocations.keys()[0:maxidx-1]
        for i in keys:
            UpStream = AliveLocations[i]
            DownStream = AliveLocations[i+1]
            if(updTTMatrix[UpStream].has_key(DownStream)):
                TT = updTTMatrix[UpStream][DownStream]
                tmpList = [UpStream, DownStream, crtDate, idxTOD, TT]
                self.__LinkTT__.append(tmpList)
            
        #WB
        keys.reverse()
        for i in keys:
            UpStream = AliveLocations[i+1]
            DownStream = AliveLocations[i]
            if(updTTMatrix[UpStream].has_key(DownStream)):
                TT = updTTMatrix[UpStream][DownStream]
                tmpList = [UpStream, DownStream, crtDate, idxTOD, TT]
                self.__LinkTT__.append(tmpList)        
    
    #function to insert into database
    def InsertEstLinkTT2DB(self):
        Query = SqlConnect.MySQLQuery()
        dicIntID = Query.GetIntersectionTable()
        
        MySQLConn_bt = SqlConnect.MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor = Connection.cursor()
        
        for i in range(len(self.__LinkTT__)):
            FromLocation = self.__LinkTT__[i][0]
            ToLocation = self.__LinkTT__[i][1]
            FROM_INT = dicIntID[FromLocation]
            TO_INT = dicIntID[ToLocation]
            HISTORY_DATE = self.__LinkTT__[i][2]
            TOD_idx = self.__LinkTT__[i][3]
            AVG_TT = self.__LinkTT__[i][4]
            
            HISTORY_DATE = datetime.datetime.combine(HISTORY_DATE, datetime.datetime.min.time())
            Timestamp = HISTORY_DATE + datetime.timedelta(minutes=((TOD_idx + 24 - 1)* 15))
            
            if(self.__flagSTA__):
                MySqlCursor.execute("""INSERT INTO linktt_15m_est VALUES (%s,%s,%s,%s,%s,%s)""", (FROM_INT,TO_INT,HISTORY_DATE,TOD_idx,AVG_TT,Timestamp))
            #else:
            #    MySqlCursor.execute("""INSERT INTO linktt_15m_beforesta VALUES (%s,%s,%s,%s,%s)""", (FROM_INT,TO_INT,HISTORY_DATE,TOD_idx,AVG_TT))                
        
        try:
            Connection.commit()
        except:
            Connection.rollback()     
                    
        
    def EstimateLinkTT(self,F2FTTContainer,L2LTTContainer,MacContainer,F2FTSContainer, StartTime, EndTime):
        PairedTT = self.ReshapeData2List(F2FTTContainer, L2LTTContainer, MacContainer, F2FTSContainer)  #get paired TT samples
        
        BTStatus = self.getDeviceStatus(F2FTTContainer)
        # BTStatus = self.RemoveCherry(BTStatus) # remove the intersection at Cherry: it's too far from center of intersection 
        
        StartTime = datetime.datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
        EndTime = datetime.datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")
        
        TimeIterator_S = StartTime
        TimeIterator_E =TimeIterator_S + datetime.timedelta(minutes = self.__AggIntvl__)
        RowMark = 0
        
        AliveLocations = {}        
        index= 1
        
        #get the alive location list for current day
        for location in self.__LocationList__:
            if(BTStatus[location] == 1):
                AliveLocations[index] = location
                index = index + 1                
        self.__AliveLocations__ = AliveLocations
                
        idxTOD = 1
        
        while(TimeIterator_E <= EndTime):
            #Calculate Link TT for each interval
            updRowMark, TTMatrix = self.Convert2TTMatrix(PairedTT,TimeIterator_S,TimeIterator_E,self.__LocationList__, RowMark)
            RowMark = updRowMark
            
            #Apply Spatial Traversal Algorithm
            updTTMatrix = self.getEstimatedTTMatrix(TTMatrix,AliveLocations)
            
            #organize into list
            self.Save2LinkTTList(updTTMatrix, AliveLocations, StartTime, idxTOD)
                                                                    
            TimeIterator_S = TimeIterator_E
            TimeIterator_E = TimeIterator_E + datetime.timedelta(minutes = self.__AggIntvl__)
            idxTOD = idxTOD + 1            
            
    def SearchHistoryLinkTT(self,StartDate,EndDate):
        MySQLConn_bt = SqlConnect.MySQLConn()
        Connection = MySQLConn_bt.NewConn("tucsonbttt")
        MySqlCursor = Connection.cursor()
        
        if(self.__flagSTA__):    
            MySqlCursor.execute("""SELECT * from LINKTT_15M_EST where HISTORY_DATE >= %s and HISTORY_DATE <= %s order by HISTORY_DATE,TOD_idx,FROM_INT,TO_INT""", (StartDate,EndDate))
        #else:
        #    MySqlCursor.execute("""SELECT * from LINKTT_15M_beforesta where HISTORY_DATE >= %s and HISTORY_DATE <= %s order by HISTORY_DATE,TOD_idx,FROM_INT,TO_INT""", (StartDate,EndDate))                
        
        Connection.close()
        return MySqlCursor
    
    
    def GetNodes(self):
        lstNodes = []
        Query = SqlConnect.MySQLQuery()
        dicIntID = Query.GetIntersectionTable()
        
        maxidx = len(self.__AliveLocations__)
        keys = self.__AliveLocations__.keys()[0:maxidx-1]
        for i in keys:
            firstNode = self.__AliveLocations__[i]
            secondNode = self.__AliveLocations__[i+1]
            lstNodes.append([dicIntID[firstNode],dicIntID[secondNode]])
            
        return lstNodes
    
                                            
    #Export pre-pared imputation dataset to CSV files
    def GenerateImpDataset(self, ImpTime):        
        
        #Get Begin and End nodes of links
        Nodes = self.GetNodes()
        TotalInterval = 14 * (60 / self.__AggIntvl__)                        
                
        EndDate = datetime.datetime.strptime(ImpTime, "%Y-%m-%d %H:%M:%S")
        EndDate = EndDate.date()
        
        #======================================================
        #Notes: StartDate should at least later than 2014-06-15
        #HISTORICAL INTERVAL SELECTION MATTERS!
        #CANNOT GUARANTEE DATA AVAILABLE FOR EACH LINK FOR EACH DAY
        #USE THE CURREN DAY ONLY TO IMPUTE
        #======================================================
                
        # StartDate = EndDate + datetime.timedelta(days = -14)  #start from two weeks ago
        StartDate = EndDate
        
        if(StartDate < datetime.date(2014, 6, 1)):
            print "Warning:Need at lease two weeks data to generate imputation dataset. Current date: %s. Please choose a date after 2014-06-15\n" % (ImpTime)
            return
        
        Cursor = self.SearchHistoryLinkTT(StartDate,EndDate)

        tmpTOD_idx = 1
        tmpDate= StartDate        
        tmpRow_EB = {}  #Each row for EB
        lstTOD_EB = {}  #Each Time-of-day for EB
        lstDay_EB = {}  #Each day for EB
        tmpRow_WB = {}  #Each row for WB
        lstTOD_WB = {}  #Each Time-of-day for WB
        lstDay_WB = {}  #Each day for WB
        
        for i in range(Cursor.rowcount):
            row = Cursor.fetchone()
            FROM_INT = row[0]
            TO_INT = row[1]
            HISTORY_DATE = row[2]
            TOD_idx = row[3]        
            
            if(TOD_idx != tmpTOD_idx): #TOD changed, put each row in the list
                lstTOD_EB[tmpTOD_idx] = tmpRow_EB
                lstTOD_WB[tmpTOD_idx] = tmpRow_WB
                tmpTOD_idx = TOD_idx
                tmpRow_EB = {}
                tmpRow_WB = {}
                            
            if(HISTORY_DATE != tmpDate): #Day chagned
                lstDay_EB[tmpDate] = lstTOD_EB
                lstDay_WB[tmpDate] = lstTOD_WB
                tmpDate = HISTORY_DATE
                lstTOD_EB = {}       
                lstTOD_WB = {}        
                                                                                    
            #check the list of links
            for link in range(len(Nodes)):
                #if in the list
                if((FROM_INT == Nodes[link][0]) and (TO_INT == Nodes[link][1])):  #EB                    
                    AVG_TT = row[4]
                    tmpRow_EB[link] = AVG_TT
                    break
                
                if((FROM_INT == Nodes[link][1]) and (TO_INT == Nodes[link][0])):  #WB
                    AVG_TT = row[4]
                    tmpRow_WB[link] = AVG_TT
                    break
                
        #for the last TOD and last day        
        lstTOD_EB[tmpTOD_idx] = tmpRow_EB        
        lstDay_EB[tmpDate] = lstTOD_EB
        lstTOD_WB[tmpTOD_idx] = tmpRow_WB        
        lstDay_WB[tmpDate] = lstTOD_WB  
                                        
        self.WriteLinkTT2CSV(lstDay_EB, ImpTime, Nodes,'EB')
        self.WriteLinkTT2CSV(lstDay_WB, ImpTime, Nodes,'WB')
                
                
    def WriteLinkTT2CSV(self,HistoricalData,ImputationDate,Nodes,Direction):
        NumLinks = len(Nodes)
        Year = ImputationDate[0:4]  #get the year
        Month = ImputationDate[5:7] #get the month
        Day = ImputationDate[8:10]  #get the date
        
        tbl = []
        row = []
        
        FileName = 'LinkTT\LinkTT_%s_%s_%s_%s.csv' % (Year,Month,Day,Direction)
                
        csvfile = open(FileName,'ab')
        writer = csv.writer(csvfile, delimiter=',')
        
        head = []
        head.append("Day")
        head.append("TOD")
        
        for i in range(NumLinks):
            if(Direction == 'EB'):
                ColName = "L%d_%d" % (Nodes[i][0],Nodes[i][1])
            else:
                ColName = "L%d_%d" % (Nodes[i][1],Nodes[i][0])
                
            head.append(ColName)
                        
        writer.writerow(head)        
        
        for day in HistoricalData:                        
            for TOD in HistoricalData[day]:
                row.append(day)
                row.append(TOD)                            
                for k in range(NumLinks):
                    if(HistoricalData[day][TOD].has_key(k)):
                        row.append(HistoricalData[day][TOD][k])
                    else:
                        row.append("")
                        
                tbl.append(row)
                row = []
        
        tbl.sort(key=lambda x:(x[0], x[1]))  #sorted by the third column
        
        for line in tbl:
            writer.writerow(line)            
            

                

                
            
             
            
            
        
        
        
         
        
        