'''
Created on Oct 12, 2014

@author: mingchen7
'''

import os;


class StationStatus:
    __StationList__ = None;
    
    def __init__(self,Stations):
        self.__StationList__ = Stations;
        
        
    def checkStatus(self,f):
        StationRecords = {}
        for location in self.__StationList__:
            StationRecords[location] = 0
        
        for line in f:
            OneRecord = line.split(';')
            if(len(OneRecord) >= 3): #the record is complete
                Time = OneRecord[0]
                Location = OneRecord[1]
                MacSequence = OneRecord[2].split('\'')
                count = (len(MacSequence) -1) /2
                
                if(Location in self.__StationList__):  #if this location is in the list
                    if(len(MacSequence) == 1):
                        continue
                    else:
                        StationRecords[Location] = StationRecords[Location] + count
                    
            
        return StationRecords
      
            
            
        
            
            
        
        
    