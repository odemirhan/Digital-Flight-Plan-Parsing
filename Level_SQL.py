# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 17:02:52 2020

@author: Engineering
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta, date
import math
import isodate


def cumulativeFB(counter):
    subAeroDB=AeroDB[(AeroDB["flight_id"]==AeroDB.iat[counter, 5]) & AeroDB["Value_Name"].str.startswith("Fuel Burn") & (AeroDB["Value Start"]<=AeroDB.iat[counter, -3]) ]
    totalburn=subAeroDB['Value'].sum()
    return totalburn

for cntday in range(5):
    today = date.today()
    
    specdate= datetime.strftime((today - timedelta(days=cntday)), '%Y-%m-%d %H:%M:%S')
    print(specdate)
    specplus= datetime.strftime((today - timedelta(days=cntday) + timedelta(days=1)), '%Y-%m-%d %H:%M:%S')
    print(specplus)
    username='aerobytes_app'
    pwd='aerobytes'
    conn155=pyodbc.connect('Driver={SQL Server};'
                              'Server=10.1.0.155;'
                              'Database=fdm_v03_48;'
                              'UID='+username+';PWD='+pwd+';'
                              )
    
    
    conn36=pyodbc.connect('Driver={SQL Server};'
                              'Server=localhost;'
                              'Database=FuelMasterDB;'
                              'Trusted_Connection=yes;'
                              )
    AeroDB=pd.read_sql_query("exec CAI_SP_GET_LEVEL_AND_FB ?, ?",conn155,  params=(specdate, specplus))
    WypDB=pd.read_sql_query("Select * From Waypoints WHERE Date=?", conn36, params=[specdate])
    FPDB=pd.read_sql_query("""Select DepartureAirport, ArrivalAirport, Aircraft, KeyFP
                           From FlightPlans WHERE Date>? AND Date<?""", conn36, params=(specdate, specplus))
    
    WypDB=pd.merge(WypDB, FPDB, left_on='KeyWYP', right_on='KeyFP' )
    AeroDB=AeroDB.groupby(["A/C TO Datetime","A/C TD Datetime","aircraft_reg","TO Airport","TD Airport","flight_id",
                           "State ID","Value_Name","Value Start","Value End"])['Value'].agg('sum').reset_index()
    
    
    
    
    
    
        
    WypDB['FID']="N/A"
    for cnt0 in range(len(WypDB.index)):
       parameterwyp=WypDB.iat[cnt0, -3]+WypDB.iat[cnt0, -5]+WypDB.iat[cnt0, -4]+WypDB.iat[cnt0, 2]
       for cnt1 in range(len(AeroDB.index)):
           dummy1=datetime.strftime(AeroDB.iat[cnt1, 0], '%Y-%m-%d')
           
           dummy2=AeroDB.iat[cnt1, 2] 
           dummy2=dummy2.replace("-","")
          
           parameterae=dummy2+AeroDB.iat[cnt1, 3]+ AeroDB.iat[cnt1, 4]+dummy1
           
           if parameterwyp==parameterae:
               
               WypDB.iat[cnt0,-1]=AeroDB.iat[cnt1, 5]
               break
               
    
    
    DF2write=pd.DataFrame([])
    
    for cnt2 in range(len(AeroDB.index)):
        #altitude 0 ekle
        try:
            if AeroDB.iat[cnt2, 7]=="Altitude - average":
                if DF2write.empty:
                    dummydf1=pd.DataFrame([AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 0 ], "Altitude- FDM", 0])
                    dummydf2=pd.DataFrame([AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 1 ], "Altitude- FDM", 0])
                    DF2write=DF2write.append(dummydf1.T)
                    DF2write=DF2write.append(dummydf2.T)
                else:
                    checkFIDexist=len(DF2write[(DF2write.iloc[:, 0]==AeroDB.iat[cnt2, 5]) & (DF2write.iloc[:, 2]=="Altitude- FDM")])
                    if checkFIDexist==0:
                        dummydf1=pd.DataFrame([AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 0 ], "Altitude- FDM", 0])
                        dummydf2=pd.DataFrame([AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 1 ], "Altitude- FDM", 0])
                        DF2write=DF2write.append(dummydf1.T)
                        DF2write=DF2write.append(dummydf2.T)
                    
                dummylist1=[AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 8 ], "Altitude- FDM", math.ceil(AeroDB.iat[cnt2, 10])]
                dummyDF11=pd.DataFrame(dummylist1)
                
                dummylist2=[AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, 9 ], "Altitude- FDM", math.ceil(AeroDB.iat[cnt2, 10])]
                dummyDF22=pd.DataFrame(dummylist2)
                
                DF2write=DF2write.append(dummyDF11.T)
                DF2write=DF2write.append(dummyDF22.T)
            
            #fuel part
            else: 
                if not AeroDB.iat[cnt2, 7]=="Fuel Burn - A Taxi Out":
                    dummydf2=pd.DataFrame([AeroDB.iat[cnt2, 5], AeroDB.iat[cnt2, -2 ], "Fuel Burn- FDM"  , math.ceil(cumulativeFB(cnt2))])
                   
                    DF2write=DF2write.append(dummydf2.T)
        except Exception as E1:
            print(E1)
            pass
    
    
    for cnt3 in range(len(WypDB.index)):
        try:
            aa=WypDB.iat[cnt3,11]
            
            try:
                duration=isodate.parse_duration(aa).total_seconds()
            except:
                duration=0
                
            
            subDB=AeroDB[AeroDB["flight_id"]==WypDB.iat[cnt3, -1]]
            TOdatetime=subDB.iat[0,0]
            instantDT=TOdatetime+ timedelta(seconds=duration)
            
            dummydf1=pd.DataFrame([WypDB.iat[cnt3, -1], instantDT, "Fuel Burn- FP"  , WypDB.iat[cnt3, 14] ])
            
                       
            DF2write=DF2write.append(dummydf1.T)
        
            
            subwypDB=WypDB[WypDB["KeyFP"]==WypDB.iat[cnt3,-2]]
            if WypDB.iat[cnt3, 0]==1 or WypDB.iat[cnt3, 0]==len(subwypDB):
                
                
                dummydf2=pd.DataFrame([WypDB.iat[cnt3, -1], instantDT, "Altitude- FP"  , 0 ])
            
                       
                DF2write=DF2write.append(dummydf2.T)
                
            else:
                dummydf3=pd.DataFrame([WypDB.iat[cnt3, -1], instantDT, "Altitude- FP"  ,  WypDB.iat[cnt3, 6]*100 ])
            
                       
                DF2write=DF2write.append(dummydf3.T)
                
        except:
           
            pass
    
    
    for cnt4 in range(len(DF2write.index)):
        try:
            
            dfaslist=DF2write.iloc[cnt4].astype(str).tolist()
            
            
            try:
                
                dfaslist[1]=datetime.strptime(dfaslist[1], '%Y-%m-%d %H:%M:%S.%f')
            except:
                dfaslist[1]=datetime.strptime(dfaslist[1], '%Y-%m-%d %H:%M:%S')
                
            cur1=conn36.cursor()
            cur1.execute("SELECT * from dbo.Level WHERE FID=?  AND InstantTime=? AND Category=?", [dfaslist[0], dfaslist[1], dfaslist[2]])
            ftcur=cur1.fetchone()
            
            if not ftcur:
               cur1.execute("""INSERT INTO dbo.Level VALUES(?,?,?,?)""",
                          dfaslist)
               conn36.commit()
               
        except Exception as E3:
            print(E3)
            pass
       
