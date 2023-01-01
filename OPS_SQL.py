# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 15:36:47 2020

@author: Engineering
"""
import os
import win32com.client as win32
from datetime import datetime, timedelta, date
import pandas as pd
import pyodbc
import numpy as np


try:
            
    OPSAct= r'C:\Users\Engineering\TURISTIK HAVA TASIMACILIK A.S\eng-bi - ERIM/CAI_OPS_actual.xlsx'
    
    o = win32.gencache.EnsureDispatch("Excel.Application")
    o.Visible = False
    
    output = r'C:\Users\Engineering\dummyexcel.xlsx'
    
    wb = o.Workbooks.Open(OPSAct, CorruptLoad=1)
    wb.ActiveSheet.SaveAs(output,51)
    
    wb.Close()
    
    
    df = pd.ExcelFile(output).parse('CAI OPS Actual')
    df['Dateof']=pd.to_datetime(df['Dateof'],format="%m/%d/%Y %H:%M:%S")
    df['Planning_STD']=pd.to_datetime(df['Planning_STD'],format="%m/%d/%Y %H:%M:%S")
    today = date.today()
    todayminus20= today- timedelta(days=20)
    todaymindt=datetime.combine(todayminus20, datetime.min.time())
    
    df=df[df["Planning_STD"]>=todaymindt]
    
    

    df = df.replace({np.nan: None})
    
    conn=pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=FuelMasterDB;'
                          'Trusted_Connection=yes;')
    
    for cnt0 in range(len(df.index)):
        
        dfaslist=df.iloc[cnt0].tolist()
        for cnt1 in range(len(dfaslist)):
            
            val=dfaslist[cnt1]
            if isinstance(val, str):
                if val[-2:]==".0":
                    aval=val.replace(".0","")
                    dfaslist[cnt1]=aval
            elif isinstance(val, float):
              
                val=str(val)
                if val[-2:]==".0":
                    aval=val.replace(".0","")
                    dfaslist[cnt1]=aval
                else: 
                    dfaslist[cnt1]=val
                
            elif val is None:
                pass
            
            elif isinstance(val, datetime):
                pass
            
            else:
                pyval=val.item()
                kal=str(pyval)
                if kal[-2:]==".0":
                    aval=kal.replace(".0","")
                    dfaslist[cnt1]=aval
                else: 
                    dfaslist[cnt1]=kal
               
                
        
        
        
        try:
            dfaslist[6]=datetime.strftime(dfaslist[6], "%Y-%m-%d %H:%M:%S")
            dfaslist[-3]=datetime.strftime(dfaslist[-3] ,"%Y-%m-%d %H:%M:%S")
            sublist=[dfaslist[-3],dfaslist[9],dfaslist[15],dfaslist[16]]
            dfaslist2=dfaslist + sublist
            
            cur=conn.cursor()
            cur.execute("SELECT * from dbo.OPS WHERE Planning_STD=?  AND Reg=? AND Fromm=? AND Too=?", [dfaslist[-3], dfaslist[9], dfaslist[15], dfaslist[16] ])
            ftcur=cur.fetchone()
            if ftcur:
                cur.execute("""UPDATE dbo.OPS SET [IDCustomer]=?
                              ,[WN]=?
                              ,[DayNumber]=?
                              ,[Start]=?
                              ,[Finish]=?
                              ,[IDFLActual]=?
                              ,[Dateof]=?
                              ,[LogNo]=?
                              ,[IDAirCraft]=?
                              ,[Reg]=?
                              ,[FltType]=?
                              ,[Ftype]=?
                              ,[CarrierCode]=?
                              ,[FlightNumber]=?
                              ,[FltNo]=?
                              ,[Fromm]=?
                              ,[Too]=?
                              ,[P1]=?
                              ,[Cockpit]=?
                              ,[TechnicianList]=?
                              ,[ENGINERATING]=?
                              ,[TOW]=?
                              ,[STD]=?
                              ,[OffBlock]=?
                              ,[TakeOff]=?
                              ,[STA]=?
                              ,[Landing]=?
                              ,[OnBlock]=?
                              ,[ActualFltTime]=?
                              ,[SchBlockTime]=?
                              ,[Actual_Block_Time]=?
                              ,[Accap]=?
                              ,[AvaiblSeatKm]=?
                              ,[PaxRevenueKM]=?
                              ,[BeforeUplift]=?
                              ,[Planned_Block_Fuel_kg]=?
                              ,[Extra_Fuel_kg]=?
                              ,[Actual_Block_Fuel_kg]=?
                              ,[Fuel_Uplift_Lt]=?
                              ,[Plan_Trip_Fuel_kg]=?
                              ,[Actual_Trip_Fuel_kg]=?
                              ,[Actual_Rem_Fuel_kg]=?
                              ,[BFuel_Cons_FH_Kgs]=?
                              ,[BFuel_Cons_FH_USG]=?
                              ,[Density]=?
                              ,[PaxCurAdu]=?
                              ,[PaxCurChi]=?
                              ,[PaxCurInf]=?
                              ,[PaxIntAdu]=?
                              ,[PaxIntChi]=?
                              ,[PaxIntInf]=?
                              ,[PaxTraAdu]=?
                              ,[PaxTraChi]=?
                              ,[PaxTraInf]=?
                              ,[Adult_IN]=?
                              ,[Child_IN]=?
                              ,[Inf_IN]=?
                              ,[Adult_OUT]=?
                              ,[Child_OUT]=?
                              ,[Inf_OUT]=?
                              ,[PAD]=?
                              ,[Wind]=?
                              ,[Total_Dist_NM]=?
                              ,[GreatCircle_Dist_NM]=?
                              ,[Air_Dist_NM]=?
                              ,[Tot_Baggage_Wt_kg]=?
                              ,[Cargo_Wt_kg]=?
                              ,[SCH_DEP]=?
                              ,[ACT_DEP]=?
                              ,[DelCode1]=?
                              ,[DelTime1]=?
                              ,[DelCode2]=?
                              ,[DelTime2]=?
                              ,[DelCode3]=?
                              ,[DelTime3]=?
                              ,[TotalDelay]=?
                              ,[DEPCntry]=?
                              ,[DEPCntryName]=?
                              ,[ARRCntry]=?
                              ,[ARRCntryName]=?
                              ,[Operator]=?
                              ,[Info]=?
                              ,[FuelCompany]=?
                              ,[Cabin]=?
                              ,[AC_OWNER]=?
                              ,[Planning_STD]=?
                              ,[RouteIDForCost]=?
                              ,[PredID]=?
                                WHERE Planning_STD=?  AND Reg=? AND Fromm=? AND Too=?""", dfaslist2)
                conn.commit()
            else:
               cur.execute("""INSERT INTO dbo.OPS VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          dfaslist)
               conn.commit()
        except Exception as E:
            print(E)
            
            pass  
         
    os.remove(output)
    conn.close() 
    os.system("taskkill /im EXCEL.EXE")
        
except Exception as e:
    print(e)
    os.system("taskkill /im EXCEL.EXE")

    os.remove(output)
    conn.close()
