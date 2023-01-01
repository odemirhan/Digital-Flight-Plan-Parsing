import glob
from pyunpack import Archive
import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pyodbc



def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)


        

def waypoints(path):
    global param
    
    
    tree=ET.parse(path)
    root = tree.getroot()
    try:
        FPID= root.attrib['flightPlanId']
    except: FPID=""
    
    ns='{http://aeec.aviation-ia.net/633}'
    nsall='.//{http://aeec.aviation-ia.net/633}'
    
        
        
    for M633SupplementaryHeader in root.iter(ns+ 'M633SupplementaryHeader'):
        for Flight in root.findall(nsall+'Flight'):

           
            try:
                DepTm=Flight.attrib['scheduledTimeOfDeparture']
                DepTm=list(DepTm)
                DepTime=DepTm[0:10]
                DepTim="".join(DepTime)
                
               
            except:
                DepTim=""
                
    for Waypoints in root.iterfind(ns + 'Waypoints'):
        
       
        for Waypoint in Waypoints.iterfind(ns+'Waypoint'):
            Mach=''
            TFPW=''
            CFT=''
            Burnoff=''
            CBurnoff=''
            try:
                SequenceId=Waypoint.attrib['sequenceId']
                WaypointName=Waypoint.attrib['waypointName']
                               
               
            except:
                SequenceId=''
                WaypointName=''
            for Coordinates in Waypoint.iterfind(ns +'Coordinates'):
                try:
                    longi=Coordinates.attrib['longitude']
                    latti=Coordinates.attrib['latitude']
                except:
                    longi=''
                    latti=''
            for Altitude in Waypoint.iterfind(ns +'Altitude'):
                try:
                    EstAlt=Altitude.find(nsall+ 'Value').text
                    
                except:
                    EstAlt=''
            for MachNumber in Waypoint.iterfind(ns +'MachNumber'):
                try:
                    Mach=MachNumber.find(nsall+ 'Value').text
                    
                except:
                    Mach=''
            for GroundDistance in Waypoint.iterfind(ns +'GroundDistance'):
                try:
                    GD=GroundDistance.find(nsall+ 'Value').text
                    
                except:
                    GD=''
            for RemainingGroundDistance in Waypoint.iterfind(ns +'RemainingGroundDistance'):
                try:
                    RemGD=RemainingGroundDistance.find(nsall+ 'Value').text
                    
                except:
                    RemGD=''
            for TimeFromPreviousWaypoint in Waypoint.iterfind(ns +'TimeFromPreviousWaypoint'):
                try:
                    TFPW=TimeFromPreviousWaypoint.find(nsall+ 'Value').text
                    
                except:
                    TFPW=''
            for CumulatedFlightTime in Waypoint.iterfind(ns +'CumulatedFlightTime'):
                try:
                    CFT=CumulatedFlightTime.find(nsall+ 'Value').text
                    
                except:
                    CFT=''
            for RemainingFlightTime in Waypoint.iterfind(ns +'RemainingFlightTime'):
                try:
                    RFT=RemainingFlightTime.find(nsall+ 'Value').text
                    
                except:
                    RFT=''
            for BurnOff in Waypoint.iterfind(ns +'BurnOff'):
                try:
                    Burnoff=BurnOff.find(nsall+ 'Value').text
                    
                except:
                    Burnoff=''
            for CumulatedBurnOff in Waypoint.iterfind(ns +'CumulatedBurnOff'):
                try:
                    CBurnoff=CumulatedBurnOff.find(nsall+ 'Value').text
                    
                except:
                    CBurnoff=''
            for FuelOnBoard in Waypoint.iterfind(ns +'FuelOnBoard'):
                try:
                    FuelOnBoard=FuelOnBoard.find(nsall+ 'Value').text
                    
                except:
                    FuelOnBoard=''
            for AircraftWeight in Waypoint.iterfind(ns +'AircraftWeight'):
                try:
                    AircraftWeight=AircraftWeight.find(nsall+ 'Value').text
                    
                except:
                    AircraftWeight=''
                    
                    
                    

            FPID=FPID.lstrip("0")
            KeyWyp=FPID+"_"+DepTim
            param=[SequenceId, FPID, DepTim,  WaypointName, latti, longi, EstAlt, Mach, GD, RemGD, TFPW, CFT, RFT, Burnoff, CBurnoff, FuelOnBoard, AircraftWeight, KeyWyp]
            print(param)
            keywyp0=param[-1]
                
                
                
            cur=conn.cursor()
            cur.execute("SELECT * from dbo.Waypoints WHERE KeyWYP=? AND SequenceID=?", [keywyp0, param[0]])
            ftcur=cur.fetchone()
    
            if not ftcur:
               cur.execute("""INSERT INTO dbo.Waypoints VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          param)
               
            else:
                cur.execute("DELETE FROM dbo.Waypoints WHERE KeyWYP=?", [keywyp0])
                cur.execute("""INSERT INTO dbo.Waypoints VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          param)
                
            conn.commit()
            
                
        

                

dlt = r'C:\Users\Engineering\Desktop\Engineering\dummyWYP'
succ_read=0
fail_read=0

currpath=os.getcwd()



conn=pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=FuelMasterDB;'
                      'Trusted_Connection=yes;')
    


for cnt1 in range(3):
    
    
    yesterday=datetime.strftime(datetime.now() - timedelta(cnt1), '%Y\%m\%d')
    print(yesterday)

    n=0
    
   
    flts=glob.glob(r'C:\FliteBrief\Archive/' + str(yesterday)+'/*')
    flts.sort()
    
    for k in range(len(flts)):
     
        try:
            createFolder(dlt)
            Archive(flts[k]).extractall(dlt)
            datfile=glob.glob(dlt+'/*.dat')
            datfile=datfile[0]
           
            Archive(datfile).extractall(dlt)
            xmlfile=glob.glob(dlt+'/*.xml')
            for cntXML in range(len(xmlfile)):
                if 'OFP' in xmlfile[cntXML]:
                    xmlfiletoparse=xmlfile[cntXML]
                
            
            waypoints(xmlfiletoparse)
            conn.commit()
            
            
            succ_read+=1
        except Exception as E:
            print(E)
            fail_read+=1
        
            

        finally:


            shutil.rmtree(dlt)
            
            
for cnt1 in range(3):
    
    
    yesterday=datetime.strftime(datetime.now() - timedelta(cnt1), '%Y\%m\%d')
    print(yesterday)

    n=0
    
   
    flts=glob.glob(r'C:\FliteBrief\EffPathAsLinuxFormattedDirectory/' + str(yesterday)+'/*')
    flts.sort()
    
    for k in range(len(flts)):
     
        try:
            createFolder(dlt)
            Archive(flts[k]).extractall(dlt)
            datfile=glob.glob(dlt+'/*.dat')
            datfile=datfile[0]
           
            Archive(datfile).extractall(dlt)
            xmlfile=glob.glob(dlt+'/*.xml')
            for cntXML in range(len(xmlfile)):
                if 'OFP' in xmlfile[cntXML]:
                    xmlfiletoparse=xmlfile[cntXML]
                
            
            waypoints(xmlfiletoparse)
            conn.commit()
            
            
            succ_read+=1
        except Exception as E:
            print(E)
            fail_read+=1
        
            

        finally:


            shutil.rmtree(dlt)
            
           
    #except:
        #n+=1



  
conn.commit()
conn.close()
print("succ=", succ_read)
print("fail=", fail_read)
    
              
         
   
    
