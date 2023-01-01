import glob
from pyunpack import Archive
import time
import os
import shutil
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta
import pyodbc



start_time = time.time()

odpath=(os.getcwd()).replace("phyton\db_python\Fuel Scripts\SQL implemented","")

currpath=os.getcwd()
today=datetime.strftime(datetime.now(), '%Y-%#m-%#d')

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)




def xmlread(xmlfiletoparse):
    global Parameters
    global keyparams
    
    tree=ET.parse(xmlfiletoparse)
    root = tree.getroot()
    try:
        FPID= root.attrib['flightPlanId']
    except: FPID=""
    
    #print(FPID)

    ns='{http://aeec.aviation-ia.net/633}'
    nsall='.//{http://aeec.aviation-ia.net/633}'
    
        #FlightPlanVersion
        #VN=M633Header.attrib['versionNumber']
        #print(VN)
        
    for M633SupplementaryHeader in root.iter(ns+ 'M633SupplementaryHeader'):
        for Flight in root.findall(nsall+'Flight'):

           
            try:
                DepTm=Flight.attrib['scheduledTimeOfDeparture']
                DepTm=list(DepTm)
                DepTime=DepTm[0:10]
                DepHour=DepTm[11:16]
                DepTim="".join(DepTime)
                DepHou="".join(DepHour)
               
            except:
                    DepTim=""
                    DepHou=""
            
           
            for DepartureAirport in root.findall(nsall + 'DepartureAirport'):
                try: 
                
                    ICAOdep=DepartureAirport.find(nsall+ 'AirportICAOCode').text
                except: ICAOdep=""
               
            for ArrivalAirport in root.findall(nsall + 'ArrivalAirport'):
                try:
                    ICAOarr=ArrivalAirport.find(nsall+ 'AirportICAOCode').text
                except: ICAOarr=""
                

        for Aircraft in root.findall(nsall+ 'Aircraft'):
           #Aircraft Info
            try: 
                ACTail=Aircraft.attrib['aircraftRegistration']
            except: ACTail=""
            #print(ACTail)
            for AircraftModel in root.findall(nsall + 'AircraftModel'):
                #AC Model
                try: 
                
                    ACModel=AircraftModel.find(nsall+ 'AircraftICAOType').text
                except: ACModel=""
                #print(ACModel)
    for FlightInfo in root.iter(ns + 'FlightInfo'):
        #CPT and CallSign
        try:
            
             CPT=FlightInfo.attrib['captain']
        except: CPT=""

        try: 

            CallSign=FlightInfo.attrib['aTCCallsign']
        except: CallSign=""
     
    for Remarks in root.iter(ns+ 'Remarks'):
        for Remark in Remarks.iter(ns + 'Remark'):
            for Paragraph in Remark.findall(nsall+ 'Paragraph'):
                try: 
                #TimeHours Dep Arr
                    TimeHours=Paragraph.find(nsall+ 'Text').text
                    TimeHours=TimeHours.replace('STD-','')
                    TimeHours=TimeHours.replace("STA-", "")
                    (DepHour, ArrHour)=TimeHours.split(" ",1)
                    DepHourr=DepHour[:2]+":"+DepHour[2:]
                    ArrHourr=ArrHour[:2]+":"+ArrHour[2:]
                except:
                    DepHourr=""
                    ArrHourr=""

                #print(CPT, CallSign, DepHour, ArrHour)
    for FlightPlanHeader in root.iter(ns+ 'FlightPlanHeader'):
        try: CruDeg=FlightPlanHeader.find(nsall+ 'PerformanceFactor').text
        except: CruDeg=""
        
        for RouteInformation in FlightPlanHeader.findall(ns+'RouteInformation'):
            #Route Optimization
            try:
                Optim=RouteInformation.attrib['optimization']
            except: Optim=""

            
            try: RouNam=RouteInformation.attrib['routeName']
            except: RouNam=""
            
            #print(Optim, RouNam)
            
            try: RouteD=RouteInformation.find(nsall+ 'RouteDescription').text
            except: RouteD=""
            #print(RouteD)
            #RoutWYP=RouteD.split(" ")
            #print(len(RoutWYP))
            FLS=[]
            try:
                
                for FL in re.finditer("F", RouteD):
                    if RouteD[FL.end():FL.end()+3].isnumeric()== True:
                       
                        FLs=int(RouteD[FL.end():FL.end()+3])   
                        
                        if FLs % 10 == 0:
                            
                            FLS.append(FLs)  
                           
                
                FLS=list(dict.fromkeys(FLS))
                Flev=max(FLS)

            except: Flev=""
            
            #print(Flev)
            for GroundDistance in RouteInformation.findall(ns+'GroundDistance'):
                
                try: GrnD=GroundDistance.find(nsall+ 'Value').text
                except: GrnD=""
            for AirDistance in RouteInformation.findall(ns+'AirDistance'):
                
                try: AirD=AirDistance.find(nsall+ 'Value').text
                except: AirD=""
                
            for GreatCircleDistance in RouteInformation.findall(ns+'GreatCircleDistance'):
                try: GCD=GreatCircleDistance.find(nsall+ 'Value').text
                except: GCD=""
            #print(GrnD, AirD, GCD)    
    for FuelHeader in root.iter(ns+ 'FuelHeader'):
        for TripFuel in FuelHeader.findall(ns+ 'TripFuel'):
            for EstimatedWeight in TripFuel.findall(ns+ 'EstimatedWeight'):
            
                try: EstW=EstimatedWeight.find(nsall+ 'Value').text
                except: EstW=""
                #print(EstW)
            for Duration in TripFuel.findall(ns+ 'Duration'):
                try:
                    RawDrt=Duration.find(nsall+ 'Value').text
                    
                    
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    Drt=Hr+ ":" + Min
                except: Drt=""
                
                   
                    

            
        for ContingencyFuel in FuelHeader.findall(ns+ 'ContingencyFuel'):
            for EstimatedWeight in ContingencyFuel.findall(nsall + 'EstimatedWeight'):
                try: CEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: CEstW=""
                #print(CEstW)
            for Duration in ContingencyFuel.findall(ns+ 'Duration'):
                try:
                    RawDrt=Duration.find(nsall+ 'Value').text
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    cDrt=Hr+ ":" + Min
                except: cDrt=""
                
                #print(cDrt)
            for ContingencyPolicy in ContingencyFuel.findall(ns+ 'ContingencyPolicy'):
                try: ContPol=ContingencyPolicy.attrib['policyName']
                except: ContPol=""
                #print(ContPol)
        for AlternateFuels in FuelHeader.findall(ns+ 'AlternateFuels'):
            for AlternateFuel in AlternateFuels.findall(nsall + 'AlternateFuel'):
                for EstimatedWeight in AlternateFuel.findall(nsall + 'EstimatedWeight'):
                    try: AEstW=EstimatedWeight.find(nsall+ 'Value').text
                    except: AEstW=""

                    #print(AEstW)
                for Duration in AlternateFuel.findall(ns+ 'Duration'):
                    try: 
                        RawDrt=Duration.find(nsall+ 'Value').text
                        CntHr=RawDrt.find("H")
                    
                        CntT=RawDrt.find("T")
                   
                        CntMin=RawDrt.find("M")
                        if CntMin==-1:
                            Min="00"
                            if CntHr==CntT+2:
                                Hr="0"+RawDrt[CntT+1]
                            else:
                                Hr=RawDrt[CntT+1:CntT+3]
                        elif CntHr==-1:
                            Hr="00"
                            if CntMin==CntT+2:
                                Min="0"+RawDrt[CntT+1]
                            else:
                                Min=RawDrt[CntT+1:CntT+3]
                        elif CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                            if CntHr==CntMin-2:
                                Min="0"+RawDrt[CntHr+1]
                            else:
                                Min=RawDrt[CntHr+1:CntHr+3]
                        elif CntHr==CntT+3:
                            Hr=RawDrt[CntT+1:CntT+3]
                            if CntHr==CntMin-2:
                                Min="0"+RawDrt[CntHr+1]
                            else:
                                Min=RawDrt[CntHr+1:CntHr+3]
                        aDrt=Hr+ ":" + Min
                    except: aDrt=""
                    
                    #print(aDrt)
        for FinalReserve in FuelHeader.findall(ns+ 'FinalReserve'):
            for EstimatedWeight in FinalReserve.findall(nsall + 'EstimatedWeight'):

                try:FEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: FEstW=""
                #print(FEstW)
            for Duration in FinalReserve.findall(ns+ 'Duration'):
                try:
                    RawDrt=Duration.find(nsall+ 'Value').text
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    FDrt=Hr+ ":" + Min
                except: FDrt=""
                #print(FDrt)

        for TakeOffFuel in FuelHeader.findall(ns+ 'TakeOffFuel'):
            for EstimatedWeight in TakeOffFuel.findall(nsall + 'EstimatedWeight'):

                try: TOEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: TOEstW=""
                #print(TOEstW)
            for Duration in TakeOffFuel.findall(ns+ 'Duration'):
                try: 
                
                    RawDrt=Duration.find(nsall+ 'Value').text
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    TODrt=Hr+ ":" + Min
                except: TODrt=""
                #print(TODrt)
        for TaxiFuel in FuelHeader.findall(ns+ 'TaxiFuel'):
            for EstimatedWeight in TaxiFuel.findall(nsall + 'EstimatedWeight'):
                try: TaEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: TaEstW="" 
                #print(TaEstW)
        for BlockFuel in FuelHeader.findall(ns+ 'BlockFuel'):
            for EstimatedWeight in BlockFuel.findall(nsall + 'EstimatedWeight'):
                try: BEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: BEstW=""
                #print(BEstW)
            for Duration in BlockFuel.findall(ns+ 'Duration'):
                try:
                    RawDrt=Duration.find(nsall+ 'Value').text
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    BDrt=Hr+ ":" + Min
                except: BDrt=""
                #print(BDrt)
        for ArrivalFuel in FuelHeader.findall(ns+ 'ArrivalFuel'):
            for EstimatedWeight in ArrivalFuel.findall(nsall + 'EstimatedWeight'):

                try: AFEstW=EstimatedWeight.find(nsall+ 'Value').text
                except: AFEstW=""
                #print(AFEstW)
            for Duration in ArrivalFuel.findall(ns+ 'Duration'):
                try:
                    RawDrt=Duration.find(nsall+ 'Value').text
                    CntHr=RawDrt.find("H")
                    
                    CntT=RawDrt.find("T")
                   
                    CntMin=RawDrt.find("M")
                    if CntMin==-1:
                        Min="00"
                        if CntHr==CntT+2:
                            Hr="0"+RawDrt[CntT+1]
                        else:
                            Hr=RawDrt[CntT+1:CntT+3]
                    elif CntHr==-1:
                        Hr="00"
                        if CntMin==CntT+2:
                            Min="0"+RawDrt[CntT+1]
                        else:
                            Min=RawDrt[CntT+1:CntT+3]
                    elif CntHr==CntT+2:
                        Hr="0"+RawDrt[CntT+1]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    elif CntHr==CntT+3:
                        Hr=RawDrt[CntT+1:CntT+3]
                        if CntHr==CntMin-2:
                            Min="0"+RawDrt[CntHr+1]
                        else:
                            Min=RawDrt[CntHr+1:CntHr+3]
                    AFDrt=Hr+ ":" + Min
                except: AFDrt="" 
                #print(AFDrt)  
    for WeightHeader in root.iter(ns + 'WeightHeader'):
        for DryOperatingWeight in WeightHeader.findall(ns+ 'DryOperatingWeight'):
            for EstimatedWeight in DryOperatingWeight.findall(ns+ 'EstimatedWeight'):
                try: DOW=EstimatedWeight.find(nsall+ 'Value').text
                except: DOW=""
                #print(DOW)
        for Load in WeightHeader.findall(ns+ 'Load'):
            for EstimatedWeight in Load.findall(ns+ 'EstimatedWeight'):
                try: Payload=EstimatedWeight.find(nsall+ 'Value').text
                except: Payload=""
                #print(Payload)
        for ZeroFuelWeight in WeightHeader.findall(ns+ 'ZeroFuelWeight'):
            for EstimatedWeight in ZeroFuelWeight.findall(ns+ 'EstimatedWeight'):
                try: ZFW=EstimatedWeight.find(nsall+ 'Value').text
                except: ZFW=""
                #print(ZFW)
        for TakeoffWeight in WeightHeader.findall(ns+ 'TakeoffWeight'):
            for EstimatedWeight in TakeoffWeight.findall(ns+ 'EstimatedWeight'):
                try: TOW=EstimatedWeight.find(nsall+ 'Value').text
                except: TOW=""
                #print(TOW)
        for LandingWeight in WeightHeader.findall(ns+ 'LandingWeight'):
            for EstimatedWeight in LandingWeight.findall(ns+ 'EstimatedWeight'):
                try: LDGWT=EstimatedWeight.find(nsall+ 'Value').text
                except: LDGWT=""
                #print(LDGWT)
                
    try: FPID
    except: FPID=""

    try: DepTim
    except: DepTim=""

    try: DepHou
    except: DepHou=""


    try: ICAOdep
    except: ICAOdep=""

    try: ICAOarr
    except: ICAOarr=""
    try: ACTail
    except: ACTail=""

    try: ACModel
    except: ACModel=""
    try: CPT
    except: CPT=""

    try: CallSign
    except: CallSign=""

    try: DepHourr
    except: DepHourr=""

    try: ArrHourr
    except: ArrHourr=""

    try: Optim
    except: Optim=""

    try: RouNam
    except: RouNam=""

    try: Flev
    except: Flev=""

    try: GrnD
    except: GrnD=""

    try: AirD
    except: AirD=""

    try: GCD
    except: GCD=""

    try: EstW
    except: EstW=""

    try: Drt
    except: Drt=""

    try: CEstW
    except: CEstW=""

    try: cDrt
    except: cDrt=""

    try: AEstW
    except: AEstW=""

    try: aDrt
    except: aDrt=""

    try: FEstW
    except: FEstW=""

    try: FDrt
    except: FDrt=""

    try: TOEstW
    except: TOEstW=""

    try: TODrt
    except: TODrt=""

    try: TaEstW
    except: TaEstW=""

    try: BEstW
    except: BEstW=""

    try: BDrt
    except: BDrt=""

    try: AFEstW
    except: AFEstW=""

    try: AFDrt
    except: AFDrt=""

    try: DOW
    except: DOW=""

    try: Payload
    except: Payload=""


    try: ZFW
    except: ZFW=""


    try: TOW
    except: TOW=""

    try: LDGWT
    except: LDGWT=""





    


    
              
         
    #Clmns=["Flight Plan ID", "Date", "Departure Hour1", "Departure Airport", "Arrival Airport", "Aircraft", "AC Model", "Cruise Degradation", "CPT", "Flight ID", "Departure Hour", "Arrival Hour", "Route Optimization",
           #"Route Name", "Flight Level", "Ground Distance", "Air Distance", "Great Circle Distance", "Trip Fuel Weight", "Trip Fuel Duration", "Contingency Fuel Weight",
           #"Contingency Fuel Duration", "Alternate Fuel Weight", "Alternate Fuel Duration", "Final Fuel Reserve", "Capable Duration", "Minimum TO Fuel Weight", "Full Duration",
           #"Taxi Fuel Weight", "Block Fuel Weight", "Block Full Duration", "Arrival Fuel Weight", "Arrival Fuel Duration", "Dry Operating Weight", "Payload",
          # "Zero Fuel Weight", "Takeoff Weight", "Landing Weight"]
    DepDateTime= DepTim +" "+ DepHou
    DepDateTimeasTime=datetime.strptime(DepDateTime, '%Y-%m-%d %H:%M')
    DepDateasstr=datetime.strftime(DepDateTimeasTime, '%Y-%m-%d')
    FPID=FPID.lstrip("0")
    KeyFP=FPID+ "_" + DepDateasstr
    Parameters=[FPID, DepDateTime, ICAOdep, ICAOarr, ACTail, ACModel, CruDeg, CPT, CallSign, DepHourr, ArrHourr, Optim,
                RouNam, Flev, GrnD, AirD, GCD, EstW, Drt, CEstW, cDrt, AEstW, aDrt, FEstW, FDrt, TOEstW, TODrt, TaEstW, 
                BEstW, BDrt,AFEstW, AFDrt, DOW, Payload, ZFW, TOW, LDGWT, KeyFP]

    keyparams=[DepDateTime,ICAOdep, ICAOarr, ACTail, CallSign]
     
    
    

              
dlt = r'C:\Users\Engineering\Desktop\Engineering\dummyFP'





conn=pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=FuelMasterDB;'
                      'Trusted_Connection=yes;')

n=0
succ=0
unsucc=0
for iii in range(7):
    
    
    yesterday=datetime.strftime(datetime.now() - timedelta(iii), '%Y\%m\%d')


    
    
    flts=glob.glob(r'C:\FliteBrief\Archive/' + str(yesterday)+'/*')
                   
    flts.sort()
    print(flts)
    if len(flts)>0:
       
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
                
            
            xmlread(xmlfiletoparse)
            keyfp0=Parameters[-1]
            
            
            
            cur=conn.cursor()
            cur.execute("SELECT * from dbo.FlightPlans WHERE Date=? AND DepartureAirport=? AND ArrivalAirport=? AND Aircraft=? AND FlightID=?", keyparams)
            
            ftcur=cur.fetchone()
    
            if not ftcur:
               cur.execute("""INSERT INTO dbo.FlightPlans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          Parameters)
               conn.commit()
            else:
               
               cur.execute("DELETE FROM dbo.FlightPlans WHERE Date=? AND DepartureAirport=? AND ArrivalAirport=? AND Aircraft=? AND FlightID=?", keyparams)
               
               cur.execute("""INSERT INTO dbo.FlightPlans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          Parameters) 
                           
               conn.commit()

            succ+=1
            
         except Exception as exceptionn:
            unsucc+=1
            
            with open(currpath + "\LOG"+ "\FP_LOG"+today+".txt","a") as file:
                file.write(str(flts[k])+"\n"+ str(exceptionn) +"\n")    

         finally:


            shutil.rmtree(dlt)
                
                
for iii in range(7):
    
    
    yesterday=datetime.strftime(datetime.now() - timedelta(iii), '%Y\%m\%d')


    
    
    flts=glob.glob(r'C:\FliteBrief\EffPathAsLinuxFormattedDirectory/' + str(yesterday)+'/*')
                   
    flts.sort()
    print(flts)
    if len(flts)>0:
       
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
                
            
            xmlread(xmlfiletoparse)
            keyfp0=Parameters[-1]
            
            
            
            cur=conn.cursor()
            cur.execute("SELECT * from dbo.FlightPlans WHERE Date=? AND DepartureAirport=? AND ArrivalAirport=? AND Aircraft=? AND FlightID=? ", keyparams)
            
            ftcur=cur.fetchone()
    
            if not ftcur:
               cur.execute("""INSERT INTO dbo.FlightPlans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          Parameters)
               conn.commit()
            else:
               
               cur.execute("DELETE FROM dbo.FlightPlans WHERE Date=? AND DepartureAirport=? AND ArrivalAirport=? AND Aircraft=? AND FlightID=?", keyparams)
               
               cur.execute("""INSERT INTO dbo.FlightPlans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          Parameters) 
                           
               conn.commit()

            succ+=1
            
         except Exception as exceptionn:
            unsucc+=1
            
            with open(currpath + "\LOG"+ "\FP_LOG"+today+".txt","a") as file:
                file.write(str(flts[k])+"\n"+ str(exceptionn) +"\n")    

         finally:


            shutil.rmtree(dlt)
                   


with open(currpath + "\LOG"+ "\FP_LOG"+today+".txt","a") as file:
    file.write("""
               
               unsucc: """+ str(unsucc)+ "succ: "+ str(succ)+"""
               **************************************************\n""") 
      
print(n)
elapsed_time = time.time() - start_time
print(elapsed_time)
conn.commit()

conn.close()
