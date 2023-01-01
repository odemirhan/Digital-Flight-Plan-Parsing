import pandas as pd
import glob
import os
import PyPDF2
from pyunpack import Archive
import time
import shutil
import pyodbc
from datetime import datetime
from datetime import timedelta
import copy


odpath=(os.getcwd()).replace("phyton\db_python\Fuel Scripts\SQL implemented","")

conn=pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=FuelMasterDB;'
                      'Trusted_Connection=yes;')

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
def validate(date_text):
    try:
        
        strp=datetime.strptime(date_text, '%d/%m/%Y')
        
        return True
    except:
        
        return False
    
def validate2(date_text2):
    try:
        
        strp2=datetime.strptime(date_text2, '%d/%m/%y')
        
        return True
    except:
        
        return False

for iii in range(3):
            
    yesterday=datetime.strftime(datetime.now() - timedelta(iii), '%Y\%m\%d')

    start_time = time.time()
    cnt1=0
    succcnt=0

    tankcnt=0
    failcnt=0
    currpath=r"C:\Users\Engineering\Desktop\Engineering"
    df=pd.DataFrame()
    txt=open(currpath+ "/dateformat.txt", "a")

    dummypath=currpath + '/dummytk'


    content=glob.glob(r'C:\FliteBrief\Archive/' + str(yesterday)+'/*')
                    
    content.sort()
    for i in range(len(content)):
        
        try:

                       
            createFolder(dummypath)
            
            
            Archive(content[i]).extractall(dummypath)
            datfile=glob.glob(dummypath+'/*.dat')
            datfile=datfile[0]
           
            Archive(datfile).extractall(dummypath)
            pdffiles=glob.glob(dummypath+'/*.pdf')
            pdffile=pdffiles[0]
            with open(pdffile, 'rb') as pdftoread:
                pdfReader = PyPDF2.PdfFileReader(pdftoread) 

                rawText0 = (pdfReader.getPage(0)).extractText()
                rawList0 = rawText0.split(" ")
                rawList0 = list(filter(None, rawList0))
               
                nnn=0
                kkk=0
               
                for i in range(len(rawList0)):
                    dummyvar=rawList0[i]
                    if validate2(dummyvar[0:8]) and kkk==0:
                        kkk+=1
                        splandate=datetime.strptime(dummyvar[0:8], '%d/%m/%y')
                        plandate=datetime.strftime(splandate, '%Y-%m-%d')
                        
                    elif validate(dummyvar) and nnn==0:
                        nnn+=1
                        
                        
                        sdepdate=datetime.strptime(dummyvar, '%d/%m/%Y')
                        keydepdate=datetime.strftime(sdepdate, '%Y-%m-%d')
                        depdate=datetime.strftime(sdepdate, '%d-%m-%y')
                        
                        
                        

                       
                name=str(rawList0[1])+'_'+str(depdate)+'.pdf'
                   
                
                rawText1 = (pdfReader.getPage(-1)).extractText()
                rawText2 = (pdfReader.getPage(-2)).extractText()
                rawText = rawText2+rawText1
                rawText= rawText.replace('Page', '')
                
                rawList = rawText.split(" ")
                rawList = list(filter(None, rawList))
                

                

                copyrawList=copy.copy(rawList)
                for cntt in range(len(copyrawList)):
                    if copyrawList[cntt]=='-':
                        if copyrawList[cntt+2]=='of':
                            if len(copyrawList[cntt+3])==1:
                                
                               del rawList[cntt:cntt+4]
                                
                            else:
                                
                                del rawList[cntt:cntt+3]
                               

                
                    
                                  
                                  

                    
            try:
                
                
                    
                    
                shutil.move(pdffile, os.path.join(odpath, "Flight OPS","FUEL","DBs","FltPlans","FlightPlans", name))
                shutil.rmtree(dummypath)
            except PermissionError:
                
                time.sleep(5)
                
                shutil.move(pdffile, os.path.join(odpath, "Flight OPS","FUEL","DBs","FltPlans","FlightPlans", name))
                shutil.rmtree(dummypath)
            

            
        
            for cnt0 in range(len(rawList)):
                

                
                if rawList[cnt0]=='20PC':
                    PlanID=rawList0[1].lstrip("0")
                    KeyTK=PlanID+"_"+keydepdate
                    List=[PlanID, keydepdate, plandate, rawList[cnt0+12],rawList[cnt0+13],rawList[cnt0+14],
                          rawList[cnt0+15],rawList[cnt0+16],rawList[cnt0+17],rawList[cnt0+28],rawList[cnt0+29],
                          rawList[cnt0+30],rawList[cnt0+31],rawList[cnt0+32],rawList[cnt0+33].replace('TANKERING', ''),
                          rawList[cnt0+21],rawList[cnt0+22],rawList[cnt0+23],rawList[cnt0+24],
                          rawList[cnt0+25],rawList[cnt0+26], KeyTK ]
                    ListProcessed= [ele.lstrip('0') for ele in List]   
                    
                    try:
                        cur=conn.cursor()
                        cur.execute("SELECT * from dbo.Tankering WHERE KeyTK=?", [KeyTK])
                        ftcur=cur.fetchone()
                        
                        if not ftcur:
                           
                            cur.execute("INSERT INTO dbo.Tankering VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ListProcessed)
                            
                            
                        else:
                            
                            cur.execute("DELETE FROM dbo.Tankering WHERE KeyTK=?", [KeyTK])
                            
                            cur.execute("INSERT INTO dbo.Tankering VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ListProcessed)
                                      
                            
                           
                    except Exception as e: 
                        print(e, 'SQLe Yazamadi')
                    
                    
                    tankcnt+=1
                

            succcnt+=1
            conn.commit()
                
        except:
            
            failcnt+=1
            if os.path.isdir(dummypath)==True:
                shutil.rmtree(dummypath)
                
                
content=glob.glob(r'C:\FliteBrief\EffPathAsLinuxFormattedDirectory/' + str(yesterday)+'/*')
                
content.sort()
for i in range(len(content)):
    
    try:

                   
        createFolder(dummypath)
        
        
        Archive(content[i]).extractall(dummypath)
        datfile=glob.glob(dummypath+'/*.dat')
        datfile=datfile[0]
       
        Archive(datfile).extractall(dummypath)
        pdffiles=glob.glob(dummypath+'/*.pdf')
        pdffile=pdffiles[0]
        with open(pdffile, 'rb') as pdftoread:
            pdfReader = PyPDF2.PdfFileReader(pdftoread) 

            rawText0 = (pdfReader.getPage(0)).extractText()
            rawList0 = rawText0.split(" ")
            rawList0 = list(filter(None, rawList0))
           
            nnn=0
            kkk=0
           
            for i in range(len(rawList0)):
                dummyvar=rawList0[i]
                if validate2(dummyvar[0:8]) and kkk==0:
                    kkk+=1
                    splandate=datetime.strptime(dummyvar[0:8], '%d/%m/%y')
                    plandate=datetime.strftime(splandate, '%Y-%m-%d')
                    
                elif validate(dummyvar) and nnn==0:
                    nnn+=1
                    
                    
                    sdepdate=datetime.strptime(dummyvar, '%d/%m/%Y')
                    keydepdate=datetime.strftime(sdepdate, '%Y-%m-%d')
                    depdate=datetime.strftime(sdepdate, '%d-%m-%y')
                    
                    
                    

                   
            name=str(rawList0[1])+'_'+str(depdate)+'.pdf'
               
            
            rawText1 = (pdfReader.getPage(-1)).extractText()
            rawText2 = (pdfReader.getPage(-2)).extractText()
            rawText = rawText2+rawText1
            rawText= rawText.replace('Page', '')
            
            rawList = rawText.split(" ")
            rawList = list(filter(None, rawList))
            

            

            copyrawList=copy.copy(rawList)
            for cntt in range(len(copyrawList)):
                if copyrawList[cntt]=='-':
                    if copyrawList[cntt+2]=='of':
                        if len(copyrawList[cntt+3])==1:
                            
                           del rawList[cntt:cntt+4]
                            
                        else:
                            
                            del rawList[cntt:cntt+3]
                           

            
                
                              
                              

                
        try:
            
            
                
                
            shutil.move(pdffile, os.path.join(odpath, "Flight OPS","FUEL","DBs","FltPlans","FlightPlans", name))
            shutil.rmtree(dummypath)
        except PermissionError:
            
            time.sleep(5)
            
            shutil.move(pdffile, os.path.join(odpath, "Flight OPS","FUEL","DBs","FltPlans","FlightPlans", name))
            shutil.rmtree(dummypath)
        

        
    
        for cnt0 in range(len(rawList)):
            

            
            if rawList[cnt0]=='20PC':
                PlanID=rawList0[1].lstrip("0")
                KeyTK=PlanID+"_"+keydepdate
                List=[PlanID, keydepdate, plandate, rawList[cnt0+12],rawList[cnt0+13],rawList[cnt0+14],
                      rawList[cnt0+15],rawList[cnt0+16],rawList[cnt0+17],rawList[cnt0+28],rawList[cnt0+29],
                      rawList[cnt0+30],rawList[cnt0+31],rawList[cnt0+32],rawList[cnt0+33].replace('TANKERING', ''),
                      rawList[cnt0+21],rawList[cnt0+22],rawList[cnt0+23],rawList[cnt0+24],
                      rawList[cnt0+25],rawList[cnt0+26], KeyTK ]
                ListProcessed= [ele.lstrip('0') for ele in List]   
                
                try:
                    cur=conn.cursor()
                    cur.execute("SELECT * from dbo.Tankering WHERE KeyTK=?", [KeyTK])
                    ftcur=cur.fetchone()
                    
                    if not ftcur:
                       
                        cur.execute("INSERT INTO dbo.Tankering VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ListProcessed)
                        
                        
                    else:
                        
                        cur.execute("DELETE FROM dbo.Tankering WHERE KeyTK=?", [KeyTK])
                        
                        cur.execute("INSERT INTO dbo.Tankering VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ListProcessed)
                                  
                        
                       
                except Exception as e: 
                    print(e, 'SQLe Yazamadi')
                
                
                tankcnt+=1
            

        succcnt+=1
        conn.commit()
            
    except:
        
        failcnt+=1
        if os.path.isdir(dummypath)==True:
            shutil.rmtree(dummypath)

                 
conn.commit()
conn.close()                   

elapsed_time = time.time() - start_time


txt.write('succ counts=' +str(succcnt))
txt.write('fail counts='+str(failcnt))
txt.write('tank cnt='+str(tankcnt))
txt.write('elapsed time='+str(elapsed_time))
txt.close()






