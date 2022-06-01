import pandas as pd
import os
import re
import collections
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import json as js
from datetime import datetime as dt
from datetime import timedelta as td
import csv

def readcsvfile(flcsv):
    
    linelst=[]
    
    with open(flcsv,"r") as fcsv:
        count=0
        for lines in fcsv:
            count+=1
            if count>1:
                line=lines.strip().split(",")
                linelst.append(line)
    
    return linelst
    
        
def DCReplication(csvpth,svr,IDship,lgdate):
    drows=[]
    lines=readcsvfile(csvpth)
    for line in lines:
            difline=len(line)-7
            line[0]=",".join(line[:difline+1])
            del line[1:difline+1]
            if re.search(svr,line[1],re.IGNORECASE)!=None or re.search(svr,line[2],re.IGNORECASE)!=None:
                Rpdate=dt.strptime(line[5].replace('"',''),"%Y-%m-%d %H:%M:%S")
                numdays=(Rpdate-lgdate).days            
                if numdays>=3:
                    status="Red"
                elif numdays>1 and numdays <3:
                    status="Yellow"
                else:
                    status="Green"
                    drows.append([line[1].replace('"',''),line[2].replace('"',''),status,numdays,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")]) 
    return drows
    
def ECRReport(csvpth,svr,IDship,lgdate):
    erows=[]
    lines=readcsvfile(csvpth)
    for line in lines:
        if re.search(svr,line[1],re.IGNORECASE)!=None:
            erows.append([line[0].replace('"','').upper(),line[1].replace('"',''),int(line[2].replace('"','')),IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
            
    return erows
    
def ServerCertStatus(csvpth,svr,IDship,lgdate):
    crtrows=[]
    lines=readcsvfile(csvpth)
    for line in lines:
        if re.search(svr,line[0],re.IGNORECASE)!=None:
            validdate=dt.strptime(line[-4].replace('"',''),"%m/%d/%Y %I:%M:%S %p")
            vdays=(validdate-lgdate).days
            if vdays>180:
                status="Green"
            elif vdays>90 and vdays<=180:
                status="Yellow"
            else:
                status="Red"
            issue=",".join(line[1:6])
            crtrows.append([IDship,lgdate.strftime("%m/%d/%YT%H:%M:%S"),line[0].replace('"',''),issue.replace('"',''),line[6].replace('"',''),line[7].replace('"',''),line[8].replace('"',''),line[9].replace('"',''),line[10].replace('"',''),status])
    return crtrows
    
def WSUSServerStatus(csvpth,svr,IDship,lgdate):
    wsrows=[]
    wsudata=pd.read_csv(csvpth,header=0)
    for idx,cl in enumerate(wsudata["Client"]):
        if type(cl)==str:
            svname=cl.split(".")[0]
            if svname.upper()==svr:
                time=dt.strptime(wsudata["LastSync"][idx],"%m/%d/%Y %I:%M:%S %p")
                failnum=int(wsudata["Failed"][idx])
                update=int(wsudata["Updates"][idx])
                ntitles=wsudata["NeededUpdateTitles"][idx]
                if type(ntitles)==float:
                    ntitles="NULL"
                else:
                    nlst=ntitles.split()
                    ntitles=" ".join(nlst)
                ftitles=wsudata["FailedUpdateTitles"][idx]
                if type(ftitles)==float:
                    ftitles="NULL"
                else:
                    flst=ftitles.split()
                    ftitles=" ".join(flst)
                if failnum>15:
                    status="Red"
                elif failnum<=15 and failnum>5:
                    status="Yellow"
                else:
                    status="Green"
                wsrows.append(["Server",svname.upper(),update,failnum,time.strftime("%b %d %Y %I:%M%p"),ntitles.replace('"',''),ftitles.replace('"',''),status,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])      
                    
    return wsrows
    
    
def DiskSpace(csvpth,svr,IDship,lgdate,dskarch):
    dskrows=[]
    lines=readcsvfile(csvpth)
    for line in lines:
        if re.search(svr,line[0],re.IGNORECASE)!=None:
            if str(IDship) not in dskarch[svr].keys():
                dskarch[svr][str(IDship)]={}
                dskarch[svr][str(IDship)][line[1].replace('"','')]=int(line[4].replace('"',''))
                chngspace=0
                if int(line[3].replace('"',''))>0:
                    freePer=int(line[4].replace('"',''))/int(line[3].replace('"',''))
                    used=int(line[3].replace('"',''))-int(line[4].replace('"',''))
                    if freePer>.20:
                        status="Green"
                    elif freePer<=.20 and freePer >.10:
                        status="Yellow"
                    else:
                        status="Red"
                else:
                    freePer=1
                    used=0
                    status="Green"
            else:
                if line[1].replace('"','') not in dskarch[svr][str(IDship)].keys():
                    lastspace=int(line[4].replace('"','')) 
                else:
                    lastspace=int(dskarch[svr][str(IDship)][line[1].replace('"','')])
                chngspace=lastspace-int(line[4].replace('"',''))
                dskarch[svr][str(IDship)][line[1].replace('"','')]=int(line[4].replace('"',''))
                if int(line[3].replace('"',''))>0:
                    freePer=int(line[4].replace('"',''))/int(line[3].replace('"',''))
                    used=int(line[3].replace('"',''))-int(line[4].replace('"',''))
                    if freePer>.20:
                        status="Green"
                    elif freePer<=.20 and freePer >.10:
                        status="Yellow"
                    else:
                        status="Red"
                else:
                    freePer=1
                    used=0
                    status="Green"
                    
            dskrows.append([line[0].replace('"',''),line[1].replace('"',''),line[2].replace('"',''),int(line[3].replace('"','')),int(line[4].replace('"','')),used,status,round(freePer,3),IDship,chngspace,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
                
    return dskrows,dskarch
    
def FileSysBackUp(csvpth,svr,IDship,lgdate,dtArch):
    svrows=[]
    lines=readcsvfile(csvpth)
    for line in lines:
        if re.search(svr,line[0],re.IGNORECASE)!=None:
            if svr not in dtArch.keys():
                dtArch[svr]={}
                if str(IDship) not in dtArch[svr].keys():
                    dtArch[svr][str(IDship)]={}
                    dtArch[svr][str(IDship)][line[1]]={}
                else:
                    dtArch[svr][str(IDship)][line[1]]={}
                if line[-2]=="succeeded":
                    datesv=dt.strptime(line[3],"%m/%d/%y %I:%M:%S %p")
                    dtArch[svr][str(IDship)][line[1]]=datesv.strftime("%m/%d/%Y %H:%M:%S")
                    hstatus="Green"
                else:
                    dtArch[svr][str(IDship)][line[1]]=lgdate-td(days=6)
                    datearchive=lgdate-td(days=6)
                    datesv=dt.strptime(line[3],"%m/%d/%y %I:%M:%S %p")
                    svdays=(datesv-datearchive).days
                    if svdays >15:
                        hstatus="Critical"
                    elif svdays<=15 and svdays>9:
                        hstatus="Red"
                    elif svdays<=9 and svdays>5:
                        hstatus="Yellow"
                    else:
                        hstatus="Green"               
            else:   
                if line[-2]=="succeeded":
                    datesv=dt.strptime(line[3],"%m/%d/%y %I:%M:%S %p")
                    dtArch[svr][str(IDship)][line[1]]=datesv.strftime("%m/%d/%Y %H:%M:%S")
                    hstatus="Green"
                else:
                    datearchive=dt.strptime(dtArch[svr][str(IDship)][line[1]],"%m/%d/%Y %H:%M:%S")
                    datesv=dt.strptime(line[3],"%m/%d/%y %I:%M:%S %p")
                    svdays=(datesv-datearchive).days
                    if svdays >15:
                        hstatus="Critical"
                    elif svdays<=15 and svdays>9:
                        hstatus="Red"
                    elif svdays<=9 and svdays>5:
                        hstatus="Yellow"
                    else:
                        hstatus="Green"
            svrows.append([svr,line[0],line[1],line[3],line[-2],hstatus,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
    
    return svrows,dtArch
    
def DCRepMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append([svr,"Missing","Missing",9999,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")]) 
    
    return msrows


def ECRMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append(["Missing","Missing",9999,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
    
    return msrows
    
def ServerCertMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append([IDship,lgdate.strftime("%m/%d/%YT%H:%M:%S"),"Missing","Missing","Missing","Missing","Missing","Missing","Missing","Missing"])
    
    return msrows
    
def WSUSMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append(["Server",svr,9999,9999,"Missing","Missing","Missing","Missing",IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
    
    return msrows
    
def DiskSpMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append(["Missing","Missing","Missing",9999,9999,9999,"Missing",9999,IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
    
    return msrows
    
def FileBkUpMissing(svr,IDship,lgdate):

    msrows=[]
    
    msrows.append([svr,"Missing","Missing","Missing","Missing","Missing",IDship,lgdate.strftime("%Y-%m-%dT%H:%M:%S")])
    
    return msrows 
    
    


    

    

                
