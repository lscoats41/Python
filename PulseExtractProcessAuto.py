import argparse
import pandas as pd
import os
import sys
import re
import shutil
import collections
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import json as js
from datetime import datetime as dt
import csv
import PulseFunctions as PF
import time as tm




def JsonWrite(file,DictKy): #### write dictionary to a json file

    J=js.dumps(DictKy)
    
    with open(file,"w") as jfile:
    
        jfile.write(J)
        
    jfile.close()
    
def JsonLoad(file): #### import json file into dictionary for usage
    
    with open(file,"r") as jfile:
    
        Dict=js.load(jfile)
        
    jfile.close()
    
    return Dict
    
    
def writecsvfile(flpth,dtRows):#### function to write csv or append to existing csv once data is extracted, transformed from original data

    if(os.path.exists(flpth))==True:

        with open (flpth,"a",encoding='UTF8',newline="") as exportfile:
    
            wrt=csv.writer(exportfile)
    
            wrt.writerows(dtRows)
    else:
    
        with open (flpth,"w",encoding='UTF8',newline="") as exportfile:
    
            wrt=csv.writer(exportfile)
    
            wrt.writerows(dtRows) 
            
            
def writetabfile(flpth,dtRows): #### function to write tab text or append to existing tab text once data is extracted, transformed from original data

    if (os.path.exists(flpth))==True:
    
        with open(flpth,"a",encoding="UTF8",newline="") as exportfile:
        
            wrt=csv.writer(exportfile,delimiter="\t")
            
            wrt.writerows(dtRows)
    else:
    
        with open(flpth,"w",encoding="UTF8",newline="") as exportfile:
        
            wrt=csv.writer(exportfile,delimiter="\t")
            
            wrt.writerows(dtRows)
            
            
def SortTxtfile(pthtxt):#### function to sort existing tab text file by report date used in final process before logging documentation
    
    nwdata=pd.read_csv(pthtxt,sep="\t",engine="python",header=None)
    
    lstcol=len(nwdata.keys())
    
    nwdata[lstcol]=[dt.strptime(dte,"%Y-%m-%dT%H:%M:%S") for dte in nwdata[lstcol-1]]
    
    nwdata=nwdata.sort_values(by=[lstcol],inplace=False)
    
    nwdata.to_csv(pthtxt,sep=("\t"),header=None,index=False)
    
    
def SortCsvfile(pthcsv):#### function to sort existing csv file by report date used in final process before logging documentation

    nwdata=pd.read_csv(pthcsv,header=None)
    
    lstcol=len(nwdata.keys())
    
    nwdata[lstcol]=[dt.strptime(dte,"%Y-%m-%dT%H:%M:%S") for dte in nwdata[lstcol-1]]
    
    nwdata=nwdata.sort_values(by=[lstcol],inplace=False)
    
    nwdata.to_csv(pthcsv,header=None,index=False)

             
        
        
def FindServer(fpth,Svname):### Function created to search for any files in zip file that have the Server of interests

    count=0
    
    if fpth.find(".csv")>-1:
        with open(fpth,"r") as Svfile:
            for line in Svfile:
                if Svname=="WEB":
                    if re.search(Svname,line)!=None or re.search(Svname,line)!=None:
                        count+=1
                else:
                    if re.search(Svname,line,re.IGNORECASE)!=None:
                        count+=1
                        
    elif fpth.find(".xml")>-1:
        tree=ET.parse(fpth)
        rtxml=tree.getroot()
        for server in rtxml.iter():
            if server.text!=None:
                txt=server.text
                if Svname=="WEB":
                    if re.search(Svname,line)!=None or re.search(Svname,line)!=None:
                        count+=1
                else:
                    if re.search(Svname,line,re.IGNORECASE)!=None:
                        count+=1
            else:
                for ky in server.attrib.keys():
                    item=server.attrib[ky]
                    if Svname=="WEB":
                        if re.search(Svname,line)!=None or re.search(Svname,line)!=None:
                            count+=1
                    else:
                        if re.search(Svname,line,re.IGNORECASE)!=None:
                            count+=1                                
    if count>=1:
        return fpth
    else:
        return None



def ExtractWalk(file,fltype,fldr): #### Function to walk through zip file find types of files that are needed and move to a temporary folder

    dirlst=[]
    
    Flnamelst=[]
    
    with ZipFile(file,"r") as zpObj:
        
        flnamelst=zpObj.namelist()
            
        for fl in flnamelst:
                
            if fl.endswith(".zip"):
                
                Idir=file.split(".")[0]
                    
                newfl=os.path.join(Idir,fl)
                    
                dirlst,Flnamelst=ExtractWalk(newfl,fltype)
                
            else:
                if fl.endswith(fltype):
                    zpObj.extract(fl,fldr)
                    flpth=os.path.join(fldr,fl.replace("/","\\"))
                    dirlst.append(flpth)
                    Flnamelst.append(fl)
                    
    return dirlst,Flnamelst                
    
    
    
    

def GetShipandDate(zpth,jspth): #### Function to pull ship id number using the zip file path structure and Dictionary. In addition, create report date log for zip file path structure.

    zpthsplit=zpth.split("\\")
    
    shipID=zpthsplit[-1].split("_")[0]

    ShipDict=JsonLoad(jspth)

    shpID=ShipDict[shipID]
    
    DateString=zpthsplit[-1].split("_")[1]+" 00:00"
    
    shipDate=dt.strptime(DateString,"%Y-%m-%d %H:%M")
    
    return shpID, shipDate
    

        
def FolderWalk(fldr): ### Create a list of zip files that meet software conditions
    
    ziplst=[]
    for root,_,files in os.walk(fldr,topdown=True):
        for f in files:
            if f.find("SW1.zip")!=-1 or f.find("SW2.zip")!=-1:
                ziplst.append(os.path.join(fldr,f))
    
    return ziplst
    
    
def NoProcessLog(fldr): ### Create a Log documentation when no zip files are in folder at time of automation

    NoProcFldr=fldr+"_NOPROC"

    if os.path.exists(NoProcFldr)==False:
        os.makedirs(NoProcFldr)
    else:
        pass
        
    DateTimeProc=dt.now().strftime("%Y-%m-%d %H:%M")
        
    noProcTxt=os.path.join(NoProcFldr,"NoneProcessed.txt")
    
    with open(noProcTxt,"w") as NFile:
    
        NFile.write("No zip files were processed during the time period of {}\n \n".format(DateTimeProc))
        
    NFile.close()
    
    
    
def FileProcessLog(fldr,lstzp):### Create a Log documentation when zip files are processed at time of automation

   
    FileProcTxt=os.path.join(fldr,"FilesProcessed.txt")
        
    DateTimeProc=dt.now().strftime("%Y-%m-%d %H:%M")
        
    with open(FileProcTxt,"w") as ProcFile:
        
        for zp in lstzp:
            
            zpfile=zp.split("\\")[-1]
    
            ProcFile.write("This file {} was processed during the time period of {}\n \n".format(zpfile,DateTimeProc))
        
    ProcFile.close()
    
def ErrorLog(afldr,err): ### Create a Log documentation when there is an error that occurs in script during process of automation

    ErrorLogTxt=os.path.join(afldr,"LogError.txt")
    
    DateTimeProc=dt.now().strftime("%Y-%m-%d %H:%M")
    
    with open(ErrorLogTxt,"w") as efile:
        
        efile.write("This error occurred on {} while processing files:\r\n{}".format(DateTimeProc,err))
    
    efile.close()
    

    
def CreateFuncDic(): ##### Function to create a dictionary of imported function utilized to process specific files and to insert missing data if not available for a report date

    ProcFileDict={"DCReplication":PF.DCReplication,"ECRReport":PF.ECRReport,"ServerCertStatus":PF.ServerCertStatus,"WSUSServerStatus":PF.WSUSServerStatus,"DiskSpace":PF.DiskSpace,"FilesystemBackup":PF.FileSysBackUp}

    MissingFileDict={"DCReplication":PF.DCRepMissing,"ECRReport":PF.ECRMissing,"ServerCertStatus":PF.ServerCertMissing,"WSUSServerStatus":PF.WSUSMissing,"DiskSpace":PF.DiskSpMissing,"FilesystemBackup":PF.FileBkUpMissing}
    
    return ProcFileDict,MissingFileDict
    
    
def CreateImportFolder(fldr,svrSel): #### Function to Create Folder for location to store final files as output

    afldr=fldr+"_"+svrSel

    if os.path.exists(afldr)==False:
        os.makedirs(afldr)
    else:
        pass
    
    return afldr
    
    
def FindFileTypes(zlst,ftypes,jsonpth,ARfldr): #### Function incorporates ExtractWalk function to move files of interests to temp folder and store into a dictionary based on shipid and report date

    DictFiles={}
    
    for z in zlst:
        ID,datelg=GetShipandDate(z,jsonpth)
        tempfldr=os.path.join(ARfldr,"temp"+str(ID)+"_"+datelg.strftime("%Y-%m-%d")) 
        if str(ID) not in DictFiles.keys():
            DictFiles[str(ID)]={}
            DictFiles[str(ID)][datelg.strftime("%Y-%m-%d %H:%M")]={}
        else:
            DictFiles[str(ID)][datelg.strftime("%Y-%m-%d %H:%M")]={}
        for ft in ftypes:
            DictFiles[str(ID)][datelg.strftime("%Y-%m-%d %H:%M")][ft]=ExtractWalk(z,ft,tempfldr)[0]
    
    
    print(DictFiles[str(ID)].keys())
        
    return DictFiles
    
    
def FindGoodFiles(svr,fldict): #### Function to utilize findserver function to locate the path directory of files that have server of interests in a new dictionary

    PthDict={}
    
    for shpky in fldict.keys():
        PthDict[shpky]={}
        print(fldict[shpky].keys())
        for dtky in fldict[shpky].keys():
            ###print(dtky)
            for ty in fldict[shpky][dtky].keys():
                lstpths=fldict[shpky][dtky][ty]
                ###print(lstpths)
                for pth in lstpths:
                    ans=FindServer(pth,svr)
                    if ans!=None:
                        if dtky not in PthDict[shpky].keys():
                            PthDict[shpky][dtky]=[]
                            PthDict[shpky][dtky].append(ans)
                        else:
                            PthDict[shpky][dtky].append(ans)
                    else:
                        pass
            
    ###print(PthDict)
            
    return PthDict


def ProcessFiles(Pthdict,djson,svr,intfiles,afldr,pfun,dskjson): #### Function that iterates through Path dictionary to call Function Dictionary to process file for table results

    ProcFilesDict={}
    
    datelst=[]
    for shpky in Pthdict.keys():
        for kyd in Pthdict[shpky].keys():
            dy=dt.strptime(kyd,"%Y-%m-%d %H:%M")
            if dy not in datelst:
                datelst.append(dy)
    
    datelst.sort()
    
    print(datelst)
        
    
    for shp in Pthdict.keys():
        ProcFilesDict[shp]={}
        shpNum=int(shp)
        for datelg in datelst:
            kydate=datelg.strftime("%Y-%m-%d %H:%M")
            if kydate in Pthdict[shp].keys():
                ProcFilesDict[shp][kydate]=[]
                csvlst=Pthdict[shp][kydate]
                for cpth in csvlst:
                    namelst=cpth.split("\\")[-1].split(".")[0].split("_")
                    if len(namelst) < 2:
                        name=namelst[0]
                        if name in intfiles:
                            if name=="DiskSpace":
                                ProcFilesDict[shp][kydate].append(name)
                                ArchDsk=JsonLoad(dskjson)
                                rows,ArchDsk=pfun[name](cpth,svr,shpNum,datelg,ArchDsk)
                                filepth=os.path.join(afldr,name+".txt")
                                writetabfile(filepth,rows)
                                JsonWrite(dskjson,ArchDsk)
                            else:
                                ProcFilesDict[shp][kydate].append(name)
                                rows=pfun[name](cpth,svr,shpNum,datelg)
                                filepth=os.path.join(afldr,name+".txt")
                                writetabfile(filepth,rows)
                                
                    else:
                        name=namelst[0]+namelst[1]
                        if name in intfiles:
                            ProcFilesDict[shp][kydate].append(name)
                            ArchDt=JsonLoad(djson)
                            rows,ArchDt=pfun[name](cpth,svr,shpNum,datelg,ArchDt)
                            filepth=os.path.join(afldr,name+".txt")
                            writetabfile(filepth,rows)
                            JsonWrite(djson,ArchDt)
    print(ProcFilesDict)
                        
    return ProcFilesDict
    
    

def UpdateDateArch(jsonfile,adict):  

    JsonWrite(jsonfile,adict)
      
            

def CreateMissFile(svr,afldr,msfunc,fls,flsProc): #### Function utilizes function dictionary to call functions where the server of interest have missing data on that particular report date
    
    print(flsProc)
    for shpky in flsProc.keys():
        shpNum=int(shpky)
        for dtky in flsProc[shpky].keys():
            datelg=dt.strptime(dtky,"%Y-%m-%d %H:%M")
            lstProc=flsProc[shpky][dtky]
            for fname in fls:
                if fname not in lstProc:
                    row=msfunc[fname](svr,shpNum,datelg)
                    fpth=os.path.join(afldr,fname+".txt")
                    writetabfile(fpth,row)
                    
def TbleSortClrTmpFldr(FinalFldr): ###### Function to process before the end where the temporary folders are deleted and existing tables are sorted by report date before ingestion into Aras
                    
    for root,dirs,files in os.walk(FinalFldr,topdown=True):
        for d in dirs:
            dirpth=os.path.join(FinalFldr,d)
            shutil.rmtree(dirpth)
        for f in files:
            if f.find(".txt")!=-1 or f.find(".csv")!=-1:
                fpth=os.path.join(FinalFldr,f)
                if fpth.endswith(".txt"):
                    SortTxtfile(fpth)
                else:
                    SortCsvfile(fpth)
        
        

def main(): #### Main Area where are inputed arguments utilizing areparser for execution of function written

    parser=argparse.ArgumentParser() 
    parser.add_argument('--ZipFiles',action="store",type=str,help="Folder for ZipFiles",default="C:\\Projects\\DigitalTwin\\Tacnet\\PulseZip")
    parser.add_argument('--Files',nargs="+",action="store",type=str,help="Files To Process", default=["DCReplication","ECRReport","ServerCertStatus","WSUSServerStatus","DiskSpace","FilesystemBackup"])
    parser.add_argument('--Server',action="store",type=str,help="Selected Server",default="DC01")
    parser.add_argument('--FileTypes',nargs="+",action="store",type=str,help="Type of files to scan",default=[".csv"])
    parser.add_argument('--ImportFolder',action="store",type=str,help="Folder for import to Aras",default="C:\\Projects\\DigitalTwin\\Tacnet\\Data\\ArasImport")
    parser.add_argument('--SHIPID',action="store",type=str,help="Json File for Ship IDs",default="C:\\Projects\\DigitalTwin\\Tacnet\\Data\\ShipID.json")
    parser.add_argument('--DateBackUp',action="store",type=str,help="Date Backup Archive in Json",default="C:\\Projects\\DigitalTwin\\Tacnet\\Data\\Shipbackupdate.json")
    parser.add_argument('--DiskArchive',action="store",type=str,help="Lastest Free Disk Space Archive in Json",default="C:\\Projects\\DigitalTwin\\Tacnet\\Data\\ShipLastDiskSpace.json")
    args=parser.parse_args()
    argdict=vars(args)
    ZpFolder=argdict["ZipFiles"]
    FldrImport=argdict["ImportFolder"]
    FilesIntr=argdict["Files"]
    SelectServer=argdict["Server"]
    Fltype=argdict["FileTypes"]
    ShpIDjson=argdict["SHIPID"]
    DateArchJson=argdict["DateBackUp"]
    DiskArchJson=argdict["DiskArchive"]
    
    startTime=tm.time()
    ZpList=FolderWalk(ZpFolder)  
    if len(ZpList)<int(1): #### This is what occurs if no files are in folder
        NoProcessLog(FldrImport)
        endTime=tm.time()
        sys.exit("No Zip Files in Folder")
    else: ### This is what occurs if zip files are in folder and if the process is successful or fail due to error
        try:
            ProcFunc,MissFunc=CreateFuncDic()
            ArasFldr=CreateImportFolder(FldrImport,SelectServer)
            FilesDict=FindFileTypes(ZpList,Fltype,ShpIDjson,ArasFldr)
            GoodPthDict=FindGoodFiles(SelectServer,FilesDict)
            FilesProc=ProcessFiles(GoodPthDict,DateArchJson,SelectServer,FilesIntr,ArasFldr,ProcFunc,DiskArchJson)
            ###UpdateDateArch(DateArchJson,DateArch)
            CreateMissFile(SelectServer,ArasFldr,MissFunc,FilesIntr,FilesProc)
            TbleSortClrTmpFldr(ArasFldr)
            FileProcessLog(ArasFldr,ZpList)
            endTime=tm.time()
        except Exception as e:
            ErrorLog(ArasFldr,e)
            endTime=tm.time()
            
    executeTime=round((endTime-startTime)/60,3)
    
    print("This script took {} minutes to complete processing of file for ingestion into Aras".format(executeTime))
        

main()
        
   
