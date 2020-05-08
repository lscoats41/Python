import pandas as pd
import argparse
import datetime as dt
import os
import arcpy
import sys
import ftplib
import gzip
import csv
import pyodbc
from arcpy import env
from time import time
import zipfile
from azure.storage.blob import BlockBlobService
from shutil import rmtree

env.overwriteOutput = True


def YearDateRng(BVar,DVar): ####### This Function creates the dates range back from date selected in a list and Several Years into a list
	
	if bool(BVar)==False:
		Date=dt.date.today()-dt.timedelta(days=2)
		LtDate=Date.strftime('%Y-%m-%d')
	else:
		LtDate=BVar

	Yr=int(LtDate[0:4])
	Mo=int(LtDate[5:7])
	Dy=int(LtDate[8:10])
	
	refDay=dt.datetime(Yr,Mo,Dy)

	Adv=int(DVar)

	Dates=[]
	
	for i in range(0,Adv):
		Past=dt.timedelta(days=i)
		Day=refDay-Past
		Date=Day.strftime('%Y-%m-%d')
		Dates.append(Date)
		Dates.sort()

	print (Dates)

	Years=[]

	for day in Dates:
		Year=day[0:4]
		if Year not in Years:
			Years.append(Year)

	print (Years)
	return Dates,Years
	
def FTPDownLoad(Ylst,Fldr): ##### Function immediately download and import GHCN raw data from the website ftp.ncdc.noaa.gov
	
	Today=dt.date.today()
	LDay=Today.strftime('%Y-%m-%d')
	GFolder=Fldr
	GFolderSpc=os.path.join(GFolder,LDay)
	
	if not os.path.exists(GFolderSpc):
		os.makedirs(GFolderSpc)
	   
	CSVList=[]
	for yr in Ylst:
		site="ftp.ncdc.noaa.gov"
		ftp=ftplib.FTP(site)       ### access site
		ftp.login("anonymous","anything") #### Login with username and password
		ftp.dir()
		next_dir="pub/data/ghcn/daily/by_year" 
		ftp.cwd(next_dir)   ##### move to this directory
		Year=str(yr)
		ftp.dir
		ChosenFile=Year+".csv.gz"
		ChosenFileOpen=open(ChosenFile,'wb')
		ftp.retrbinary("RETR " + ChosenFile,ChosenFileOpen.write) ### retrive gzip file from website and download
		ChosenFileOpen.close() #### Terminate download process of gzip file
		ftp.close() ##### Terminate access to ftp website
		CsvFile="GHCN_"+Year+".csv"
		myfile=os.path.join(GFolderSpc,CsvFile)
		CSV=open(myfile,'wb')
		with gzip.open(ChosenFile,'rb') as Zipfile: ##### open gzip file and write to csv
			for line in Zipfile:
				CSV.write(line)
		Zipfile.close()
		CSV.close()
		CSVList.append(myfile)
	print(CSVList)
	return CSVList,GFolderSpc
	
	
def TableMerge(CSVS,KEY):##### Initial Function to Merge GHCN Daily Wx Variables with GHCN Station Key with geographic coordinates to Create Inital Large Table

	GHCN_Station=pd.read_csv(KEY,names=["GHCNSTATION","LATLINES","LONLINES","ELEVATION","NAMESTATION","ID","NUMBER"])

	GHCNLst=[]
	
	
	for csv in CSVS:

		GHCN_WXVAR=pd.read_csv(csv,names=["GHCNSTATION","InitialDate","WxVar","WxReading","TraceWx","Var1","Var2","Var3"])

		GHCN_WXMERGE=pd.merge(GHCN_Station,GHCN_WXVAR,left_on="GHCNSTATION",right_on="GHCNSTATION",how="inner")
		
		GHCNLst.append(GHCN_WXMERGE)
	
	

	return GHCNLst
	
def SNOWDATA(GHCNLst):#### Function to Modify Observation date to desired format (%Y-%m-%d) and convert Snowfall from mm to inches, create table with just SnowFall data

	
	SnowLst=[]
	for Glst in GHCNLst:
		SNOWWX=Glst[(Glst["WxVar"]=="SNOW")&(Glst["WxReading"]>=float(0))&(type(Glst["WxReading"])!="str")]
		Count=len(SNOWWX["WxVar"])
		SNOWLST=[i for i in range(0,Count)]
		SNOWWX.index=SNOWLST
		SNOWWX["NEWDATE"]=pd.to_datetime(SNOWWX.loc[:,"InitialDate"],format="%Y%m%d")
		SNOWWX["OBSDATE"]=SNOWWX.loc[:,"NEWDATE"].dt.strftime("%Y-%m-%d")
		SNOWWX["SNOWFALL"]=SNOWWX.loc[:,"WxReading"]*float(.0394)	
		SNOWWXGIS=SNOWWX[["GHCNSTATION","LATLINES","LONLINES","OBSDATE","WxReading","SNOWFALL"]]
		SnowLst.append(SNOWWXGIS)
	
	
	List=[Snow for Snow in SnowLst] ##### Create one large GHCN SNOW TABLE to extract data from in next function based on dates
	SNOWTable=pd.concat(List)
	print("Table Printed")
	
	
	return SNOWTable
	
def SnowCsvOut(GSNOWTable,Dates,Fldr):###### Function to write selected data based on index of Unique Values in List to csv
	
	if not os.path.exists(Fldr):
		os.makedirs(Fldr)
	
	ULst=Dates
	SnowCsvList=[]
	for U in ULst:
		print(U)
		Name="SNOW"
		NamePath=Fldr+"\\"+Name+str(U)+".csv"
		SnowCsvList.append(NamePath)	
		GisTableDay=GSNOWTable[GSNOWTable["OBSDATE"]==U]
		print(GisTableDay)
		GisTableDay.to_csv(NamePath,sep=",",header=True,index=True)
	
	return SnowCsvList
			
	
	
def SnowKriging(SClist,LstDt,Fld,spr): ##### Process to execute Kringing into Rasters for extraction by points in next function. A list of Rasters is returned
	X_pts="LONLINES"
	Y_pts="LATLINES"
	ValueField="SNOWFALL"
	arcpy.env.workspace=Fld
	env.overwriteOutput = True
	
	RasterLst=[]
	for i in range(0,len(LstDt)):
		Yr=str(LstDt[i][0:4])
		Mo=str(LstDt[i][5:7])
		Dy=str(LstDt[i][8:10])
		
		GHCNName="GHCNData"+Yr+".gdb"
		GHCNgdb=os.path.join(Fld,GHCNName)
		Ingdb="SNOW"+Yr+Mo+Dy
		IngdbPath=os.path.join(GHCNgdb,Ingdb)
		print(Ingdb)
		print(IngdbPath)
		OutLyr="SNOW"+LstDt[i]
		SaveLyr="SNOW"+LstDt[i]+".lyr"
		filecsv=SClist[i]
		print(SClist[i])
		if not arcpy.Exists(GHCNgdb):
			arcpy.CreateFileGDB_management(Fld,GHCNName)
		GHCNpts=arcpy.MakeXYEventLayer_management(filecsv,X_pts,Y_pts,OutLyr,spr)
		GHCNSNOWPts=arcpy.SaveToLayerFile_management(GHCNpts, SaveLyr)
		SnowPtsGdb=arcpy.CopyFeatures_management(GHCNSNOWPts,IngdbPath)
		print(SnowPtsGdb)
		arcpy.CheckOutExtension("Spatial")
		print ("Geo Extension")
		RName=ValueField+LstDt[i]+".img"
		RPath=os.path.join(Fld,RName)
		NoName="MIN_FLOAT.img"
		NoRaster=os.path.join(Fld,NoName)
		hasValues=False
		with arcpy.da.SearchCursor(SnowPtsGdb,[ValueField]) as cursor:
			LstValue=[]
			for row in cursor:
				if float(row[0]) not in LstValue:
					LstValue.append(row[0])
				if len(LstValue) > 1:
					hasValues=True
			del row
		del cursor
		print(LstValue)
		if not arcpy.Exists(NoRaster):
			arcpy.CreateRasterDataset_management(Fld,NoName,"","16_BIT_UNSIGNED",spr)
		if hasValues==True:
			print ("Attempt Kriging")
			try:
				arcpy.gp.Kriging_sa(SnowPtsGdb,ValueField,RPath,"Spherical 0.099048", "0.099048", "VARIABLE 12", "")
				RasterLst.append(RPath)
			except:
				arcpy.CopyRaster_management(NoRaster,RPath)
				RasterLst.append(RPath)
				print ("Something is Wrong")
		else:
			arcpy.CopyRaster_management(NoRaster,RPath)
			RasterLst.append(RPath)
			print ("All Values Were 0")
		
		print("Kriging Completed")
	return RasterLst
		
def ExtractVar(DtLst,RLst,Key,Spr,Fldr): ##### Extraction of Snow Values from Rasters to UHDB Station Points and Cleaned for negative values or null values

    if not os.path.exists(Fldr):
       os.makedirs(Fldr)
		
    X_pts="LONGITUDE"
    Y_pts="LATITUDE"
    ValueField="SNOWFALL"
    Workspc=Fldr
    arcpy.env.workspace=Workspc
    env.overwriteOutput = True
    for i in range(0,len(DtLst)):
        Yr=str(DtLst[i][0:4])
        Mo=str(DtLst[i][5:7])
        Dy=str(DtLst[i][8:10])
        Date=str(DtLst[i])
        UHDB_Name="UHDBSNOW"+Yr
        UHDBgdb=os.path.join(Workspc,UHDB_Name+".gdb")
        if not arcpy.Exists(UHDBgdb):
            arcpy.CreateFileGDB_management(Workspc,UHDB_Name)
        UHDBFc="SNOW"+Yr+Mo+Dy
        UHDBShp=os.path.join(UHDBgdb,UHDBFc)
        NewLyr="SNOW"+str(DtLst[i])
        SvLyr=NewLyr+".lyr"
        StationPts=arcpy.MakeXYEventLayer_management(Key,X_pts,Y_pts,NewLyr,Spr)
        UFDBPts=arcpy.SaveToLayerFile_management(StationPts, SvLyr)
        arcpy.CopyFeatures_management(UFDBPts,UHDBShp)
        print("Start Kriging")
        arcpy.sa.ExtractMultiValuesToPoints(UHDBShp,RLst[i],"NONE")
        DateField="ObsDate"
        OldField=ValueField+Yr+"_"+Mo+"_"+Dy
        print(OldField)
        arcpy.AddField_management(UHDBShp,ValueField,"FLOAT", "6", "3", "", "", "", "", "")
        arcpy.AddField_management(UHDBShp,DateField,"TEXT", "", "", "10", "", "", "", "")
        NwFields=[OldField,ValueField,DateField]
        DFields=[OldField,"NAME","COUNTRY","LATITUDE","LONGITUDE","ELEVATION","STATE"]
        print (NwFields)
        with arcpy.da.UpdateCursor(UHDBShp,NwFields) as ModCursor:
            for row in ModCursor:
               row[2]=Date
               if row[0] < int(0):
                  row[1]=None
                  row[2]=Date
                  ModCursor.updateRow(row)
               else:
                  row[1]=row[0]
                  row[2]=Date
                  ModCursor.updateRow(row)
        del ModCursor
        arcpy.DeleteField_management(UHDBShp,DFields)

        return UHDBgdb

def SqlInsert(Wkspc,Site,User,PWrd,DBName,DLst): ##### process used to insert new snowvaluse to Sql GHCN_Table base on dates
        
        driver='{ODBC Driver 17 for SQL Server}' #### Call for ODBC Drive
        conn=pyodbc.connect('DRIVER='+driver+';SERVER='+Site+';PORT=1433;DATABASE='+DBName+';UID='+User+';PWD='+PWrd) #### Username, password, database name, and port for connections to Sql Database
        crsor=conn.cursor()
        DClause="DELETE FROM Interpolated.GHCN_12_18 WHERE OBSERVATION_DATE_TIME=?" ### Sql Statements to delete data
        IClause="INSERT INTO Interpolated.GHCN_12_18(STATION_CODE,GHCN_SNOWFALL,OBSERVATION_DATE_TIME)VALUES(?,?,?)" ### Sql statements to insert data
        DateLst=DLst
        Fields=["STATION_CODE","SNOWFALL","ObsDate"]

        for Day in DateLst:
                UHDB_Name="UHDBSNOW"+str(Day[0:4])+".gdb"
                DBspc=os.path.join(Wkspc,UHDB_Name)
                ShpDate="SNOW"+str(Day[0:4])+str(Day[5:7])+str(Day[8:10])
                ShpFlDay=os.path.join(DBspc,ShpDate)

                crsor.execute(DClause,Day) ##### Execute Sql Statement with Variable for required condition
                conn.commit()
                print ("Finally Deleted" ,Day, "From GHCN Database")
                
                InsertLst=[]
                with arcpy.da.SearchCursor(ShpFlDay,Fields) as ShpCursor:
                        for row in ShpCursor:
                                InsertLst.append(row)
                                NumLst=len(InsertLst)
                                if NumLst==int(1000):#### Insert every 1000 rows
                                        crsor.executemany(IClause,InsertLst) ##### Execute Sql Statement to insert batches of rows with Variable for required condition
                                        conn.commit()
                                        print("Batch Inserted for" ,Day)
                                        InsertLst=[]
                del ShpCursor

                FinalNumLst=len(InsertLst)

                if FinalNumLst > int(0):
                        crsor.executemany(IClause,InsertLst)##### Execute Sql Statement to Insert last rows with Variable for required condition 
                        conn.commit()
                        InsertLst=[]
                        print ("Last Batch Inserted for", Day)
                else:
                        print ("Insert Already Completed for", Day)

def sendtoblobstorage(LocFile,Name,Key,CtName,FName): #### Function used below to send raw GHCN file to azzure storage
        block_blob_service = BlockBlobService(account_name=Name,account_key=Key)
        block_blob_service.create_blob_from_path(CtName,FName,LocFile)



def zipfoldertostorage(FTPfolder,Fldr,ZipSpc,AName,AKey,Container): ###### Zip Raw GHCN files then send to storage
        files=os.listdir(FTPfolder)
        TimeProc=str(int(time()))
        Day=dt.date.today()
        StringDay=Day.strftime('%Y-%m-%d')
        FTPZipDir=os.path.join(Fldr,ZipSpc)
        if not os.path.exists(FTPZipDir):
                os.makedirs(FTPZipDir)
        ZipName="Compressed"+TimeProc+".zip"
        ZipPathName=os.path.join(FTPZipDir,ZipName)
        FileName=StringDay+"-"+TimeProc+".zip"
        with zipfile.ZipFile(ZipPathName,'w',zipfile.ZIP_DEFLATED) as zfile: 
                for gfile in files:
                        Createdfile=os.path.join(FTPfolder,gfile)
                        if os.path.isfile(Createdfile):
                                zfile.write(Createdfile,gfile)
        sendtoblobstorage(ZipPathName,AName,AKey,Container,FileName)

        return FTPZipDir,ZipPathName
                
                
def clearworkspc(ZDir,ZPth,Gspc,TFolder,MainFldr,Yrs,Days): ###### Delete unecessary files and folders
        os.remove(ZPth)
        rmtree(ZDir)
        rmtree(Gspc)
        rmtree(TFolder)

        for yr in Yrs:
                GDBName="UHDBSNOW"+str(yr)+".gdb"
                GDBFolder=os.path.join(MainFldr,GDBName)
                rmtree(GDBFolder)

        for day in Days:
                StoredLyr="Snow"+str(day)+".lyr"
                LyrPath=os.path.join(MainFldr,StoredLyr)
                os.remove(LyrPath)
                
                
        


		
def main():
        parser=argparse.ArgumentParser()
        parser.add_argument('--LatestDate',action="store",type=str,help="Date YYYY-MM-DD",default="")
        parser.add_argument('--DaysBack',action="store",type=int,help="Number of Day in the Past",default="5")
        parser.add_argument('--GHCNFolder',action="store",type=str,help="Store GHCN Files",default="C:\\FTP\\Data")
        parser.add_argument('--TempFolder',action="store",type=str,help="Tempory Storage Folder",default="C:\\ProcessDataFiles\\GHCN_INGEST\\TEMP")
        parser.add_argument('--MainFolder',action="store",type=str,help="MAIN Folder",default="C:\\ProcessDataFiles\\GHCN_INGEST\\Data")
        parser.add_argument('--SqlSite',action="store",type=str,help="Sql Server Site",default="ucc-archv-d3-sql.database.windows.net")
        parser.add_argument('--UserName',action="store",type=str,help="User Name to Sql",default="DynamicDataUser")
        parser.add_argument('--Password',action="store",type=str,help="Password to Server",default="DynD@t@U$3r2017")
        parser.add_argument('--Database',action="store",type=str,help="Database Name",default="ucc-archv-d3")
        parser.add_argument('--AccountName',action="store",type=str,help="Blob Storage Account Name",default="historicaldbingestfiles")
        parser.add_argument('--AccountKey',action="store",type=str,help="Blob Storage Key",default="wtUpAH3KwyPZK5XZf/pbN+t81VnR7y8ORt7YhMA4hpJUW8a/zK9H0nLo1XPyRomUN5zY3HcocFGJUWaoQH0Tgw==")
        parser.add_argument('--Container',action="store",type=str,help="Container Blob Name",default="testing")
        parser.add_argument('--ZipName',action="store",type=str,help="Zip Folder Name",default="TempZips")
        args=parser.parse_args()
        argdict=vars(args)
      
        Boolvar=argdict['LatestDate']
        DeltaVar=argdict['DaysBack']
        Folder=argdict['GHCNFolder']
        TmpFolder=argdict['TempFolder']
        MnFolder=argdict['MainFolder']
        SqlSite=argdict['SqlSite']
        UName=argdict['UserName']
        PWord=argdict['Password']     
        DBaseName=argdict['Database']
        AccName=argdict['AccountName']
        AccKey=argdict['AccountKey']
        ConName=argdict['Container']
        ZipFolder=argdict['ZipName']
      
        Station="GHCN_stations.csv"
        GHCNKey=os.path.join(MnFolder,Station)
        Key="UFDBKey.csv"
        KeyPath=os.path.join(MnFolder,Key)
        SpatialName="WGS_1984_Web_Mercator_Auxiliary_Sphere.prj"
        Spatialref=os.path.join(MnFolder,SpatialName)
      
        DtList,YrLst=YearDateRng(Boolvar,DeltaVar)
        CsvPths,GHCNspc=FTPDownLoad(YrLst,Folder)
        GHCNList=TableMerge(CsvPths,GHCNKey)
        GHCNSnowTable=SNOWDATA(GHCNList)
        SnowPths=SnowCsvOut(GHCNSnowTable,DtList,TmpFolder)
        RList=SnowKriging(SnowPths,DtList,TmpFolder,Spatialref)
        ExtractVar(DtList,RList,KeyPath,Spatialref,MnFolder)
        SqlInsert(MnFolder,SqlSite,UName,PWord,DBaseName,DtList)
        ZipDir,ZipPth=zipfoldertostorage(GHCNspc,Folder,ZipFolder,AccName,AccKey,ConName)
        clearworkspc(ZipDir,ZipPth,GHCNspc,TmpFolder,MnFolder,YrLst,DtList)
      
main()
