import arcpy  ########### import modules needed to run script
from arcpy import env
import os,os.path
import sys
import datetime as dt
import numpy as np
import argparse
import zipfile


arcpy.env.overwriteOutput=True


def FindCsv(day,space): ########  function to find specific Impact CSV based on Forecast Date
	Year=day[0:4]
	Month=day[5:7]
	Day=day[8:10]
	print space
	if not os.path.exists(space):
		os.makedirs(space)
	env.workspace=space
	Workspace=env.workspace
	print Workspace
	PathDate=Year+"."+Month+"."+Day
	BasePath=r"\\ictfilesrv.wdi-net.com\wdi\tpu\d3_express\data\archive"
	FirstPath=os.path.join(BasePath,Year)
	SubPath=os.path.join(FirstPath,PathDate)
	CsvName="AccuWeather D3 Express Impact_"+Year+Month+Day+".csv"
	csvfile=os.path.join(SubPath,CsvName)
	return csvfile,Workspace
	
	
	
def DbfCreation(csv,space,base):######## function to create Lists from csv with Columns DATE,OVERALLINDEX, RAININDEX				
	Firstdbf="Impact_"+base+".dbf"
	Seconddbf="Needed_"+base+".dbf"
	arcpy.TableToTable_conversion(csv,space,Firstdbf)
	Fields=["DATE","OVERALL","RAIN_INDEX"]
	NEWDATE=[]
	HIGH=[]
	PRWND=[]
	with arcpy.da.SearchCursor(Firstdbf,Fields) as ACursor:
		for row in ACursor:
			Day=row[0].strftime('%m/%d/%Y')
			Index=row[1]
			Rain=row[2]
			NEWDATE.append(Day)	
			HIGH.append(Index)
			PRWND.append(Rain)
		
	del ACursor

	TextFields=["POSTALCODE"]

	NEWPOSTAL=[ ]
	with arcpy.da.SearchCursor(Firstdbf,TextFields) as BCursor: #### needed to reformat postal codes with leading zeros and add to Lists
			for row in BCursor:
					if len(str(row[0]))<5:
							zip='0'+str(row[0])
							NEWPOSTAL.append(zip)
					else:
							zip=str(row[0])
							NEWPOSTAL.append(zip)
	del BCursor 


	FieldLength=int(10)
	TextLength=int(5)
	Indexlength=int(2)

	ZipValues="POSTCODES"
	NEWDATES="DATES"
	FIRSTINDEX="OV_INDEX"
	SECONDINDEX="PW_INDEX"
	JFields=[ZipValues,NEWDATES,FIRSTINDEX,SECONDINDEX]
	print JFields
	

	arcpy.CreateTable_management(env.workspace, Seconddbf)  
	arcpy.AddField_management(Seconddbf,ZipValues, "TEXT","","",TextLength)
	arcpy.AddField_management(Seconddbf,NEWDATES, "TEXT","","",FieldLength)
	arcpy.AddField_management(Seconddbf,FIRSTINDEX,"SHORT",Indexlength,"","")
	arcpy.AddField_management(Seconddbf,SECONDINDEX,"SHORT",Indexlength,"","")
	arcpy.DeleteField_management (Seconddbf,"Field1")

	with arcpy.da.InsertCursor(Seconddbf,"*") as ICursor: ##### Insert Lists of POSTALCODES,DATES,OVERALLINDEX,RAININDEX into Created dbf
		for i in range (0,len(NEWDATE)):
			OID=str(i)
			ZipValues=NEWPOSTAL[i]
			NEWDATES=NEWDATE[i]
			FIRSTINDEX=HIGH[i]
			SECONDINDEX=PRWND[i]
			row=[OID,ZipValues,NEWDATES,FIRSTINDEX,SECONDINDEX]
			ICursor.insertRow(row)
	del ICursor	
	
	
	return Seconddbf,JFields


def DatelstCreation(dy,Fdy): ##### Create a list of Dates from forecast date in the future
	Dates=[]
	Year=int(dy[0:4])
	Month=int(dy[5:7])
	Day=int(dy[8:10])
	ForcastDay=dt.datetime(Year,Month,Day)
	advance=int(Fdy)
	for i in range (0,advance):
		 future=datetime.timedelta(days=i)
		 day=ForcastDay+future
		 Date=day.strftime('%m/%d/%Y')
		 Dates.append(Date)
		 
	print Dates
	return Dates
	
	
	
	
	
	
	

def JoinImpacts(space,Dtlst,Flds,Sdbf):	###### This where Impact Shapefiles are created based of forecast dates join to zipcode shapfile
	OldZipFile=r"W://Accu_Internal_Resources//ArcMap_ArcGIS//Features//Shapefiles.gdb//USA_ZIPS_HD"
	CopyName="ZIPCODES.shp"
	CopyZipFile=os.path.join(space,CopyName)
	env.workpace=space
	ShpList=[]
	for row in Dtlst:
		Name='Impact_'+row.replace('/','_')+'.dbf'
		TempShp='Imp_'+row.replace('/','_')+'.shp'
		Newshp='Imp_'+row.replace('/','_')+'_prj.shp'
		NewImpact=os.path.join(env.workspace,Name)
		NewZipshp=os.path.join(env.workspace,Newshp)
		Where_Clause='"DATES"'+'='+"'"+row+"'"
		CopyShp=arcpy.CopyFeatures_management(OldZipFile,CopyZipFile)
		arcpy.TableSelect_analysis(Sdbf,NewImpact,Where_Clause)
		Location=space
		Join_Table=arcpy.JoinField_management(CopyShp,"POSTCODE",NewImpact,"POSTCODES",Flds)
		NewFields=[f.name for f in arcpy.ListFields(Join_Table)]
		print NewFields
		Feat_Clause='"POSTCODES"'+'<>'+"' '"
		arcpy.FeatureClassToFeatureClass_conversion(Join_Table,Location,TempShp,Feat_Clause)
		Newshp=arcpy.Project_management(TempShp,NewZipshp,r"W:\\Lamar_Projects\\Repository_Codes\\Temp\\WGS_1984_Web_Mercator_Auxiliary_Sphere.prj")
		arcpy.Delete_management(Join_Table)
		DBFTables=arcpy.ListTables()
		ShpList.append(NewZipshp)

	print DBFTables 
	print Flds
        return ShpList


        ##coordsys=arcpy.SpatialReference("W:\Lamar_Projects\Impacts\Florence_Hurricane\Temp\WGS_1984_Web_Mercator_Auxiliary_Sphere.prj")
	##arcpy.Project_management(TempShp,NewZipshp,"W:\Lamar_Projects\Impacts\Florence_Hurricane\Temp\WGS_1984_Web_Mercator_Auxiliary_Sphere.prj")
        ###NewZipshp=os.path.join(env.workspace,NewShp)
        ####Join_Table=arcpy.AddJoin_management(Copy_Lyr,"POSTCODE",NewImpact,"POSTALCODE")
	###arcpy.CopyFeatures_management(Join_Table,NewZipshp)

def KMLCreator(SLst,Wrkspc,IColor,Kname):
        arcpy.env.workspace=Wrkspc
        ColorRamp=IColor
        KmzName=Kname
        ListShp=SLst
        KmlFldr=os.path.join(Wrkspc,KmzName)

        if not os.path.exists(KmlFldr):
                os.makedirs(KmlFldr)

        KList=[]
        for shp in ListShp:
                Var="OV_INDEX"
                size="1400"
                queryClause=Var+'>0'
                NwShp=shp.replace('prj','sel')
                NwShpPath=os.path.join(Wrkspc,NwShp)
                Kml=shp.replace('shp','kmz')
                Raster=shp.replace('shp','img')
                NwLyr=shp.replace('shp','lyr')
                SelectShp=arcpy.Select_analysis(shp,NwShpPath,queryClause)
                RasterImg=arcpy.PolygonToRaster_conversion(SelectShp,Var,Raster,"CELL_CENTER","NONE","14000")
                MKLyr=arcpy.MakeRasterLayer_management(RasterImg,NwLyr,"","-13886732.4430092 2878561.3211416 -8916732.44300919 6280561.3211416","")
                SymLyr=arcpy.ApplySymbologyFromLayer_management(MKLyr,ColorRamp)
                print (NwShpPath)
                KmlFile=os.path.join(KmlFldr,Kml)
                Klyr=arcpy.LayerToKML_conversion(SymLyr,KmlFile,"0","NO_COMPOSITE","DEFAULT","1024","96")
                KList.append(KmlFile)

        return KList,KmlFldr
                
def CreateKmlZip(KLst,KmlFldr,Kname):
        KMLlst=KLst
        KmzName=Kname+".zip"
        KmzZip=os.path.join(KmlFldr,KmzName)
        with zipfile.ZipFile(KmzZip,'w') as KZip:
                for KML in KMLlst:
                        KZip.write(KML)

        return KmzZip



def main(): ######## call your varialble to execute functions from input parameters via command line
	parser=argparse.ArgumentParser()
	parser.add_argument('--date',action="store",type=str,help="ForecastDate YYYY-MM-DD",default="2019-05-22") ###input Date
	parser.add_argument('--workspace',action="store",help="Output Folder",default="C:\\Data")####input Workspace
	parser.add_argument('--Name',action="store",type=str,help="Name of Project",default="Flood") ##### Name of Impact Project
	parser.add_argument('--Future',action="store",type=int,help="Forecast Days in Advance",default="7") #### Days in advance
	parser.add_argument('--KmlName',action="store",type=str,help="Name of ZipFolder",default="FloodKml")##### name of Zipfile
	args=parser.parse_args()
	argdict=vars(args)
	Date=argdict['date']
	workspace=argdict['workspace']
	ProjectName=argdict['Name']
	FutureDays=argdict['Future']
	KmlDirName=argdict['KmlName']
	ImpColor=r"W:/Lamar_Projects/Repository_Codes/Temp/Imp_Rasterprj.lyr"
	CsvFile,Workspace=FindCsv(Date,workspace)
	Seconddbf,JFields= DbfCreation(CsvFile,Workspace,ProjectName)
	DateLst=DatelstCreation(Date,FutureDays)
	ShpLst=JoinImpacts(Workspace,DateLst,JFields,Seconddbf)
	KmlLst,KFolder=KMLCreator(ShpLst,workspace,ImpColor,KmlDirName)
	KmlZip=CreateKmlZip(KmlLst,KFolder,KmlDirName)
	

main()
	





























































