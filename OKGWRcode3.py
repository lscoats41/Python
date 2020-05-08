import os
import datetime as dt
import pandas as pd
from pandas import *
import pyodbc
import arcpy
import pymssql
from arcpy.sa import *
import numpy as npy
import csv
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True




def DateListCreation(Spc,UHDBWx): ###### This is where Date List is created for Future Loops

    DateLst=[]
    Csv=UHDBWx+".csv"
    CsvFile=os.path.join(Spc,Csv)
    x=0
    with open(CsvFile,"r") as WxCsv:
        for lines in WxCsv:
            x+=1
            line=lines.split(",")
            if x>1:
                for i in range(0,len(line)):
                    Dates=line[3].strip('"')
                    if Dates not in DateLst:
                          DateLst.append(Dates)

    DateLst.sort()
    print(DateLst)

    
    return DateLst


def MesoDictCreation(Spc,Data): ###### This how the original Dictionary of OK Mesonet Weather variables and list for Station codes utilized for future iteration

    OKWxDic={}
    StationCodes=[]
    MesoCsv=Data+".csv"
    MesoFile=os.path.join(Spc,MesoCsv)

    with open(MesoFile,"r") as MesoStations:
        x=0
        for lines in MesoStations:
            x+=1
            line=lines.split(",")
            if x>1:
                    for i in range(0,len(line)):
                        StationCode=line[0].strip('"')
                        Date=line[1].strip('"')
                        Month=str(line[2].strip('"'))
                        Ltemp=float(line[3])
                        Htemp=float(line[4])
                        Rain=float(line[5].strip('\n'))
                        if StationCode not in StationCodes:
                            StationCodes.append(StationCode)
                        if StationCode not in OKWxDic.keys():
                            OKWxDic[StationCode]={}
                            OKWxDic[StationCode][Date]={}
                            OKWxDic[StationCode][Date]=Month,Ltemp,Htemp,Rain
                        elif StationCode in OKWxDic.keys():
                            OKWxDic[StationCode][Date]={}
                            OKWxDic[StationCode][Date]=Month,Ltemp,Htemp,Rain
    print(OKWxDic['ARD2'])
    MesoStations.close()

    print(StationCodes)


    return StationCodes,OKWxDic

def ShapefileCreation(Spc,DLst,WxCsv,Msk): ####### This is where a daily shapefiles created by date in datelist wth MinTemp,MaxTemp,and Precip variables for future GWR Interpolation
    DBFolder="TEST"
    RawCsv=WxCsv+".csv"
    BigDBF=WxCsv+".dbf"
    DBDataPath=os.path.join(Spc,DBFolder)
    RawFile=os.path.join(Spc,RawCsv)
    if not os.path.exists(DBDataPath):
        os.makedirs(DBDataPath)

    arcpy.env.workspace=DBDataPath

    Table=os.path.join(DBDataPath,BigDBF)

    Spref=arcpy.Describe(Msk).spatialReference

    BigTable=arcpy.TableToDBASE_conversion(RawFile,DBDataPath) ##### create a dbf file that is indexed for X,Y eventlayer function

    DBFList=[]
    ShpLst=[]

    Xpt="LONGITUDE"
    Ypt="LATITUDE"



    for Date in DLst: ##### loop for X,Y eventlayer function to create shapefile and pathway placed in a list
        Clause='"ObsDate"'+'='+"date"+" "+"'"+Date+" "+"00:00:00"+"'"
        Day=Date.replace("-","")
        Month=str(Date[5:7])
        Dy=str(Date[8:10])
        DBFName="WxVar"+Day+".dbf"
        DBFPath=os.path.join(DBDataPath,DBFName)
        TableSelect=arcpy.TableSelect_analysis(Table,DBFPath,Clause)
        XYlyr="Lyr"+Day
        StationPts=arcpy.MakeXYEventLayer_management(DBFPath,Xpt,Ypt,XYlyr,Spref)
        UHDBFc="WxVar"+Month+Dy+".shp"
        UHDBPth=os.path.join(DBDataPath,UHDBFc)
        UHDBGridPts=arcpy.FeatureClassToFeatureClass_conversion(StationPts,DBDataPath,UHDBFc)
        ShpLst.append(UHDBPth)


    

    return Spref,ShpLst


def GridCreation(GFile,Wkspc): #### Dictionary of North American Grid created


    GridTemp=os.path.join(Wkspc,GFile)
    GridLoc={}
    Fields=["FID","X","Y"]
    with arcpy.da.SearchCursor(GridTemp,Fields) as GridCursor:
        for grid in GridCursor:
            GridLoc[grid[0]]=[grid[1],grid[2]]
    del GridCursor

    return GridLoc

def GWRFunction(GLoc,Wspc,Var,shp,Grid,spr,Mask,Dt): ####### Input Shapefiles into Geographic Weighted Regression function based on Elevation as independent variable

    PredLoc=os.path.join(Wspc,Grid)
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.overwriteOutput = True

    MovePath=os.path.split(Wspc)[0]
    Folder="Raster"
    RasterPath=os.path.join(MovePath,Folder)
    if not os.path.exists(RasterPath):
        os.makedirs(RasterPath)

    RWkspc=os.path.join(RasterPath,Var)
    if not os.path.exists(RWkspc):
        os.makedirs(RWkspc)


    Day=Dt.replace("-","")

    inputFc=shp
    MaskRaster=r"W:\Lamar_Projects\ExperimentCodes\inputs\ufdb_na_mask"
    HeightRaster=arcpy.sa.Raster(MaskRaster)
    Extent=HeightRaster.extent
    arcpy.env.extent=Extent

    ufdbMask=arcpy.Raster(MaskRaster)
    LowLeftMask=arcpy.Point(ufdbMask.extent.XMin,ufdbMask.extent.YMin)
    CellSize=ufdbMask.meanCellWidth

    npMask=arcpy.RasterToNumPyArray(ufdbMask)

    spRef=spr

    Stations="in_memory\\StationBase"
    arcpy.CopyFeatures_management(inputFc, Stations)
    PtsLoc="in_memory\\CellsElev"
    arcpy.CopyFeatures_management(PredLoc, PtsLoc)

    explanatory_field = "RASTERVALU"
    kerneltype = "ADAPTIVE"
    bandwidth = "BANDWIDTH_PARAMETER"
    distance = ""
    # should make this a parameter
    numberofneighbors = 23


    depVar=Var
    depShpName=Var+Day+".shp"
    depShpFile=os.path.join(RWkspc,depShpName)
    depPredName=Var+Day+"pd.shp"
    depPredShpFile=os.path.join(RWkspc,depPredName)

    arcpy.GeographicallyWeightedRegression_stats(
            in_features=inputFc,
            dependent_field=depVar,
            explanatory_field=explanatory_field,
            out_featureclass=depShpFile,
            kernel_type=kerneltype,
            bandwidth_method=bandwidth,
            distance=distance,
            number_of_neighbors=numberofneighbors,
            weight_field="",
            coefficient_raster_workspace="",
            cell_size="",
            in_prediction_locations=PredLoc,
            prediction_explanatory_field=explanatory_field,
            out_prediction_featureclass=depPredShpFile
        )

    arraynew=npy.full(npMask.shape,-9999.0,dtype=npy.dtype('Float64'))
    copymask=npy.copy(npMask)

    PredFields=[str(f.name)for f in arcpy.ListFields(depPredShpFile)]

    PredStations="in_memory\\PredStations"                      ######### because the Predicted Shapefile is so large and is now going to go through a cursor before rasterization its best ot copy shapefile in memory
    arcpy.CopyFeatures_management(depPredShpFile, PredStations)


    fields=["FID","Predicted"]


    with arcpy.da.SearchCursor(PredStations,fields) as PCursor:
        for row in PCursor:
            IDKey=row[0]-1
            X=GLoc[IDKey][0]
            Y=GLoc[IDKey][1]
            arraynew[Y,X]=row[1]


    createRas=arcpy.NumPyArrayToRaster(arraynew,LowLeftMask,CellSize,CellSize)

    RasName=Var+Day+"pd.tif"
    OutRaster=os.path.join(RWkspc,RasName)
    createRas.save(OutRaster)



    return OutRaster

def GWRIteration(DtLst,Spref,ShpLst,Grid,GLoc,Wkspc,Msk,VLst): ##### This is where GWR Function is executed to create a tif file and pathway appended to a list for future extraction 

    LtempLst=[]
    HtempLst=[]
    RLst=[]

    for i in range(0,len(DtLst)): ##### Nested Loop by Date List and inner Loop the WxVar for GWR 
        Date=DtLst[i]
        shp=ShpLst[i]
        for var in VLst:
            Raster=GWRFunction(GLoc,Wkspc,var,shp,Grid,Spref,Msk,Date)
            if var=="LowTemp":
                LtempLst.append(Raster)
            elif var=="HighTemp":
                HtempLst.append(Raster)
            else:
                RLst.append(Raster)

    return LtempLst,HtempLst,RLst

def WDTRasterCreation(DLst,Spf,Wkspc,VLst):  ##### This is where WDT CD file are convert to Tif file then pathway appendt to a list

    MovePath=os.path.split(Wkspc)[0]

    WDTFile=os.path.join(MovePath,"WDTRaster")

    WDTFileHourly=os.path.join(MovePath,"WDTRasterHrly")

    WDTDates=[]
    for root,_,files in os.walk(WDTFile):
        for f in files:
            if f.find(".nc")>-1:
                WDTday=f[-18:-10]
                WDTDates.append(WDTday)




    RasterFolder=os.path.join(MovePath,"Raster")

    LowTempRaster=[]
    HighTempRaster=[]
    RainRaster=[]
    hourlist=[str(i).zfill(2) for i in range(0,24)]

    for Date in DLst: #### Outer Loop for Dates
        Day=Date.replace("-","")
        if Day in WDTDates: #### Condition if WDT has the Date
            NCFile="SkyWise_CONUS_SurfaceAnalysis_Daily_"+str(Day)+"-000000.nc"
            NCFilePath=os.path.join(WDTFile,NCFile)
            for v in VLst: #### inner Loop for WxVariables
                ImageFolder=os.path.join(RasterFolder,v)
                if not os.path.exists(ImageFolder):
                    os.makedirs(ImageFolder)
                RasterLayer="WDT"+v+Day+"lyr"
                RasterImg="WDT"+v+Day+".tif"
                ProjectImg="WDT"+v+Day+"pj.tif"
                RasterPath=os.path.join(ImageFolder,RasterImg)
                ProjectPath=os.path.join(ImageFolder,ProjectImg)
                if v=="LowTemp":
                    Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"minimum_temperature","lon","lat",RasterLayer,"","","BY_VALUE")
                    RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0) ###### after Raster Layer is created this where the Raster is rectified for spatial reference before projection using Raster to Numpy
                    RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                    RImg.save(RasterPath)
                    #####RImg=arcpy.CopyRaster_management(Rlyr,RasterPath)
                    RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                    arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                    LowTempRaster.append(ProjectPath)
                elif v=="HighTemp":
                    Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"maximum_temperature","lon","lat",RasterLayer,"","","BY_VALUE")
                    RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0)
                    RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                    RImg.save(RasterPath)
                    #######RImg=arcpy.CopyRaster_management(Rlyr,RasterPath)
                    RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                    arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                    HighTempRaster.append(ProjectPath)
                else:
                    Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"accumulated_precip","lon","lat",RasterLayer,"","","BY_VALUE")
                    RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0)
                    RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                    RImg.save(RasterPath)
                    ########RImg=arcpy.CopyRaster_management(Rlyr,RasterPath)
                    RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                    arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                    RainRaster.append(ProjectPath)
        else: ######## This is the condition if WDT does not have daily file the hourly files for that day is utilzed and MinTemp, MaxTemp, and Precip is generated via cell statatistics
           for v in VLst:
               ImageFolder=os.path.join(RasterFolder,v)
               if not os.path.exists(ImageFolder):
                    os.makedirs(ImageFolder)
               RasterName="WDT"+v+Day+"pj.tif"
               DailyPath=os.path.join(ImageFolder,RasterName)
               RasterLst=[]
               if v == "LowTemp":
                   for hour in hourlist:
                       NCFile="SkyWise_CONUS_SurfaceAnalysis_Hourly_"+str(Day)+"-"+hour+"0000.nc"
                       NCFilePath=os.path.join(WDTFileHourly,NCFile)
                       if os.path.exists(NCFilePath):
                           RasterLayer=v+str(Day)+hour+"lyr"
                           RasterImage=v+str(Day)+hour+".tif"
                           ProjectImage=v+str(Day)+hour+"pj.tif"
                           RasterPath=os.path.join(WDTFileHourly,RasterImage)
                           ProjectPath=os.path.join(WDTFileHourly,ProjectImage)
                           Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"temperature","lon","lat",RasterLayer,"","","BY_VALUE")
                           RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0)
                           RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                           RImg.save(RasterPath)
                           RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                           arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                           RasterLst.append(ProjectPath)
                       else:
                            print(NCFile+" This File Does Not Extst")
                   RasterStats=CellStatistics(RasterLst,"MINIMUM","NODATA")
                   RasterStats.save(DailyPath)
                   LowTempRaster.append(DailyPath)
               elif v == "HighTemp":
                   for hour in hourlist:
                       NCFile="SkyWise_CONUS_SurfaceAnalysis_Hourly_"+str(Day)+"-"+hour+"0000.nc"
                       NCFilePath=os.path.join(WDTFileHourly,NCFile)
                       if os.path.exists(NCFilePath):
                           RasterLayer=v+str(Day)+hour+"lyr"
                           RasterImage=v+str(Day)+hour+".tif"
                           ProjectImage=v+str(Day)+hour+"pj.tif"
                           RasterPath=os.path.join(WDTFileHourly,RasterImage)
                           ProjectPath=os.path.join(WDTFileHourly,ProjectImage)
                           Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"temperature","lon","lat",RasterLayer,"","","BY_VALUE")
                           RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0)
                           RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                           RImg.save(RasterPath)
                           RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                           arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                           RasterLst.append(ProjectPath)
                       else: 
                           print(NCFile+" This File Does Not Extst")
                        
                   RasterStats=CellStatistics(RasterLst,"MAXIMUM","NODATA")
                   RasterStats.save(DailyPath)
                   HighTempRaster.append(DailyPath)
               else:
                    for hour in hourlist:
                       NCFile="SkyWise_CONUS_SurfaceAnalysis_Hourly_"+str(Day)+"-"+hour+"0000.nc"
                       NCFilePath=os.path.join(WDTFileHourly,NCFile)
                       if os.path.exists(NCFilePath):
                           RasterLayer=v+str(Day)+hour+"lyr"
                           RasterImage=v+str(Day)+hour+".tif"
                           ProjectImage=v+str(Day)+hour+"pj.tif"
                           RasterPath=os.path.join(WDTFileHourly,RasterImage)
                           ProjectPath=os.path.join(WDTFileHourly,ProjectImage)
                           Rlyr=arcpy.MakeNetCDFRasterLayer_md(NCFilePath,"accumulated_precipitation_estimate_1hr","lon","lat",RasterLayer,"","","BY_VALUE")
                           RNmpy=arcpy.RasterToNumPyArray(RasterLayer,None,None,None,-9999.0)
                           RImg=arcpy.NumPyArrayToRaster(RNmpy,arcpy.Point(-130,20),.01,.01,-9999.0)
                           RImg.save(RasterPath)
                           RImgDef=arcpy.DefineProjection_management(RImg,Spf)
                           arcpy.ProjectRaster_management(RImgDef,ProjectPath,Spf)
                           RasterLst.append(ProjectPath)
                       else:
                            print(NCFile+" This File Does Not Extst")
                    RasterStats=CellStatistics(RasterLst,"SUM","NODATA")
                    RasterStats.save(DailyPath)
                    RainRaster.append(DailyPath)



    RainRaster.sort()
    HighTempRaster.sort()
    LowTempRaster.sort()
    print(RainRaster)

    return LowTempRaster,HighTempRaster,RainRaster



def ExtractWxVar(DtLst,Spref,Key,Wkspc,LRaster,HRaster,PRaster,WLRaster,WHRaster,WPRaster): ######## Create Mesonet OK Mesonet Shapefile to Extract Wx Variables from created RasterLists then a Dictionary Created 

    UHDBDic={}
    Mesoshapelst=[]
    Xline="LONLINES"
    Yline="LATLINES"
    Csv=Key+".csv"
    MesoKey=os.path.join(Wkspc,Csv)
    NewFolder="MesoShapes"
    UpPath=os.path.split(Wkspc)[0]
    ShpWkspc=os.path.join(UpPath,NewFolder)

    if not os.path.exists(ShpWkspc):
        os.makedirs(ShpWkspc)

    for i in range(0,len(DtLst)):
        Date=DtLst[i]
        Mo=Date[5:7]
        Dy=Date[8:10]
        LoTemp=LRaster[i]
        HiTemp=HRaster[i]
        Rain=PRaster[i]
        WLoTemp=WLRaster[i]
        WHiTemp=WHRaster[i]
        WRain=WPRaster[i]
        print(LRaster[i]+", "+HRaster[i]+", "+PRaster[i]+", "+WLRaster[i]+", "+WHRaster[i]+", "+WPRaster[i])
        LyrShp="OKLyr"+Mo+Dy
        OKStationPts=arcpy.MakeXYEventLayer_management(MesoKey,Xline,Yline,LyrShp,Spref)
        OKShp="OK"+Mo+Dy+".shp"
        OKShpPath=os.path.join(ShpWkspc,OKShp)
        MesoPts=arcpy.FeatureClassToFeatureClass_conversion(OKStationPts,ShpWkspc,OKShp)
        Mesoshapelst.append(OKShpPath)
        RasterList=[LoTemp,HiTemp,Rain,WLoTemp,WHiTemp,WRain]
        arcpy.sa.ExtractMultiValuesToPoints(OKShpPath,RasterList,"NONE")
        Fields=["STATION","LowTemp201","HighTemp20","Precip2015","WDTLowTemp","WDTHighTem","WDTPrecip2"]
        with arcpy.da.SearchCursor(OKShpPath,Fields) as MesoCursor:
            for rows in MesoCursor:
                Stations=rows[0]
                Ltemp=rows[1]
                Htemp=rows[2]
                if rows[3] < 0:
                    Rain=0
                else:
                    Rain=rows[3]
                WDTLtemp=((float(rows[4])-273.15)*1.8)+32  #### Conversion of Kelvin to Farenheit
                WDTHtemp=((float(rows[5])-273.15)*1.8)+32
                WDTRain= rows[6]*.0394 ##### Conversion to mm to inches
                if Stations not in UHDBDic.keys():
                    UHDBDic[Stations]={}
                    UHDBDic[Stations][Date]={}
                    UHDBDic[Stations][Date]=Ltemp,Htemp,Rain,WDTLtemp,WDTHtemp,WDTRain
                elif Stations in UHDBDic.keys():
                    UHDBDic[Stations][Date]={}
                    UHDBDic[Stations][Date]=Ltemp,Htemp,Rain,WDTLtemp,WDTHtemp,WDTRain

    print(Mesoshapelst)

    print(UHDBDic['BURN']['2015-03-02'])

    return UHDBDic,Mesoshapelst

def CSVCreation(MesDic,ExtDic,Stations,DLst,Wkspc): ##### Through CSV writer a csv generated for analysis of difference bettween sources for MinTemp,MaxTemp and Precip combing the two generated Dictionary

    Headers=["STATIONID","OBSDATE","MONTH","LOTEMP_OK","HITEMP_OK","PRECIP_OK","LOTEMP_UHDB","HITEMP_UHDB","PRECIP_UHDB","LOTEMPDIFF_UHDB","HITEMPDIFF_UHDB","PRECIPDIFF_UHDB","LOTEMP_WDT","HITEMP_WDT","PRECIP_WDT","LOTEMPDIFF_WDT","HITEMPDIFF_WDT","PRECIPDIFF_WDT"]
    MovePath=os.path.split(Wkspc)[0]
    NewFolder="DATA"
    DataWkspc=os.path.join(MovePath,NewFolder)
    if not os.path.exists(DataWkspc):
        os.makedirs(DataWkspc)

    CsvName="OKVerification.csv"
    FinalCsv=os.path.join(DataWkspc,CsvName)

    with open(FinalCsv,"wb") as OutCsv:
        rowheader=Headers
        writer=csv.writer(OutCsv,delimiter=",",quotechar='"',quoting=csv.QUOTE_MINIMAL)
        writer.writerows([rowheader])
        for station in Stations: ###### Loop to call Stations in both dictionary 
            for date in DLst: ### inner loop to call Dates in both dictionary
                if date in MesDic[station].keys(): #### condition if Station does not have a date of WxVariables
                    StationCode=station
                    ObsDate=date
                    Month=str(MesDic[station][date][0])
                    LTempOK=MesDic[station][date][1]
                    HTempOK=MesDic[station][date][2]
                    RainOK=MesDic[station][date][3]
                    LtempUHDB=ExtDic[station][date][0]
                    HtempUHDB=ExtDic[station][date][1]
                    RainUHDB=ExtDic[station][date][2]
                    LDiff=float(abs(LTempOK - LtempUHDB))
                    HDiff=float(abs(HTempOK - HtempUHDB))
                    RDiff=float(abs(RainOK - RainUHDB))
                    LtempWDT=ExtDic[station][date][3]
                    HtempWDT=ExtDic[station][date][4]
                    RainWDT=ExtDic[station][date][5]
                    LDiffWDT=float(abs(LTempOK - LtempWDT))
                    HDiffWDT=float(abs(HTempOK - HtempWDT))
                    RDiffWDT=float(abs(RainOK - RainWDT))
                    Lines=[StationCode,ObsDate,Month,LTempOK,HTempOK,RainOK,LtempUHDB,HtempUHDB,RainUHDB,LDiff,HDiff,RDiff,LtempWDT,HtempWDT,RainWDT,LDiffWDT,HDiffWDT,RDiffWDT]
                    writer.writerows([Lines])
                else:
                    print("Station "+StationCode+" not recorded on "+date) 

    OutCsv.close()

    return FinalCsv,Headers



def AggregateFunction(Csv,Wkspc,Head): ######### Using Pandas to aggregated absolute Difference variables totally or by month comparing UHDB and WDT to Ok Mesonet 

    MovePath=os.path.split(Wkspc)[0]
    NewFolder="DATA"
    DataWkspc=os.path.join(MovePath,NewFolder)
    if not os.path.exists(DataWkspc):
        os.makedirs(DataWkspc)

    MesoNetTable=pd.read_csv(Csv,names=Head,encoding="utf-8-sig")

    Count=len(MesoNetTable["PRECIPDIFF_WDT"])

    IndexLst=[j for j in range (0,Count)]

    MesoNetTable.index=IndexLst

    Variables=["LOTEMPDIFF_UHDB","HITEMPDIFF_UHDB","PRECIPDIFF_UHDB","LOTEMPDIFF_WDT","HITEMPDIFF_WDT","PRECIPDIFF_WDT"]

    for var in Variables:
        MesoNetTable[var]=pd.to_numeric(MesoNetTable[var],errors="coerce")

    MonthlyDiff=MesoNetTable.groupby(["STATIONID","MONTH"])[["LOTEMPDIFF_UHDB","HITEMPDIFF_UHDB","PRECIPDIFF_UHDB","LOTEMPDIFF_WDT","HITEMPDIFF_WDT","PRECIPDIFF_WDT"]].mean()

    MonthlyDiff=MonthlyDiff.reset_index()

    Name="MesonetMonthlyDiff.csv"

    MonthlyOutCsv=os.path.join(DataWkspc,Name)

    MonthlyDiff.to_csv(MonthlyOutCsv,sep=",",header=True,index=False)

    TotalName="MesonetTotalDiff.csv"

    TotalOutCsv=os.path.join(DataWkspc,TotalName)

    TotalDiff=MesoNetTable.groupby("STATIONID")[["LOTEMPDIFF_UHDB","HITEMPDIFF_UHDB","PRECIPDIFF_UHDB","LOTEMPDIFF_WDT","HITEMPDIFF_WDT","PRECIPDIFF_WDT"]].mean()

    TotalDiff=TotalDiff.reset_index()

    TotalDiff.to_csv(TotalOutCsv,sep=",",header=True,index=False)






def main(): ###### input of raw data and syntax of controlling functions. 
    VarLst=["LowTemp","HighTemp","Precip"]
    Workspace=r"W:\Lamar_Projects\OKMesonet\Temp"
    UHDBWx="OKUHDBWx"
    MesoData="MesonetData"
    Mask=r"W:\Lamar_Projects\OKMesonet\Raster\ufdb_na_mask"
    DateLst=DateListCreation(Workspace,UHDBWx)
    StationLst,MesoDic=MesoDictCreation(Workspace,MesoData)
    SpatialRef,ShpFileLst=ShapefileCreation(Workspace,DateLst,UHDBWx,Mask)
    GridFile="OKGRIDGCS.shp"
    IDLoc=GridCreation(GridFile,Workspace)
    StationKey="OKMesonetKey"
    LTRaster,HTRaster,PRaster=GWRIteration(DateLst,SpatialRef,ShpFileLst,GridFile,IDLoc,Workspace,Mask,VarLst)
    WDTLRaster,WDTHRaster,WDTPRaster=WDTRasterCreation(DateLst,SpatialRef,Workspace,VarLst)
    ExtractDic,MeosnetLst=ExtractWxVar(DateLst,SpatialRef,StationKey,Workspace,LTRaster,HTRaster,PRaster,WDTLRaster,WDTHRaster,WDTPRaster)
    CsvPath,Header=CSVCreation(MesoDic,ExtractDic,StationLst,DateLst,Workspace)
    AggregateFunction(CsvPath,Workspace,Header)


main()
