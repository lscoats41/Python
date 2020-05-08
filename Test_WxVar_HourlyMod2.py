import numpy as np
import pymssql
import datetime as dt
import arcpy
import argparse
import UFDBvalidationReference as uvr
import os
import zipfile
from shutil import copyfile
from arcpy import env
env.overwriteOutput = True

from arcpy.sa import *
arcpy.env.overwriteOutput = True



arcpy.CheckOutExtension("Spatial")




def ptdt(message): ##### function to state messages when process completed
	opdt = dt.datetime.utcnow()
	unixts = int((opdt - dt.datetime(1970, 1, 1)).total_seconds())
	print message
	print dt.datetime.utcnow() - opdt


# def applybin(value,parameter)
#     return 1


def CreateInputData(inputDir):  ######## function to create shapefile template for GWRFunction and Rasterfication function
	inputsDirectory=inputDir
	ELEVshp = inputsDirectory + "\\UFDB_NA_ELEV.shp"
	PTSLoc = inputsDirectory + "\\UFDB_PT_TPNTS_ELEV.shp"
	RasterLoc = inputsDirectory + "\\ufdb_na_mask"
	elevRaster = arcpy.sa.Raster(RasterLoc)
	Extent = elevRaster.extent
	Spref = arcpy.Describe(PTSLoc).spatialReference
	
	return ELEVshp,PTSLoc,RasterLoc,elevRaster,Extent,Spref 

def CreateTempDataValue(variable,Conv,ConvUnits): ##### fucntion to create values for validation process

	TempStation = "in_memory\\StationsBase"
	TempPtsLoc = "in_memory\\CellsWithElev"
	QCValH = uvr.ValidationRangeDict[variable]["High"]
	QCValL = uvr.ValidationRangeDict[variable]["Low"]
	
	if Conv=="True" and ConvUnits=="cm":
		CEquation=uvr.ConversionEquationDict[variable][ConvUnits]
	elif Conv=="True" and ConvUnits=="km":
		CEquation=uvr.ConversionEquationDict[variable][ConvUnits]
	elif Conv=="True" and ConvUnits=="Celsius":
		CEquation=uvr.ConversionEquationDict[variable][ConvUnits]
	else:
		CEquation=None
	
	return TempStation,TempPtsLoc,QCValH,QCValL,CEquation


	
def PullSqlData(TIndex,Variable,OutFolder,TempFolder,Eshp,TmpShp): ##### Function to pull data from Sql for creation of Dictionary with StationCode,Time,WxVar. In addition create a list for Targeted Times in the future,folders to store results
	
	startdt = dt.datetime.now()

	# na_elev_location = Share + DataDirectory + "/ArcMap/North_America/na_elev"

	conn = pymssql.connect(server='api-sql-qc.accu.accuwx.com', user='AES_DataReader', password='AES_DataR3ad3r!',
                       database='adc_forecast')
	cursor = conn.cursor()

	sql = """
		BEGIN
		SELECT DISTINCT DATE_TIME
		FROM [adc_forecast].[dbo].[vw_Hourly]
		WHERE DATE_TIME > CURRENT_TIMESTAMP
		ORDER BY DATE_TIME ASC
		END;
	"""

	opdt = dt.datetime.utcnow()
	cursor.execute(sql)
	rows = cursor.fetchall()
	print rows[1][0]
	ptdt("Time to retrive values")

	targetTimes = []
	for n in TIndex:
		if n != 1:
			# targetTimes.append([rows[0][0] + dt.timedelta(hours=-3),rows[n][0] + dt.timedelta(hours=-1),n])
			targetTimes.append([rows[0][0], rows[n][0] + dt.timedelta(hours=-1), n])
		else:
			targetTimes.append([rows[n][0] + dt.timedelta(hours=-1), rows[n][0] + dt.timedelta(hours=-1), n])
	print targetTimes

	mindt = min([startendtimes[0] for startendtimes in targetTimes])
	maxdt = max([startendtimes[1] for startendtimes in targetTimes])

	print "mindt", mindt
	print "maxdt", maxdt

	mindttsstr = str(int((mindt - dt.datetime(1970, 1, 1)).total_seconds()))
	currentoutdir = OutFolder + "\\p" + mindttsstr
	currenttempdir = TempFolder + "\\p" + mindttsstr
	## Copy Station features and get all stations

	opdt = dt.datetime.utcnow()
	arcpy.CopyFeatures_management(Eshp, TmpShp)
	ptdt("Time to copy to memory")

	opdt = dt.datetime.utcnow()
	STATIONS = []
	rows = arcpy.da.SearchCursor(TmpShp, ["STATION_CO"])
	for row in rows:
		STATIONS.append(row[0])

		stationtext = "(" + ",".join(["'" + station + "'" for station in STATIONS]) + ")"

	sql = """
			BEGIN
			SELECT STATION_CODE, DATE_TIME, """ + Variable + """
			FROM adc_forecast.dbo.vw_Hourly
			WHERE DATE_TIME >= """ + mindt.strftime("'%Y-%m-%d %H:%M:%S'") + """ AND DATE_TIME <= """ + maxdt.strftime(
			"'%Y-%m-%d %H:%M:%S'") + """ AND STATION_CODE in """ + stationtext + """
			END;
		"""

	######print sql
	cursor.execute(sql)
	rows = cursor.fetchall()
	ptdt("Time to get forecasts.")

	DateStationCodeTempDict = {}

	allTimes = []

	for row in rows:
		if row[1] not in allTimes:
			allTimes.append(row[1])
		if DateStationCodeTempDict.has_key(row[0]) == False:
			DateStationCodeTempDict[row[0]] = {}
			DateStationCodeTempDict[row[0]][row[1]] = row[2]
		else:
			DateStationCodeTempDict[row[0]][row[1]] = row[2]

	allTimes.sort()
	print allTimes
	
	conn.close()
	
	return allTimes,DateStationCodeTempDict,targetTimes,currentoutdir,currenttempdir,mindttsstr

def CreateRasters(LocPts,RLoc,num,outFc,Tfolder,Ofolder,Ifolder,Index,Var,PreName,minstr,Spref,Conv,ConvFormula,ConvU,CFolder,CName): ##### this function will be executed in GWRFunction to create Rasters and Conversion Raster if needed once the Predicted Output shapefile is created.
	
	arcpy.env.workspace=Ofolder
	
	FIDToXYLocation = {}
	rows = arcpy.da.SearchCursor(LocPts, ["FID", "X", "Y"])

	for row in rows:
		FIDToXYLocation[row[0]] = [row[1], row[2]]
		
	ufdbnamask = arcpy.Raster(RLoc)
	masklowerleft = arcpy.Point(ufdbnamask.extent.XMin, ufdbnamask.extent.YMin)
	maskcellsize = ufdbnamask.meanCellWidth
	npmask = arcpy.RasterToNumPyArray(ufdbnamask)

	sendrasters = []
	convsendrasters = []
	sendKML=[]
	convsendKML=[]
	npmask.shape
	newarr = np.full(npmask.shape, 0.0, dtype=np.dtype('Float64'))
	maskcopy = np.copy(npmask)
	print "shape"
	print maskcopy.shape
	thesefields = arcpy.ListFields(outFc)
	print "Fields"
	IDField = "FID"
	for afield in thesefields:
		print afield.name
		if afield.name == "ObjectID":
			IDField = "ObjectID"

	with arcpy.da.SearchCursor(outFc, [IDField, "Predicted"]) as rows:
		# print FIDToXYLocation.keys()
		for row in rows:
			# print row[0]
			# print FIDToXYLocation[row[0]]
			# print FIDToXYLocation[row[0]][0]
			X = FIDToXYLocation[row[0]][0]
			Y = FIDToXYLocation[row[0]][1]
			newarr[Y, X] = row[1]

		newras = arcpy.NumPyArrayToRaster(
			in_array=newarr,
			lower_left_corner=masklowerleft,
			x_cell_size=maskcellsize,
			y_cell_size=maskcellsize,
			value_to_nodata=0.0
		)

		newras.save(Tfolder + "\\outr" + str(num) + ".tif")

		arcpy.Resample_management(Tfolder+ "\\outr" + str(num) + ".tif",
								  Tfolder + "\\outz" + str(num) + ".tif", ".008",
								  "BILINEAR")
		outCon = Con(IsNull(Tfolder + "\\outz" + str(num) + ".tif"), 0,
					 Tfolder + "\\outz" + str(num) + ".tif")
		outCon.save(Tfolder + "\\outcon" + str(num) + ".tif")
		arcpy.DefineProjection_management(Tfolder + "\\outcon" + str(num) + ".tif", coor_system=Spref)
		opdt = dt.datetime.utcnow()

		rcraster = arcpy.sa.ReclassByTable(Tfolder + "\\outcon" + str(num) + ".tif",
										   Ifolder+ "\\NearTerm.gdb\\RCValTable" + Var, "LowValue",
										   "HighValue", "AssignValue", "NODATA")

		#    rcraster = arcpy.sa.SetNull(rcraster, rcraster, "Value = 0")
		
		TifSave=Ofolder+"\\outrrr" + str(Index) + ".tif"
		rcraster.save(TifSave)
		ptdt("reclass")


		ImgSave=Ofolder + "\\outrrr" + str(Index) + ".img"
		rcraster.save(ImgSave)
		ptdt("reclass")

		# Replace a layer/table view name with a path to a dataset (which can be a layer file) or create the layer/table view within the script
		# The following inputs are layers or table views: "outrrr0.tif"

		arcpy.SetRasterProperties_management(in_raster=Ofolder + "\\outrrr" + str(Index) + ".tif", data_type="",
											 statistics="", stats_file="",
											 nodata="1 0", key_properties="")

		arcpy.AddColormap_management(TifSave, "#",Ifolder + "\\" + Var + ".clr")

		arcpy.AddColormap_management(ImgSave, "#",Ifolder + "\\" + Var + ".clr")

		outfilename = Ofolder + "\\" + minstr + "_" + str(Index).zfill(3) + ".tif"
		outfilenamewithprefixandsuffix = PreName + "_" + minstr + "_" + str(Index).zfill(3) + ".tif"
		
		ptdt(outfilenamewithprefixandsuffix)

		arcpy.CopyRaster_management(in_raster=TifSave,out_rasterdataset=outfilename, config_keyword="",
									background_value="", nodata_value=None, onebit_to_eightbit="NONE",
									colormap_to_RGB="", pixel_type="8_BIT_UNSIGNED", scale_pixel_value="NONE",
									RGB_to_Colormap="NONE")

		#####3sendrasters.append([outfilename, outfilenamewithprefixandsuffix])

		RasterResult=[outfilename, outfilenamewithprefixandsuffix]

		
		TifLyr=PreName+".lyr"
		arcpy.MakeRasterLayer_management(outfilename,TifLyr,"","-179.14732999988 17.675849999878 -49.99532999988 83.123849999878","")
		
		KMLfilename = Ofolder + "\\" + minstr + "_" + str(Index).zfill(3) + ".kmz"
		KMLfilenamewithprefixandsuffix = PreName + "_" + minstr + "_" + str(Index).zfill(3) + ".kmz"
		
		arcpy.LayerToKML_conversion(TifLyr,KMLfilename,"0","NO_COMPOSITE","DEFAULT","1024","96")
		
		#####sendKML.append([KMLfilename,KMLfilenamewithprefixandsuffix])

		KMLResult=[KMLfilename,KMLfilenamewithprefixandsuffix]

		

		##########outfilename = Ofolder + "\\" + minstr + "_" + str(Index).zfill(3) + ".png"
		########outfilenamewithprefixandsuffix = PreName + "_" + minstr + "_" + str(Index).zfill(3) + ".png"
		
		
		#########ImgRaster=arcpy.CopyRaster_management(in_raster=TifSave,out_rasterdataset=outfilename,config_keyword="", background_value="", nodata_value=None, onebit_to_eightbit="NONE",colormap_to_RGB="", pixel_type="4_BIT")
		
		###########sendrasters.append([outfilename, outfilenamewithprefixandsuffix])

		
		print "Add Colormap done"
		# newras.save(out_prediciton_raster)


		if Conv == "True":
			rastertoconv = arcpy.Raster(Tfolder + "\\outcon" + str(num) + ".tif")
			convertedraster = eval(ConvFormula.replace("{raster}", "rastertoconv"))
			convertedraster.save(Tfolder+ "\\outcon" + str(num) + ConvU + ".tif")
			rcraster = arcpy.sa.ReclassByTable(Tfolder + "\\outcon" + str(num) + ConvU + ".tif",
											   Ifolder + "\\NearTerm.gdb\\RCValTable" + Var + ConvU,
											   "LowValue",
											   "HighValue", "AssignValue", "NODATA")

			#    rcraster = arcpy.sa.SetNull(rcraster, rcraster, "Value = 0")
			if not os.path.exists(CFolder):
				os.makedirs(CFolder)
			CTifSave=CFolder+"\\outrrr" + str(Index) + ConvU + ".tif"
			rcraster.save(CTifSave)
			ptdt("reclass")
			
			CImgSave=CFolder + "\\outrrr" + str(Index) + ConvU + ".img"
			rcraster.save(CImgSave)
			ptdt("reclass")

			# Replace a layer/table view name with a path to a dataset (which can be a layer file) or create the layer/table view within the script
			# The following inputs are layers or table views: "outrrr0.tif"

			arcpy.SetRasterProperties_management(
				in_raster=CTifSave, data_type="",
				statistics="", stats_file="",
				nodata="1 0", key_properties="")

			arcpy.AddColormap_management(CTifSave, "#",Ifolder + "\\" + Var + ConvU + ".clr")
			
			arcpy.AddColormap_management(CImgSave, "#",Ifolder + "\\" + Var + ConvU + ".clr")

			outfilename = CFolder + "\\" +minstr+ "_" + str(Index).zfill(3) + ".tif"
			outfilenamewithprefixandsuffix = CName + "_" + minstr + "_" + str(
				Index).zfill(3) + ".tif"

			arcpy.CopyRaster_management(
				in_raster=CTifSave,
				out_rasterdataset=outfilename, config_keyword="",
				background_value="", nodata_value=None, onebit_to_eightbit="NONE",
				colormap_to_RGB="", pixel_type="8_BIT_UNSIGNED", scale_pixel_value="NONE",
				RGB_to_Colormap="NONE")

			######convsendrasters.append([outfilename, outfilenamewithprefixandsuffix])

			ConvRasterResult=[outfilename,outfilenamewithprefixandsuffix]

			
			CTifLyr=CName+".lyr"
			
			KMLfilename = CFolder + "\\" + minstr + "_" + str(Index).zfill(3) + ".kmz"
			KMLfilenamewithprefixandsuffix = CName + "_" + minstr + "_" + str(Index).zfill(3) + ".kmz"
			
			arcpy.MakeRasterLayer_management(outfilename,CTifLyr,"","-179.14732999988 17.675849999878 -49.99532999988 83.123849999878","")
		
			arcpy.LayerToKML_conversion(CTifLyr,KMLfilename,"0","NO_COMPOSITE","DEFAULT","1024","96")
		
			#####convsendKML.append([KMLfilename,KMLfilenamewithprefixandsuffix])

			ConvKMLResult=[KMLfilename,KMLfilenamewithprefixandsuffix]


			########outfilename = CFolder + "\\" + minstr + "_" + str(Index).zfill(3) + ".png"
			########outfilenamewithprefixandsuffix = CName + "_" + minstr + "_" + str(Index).zfill(3) + ".png"

			########arcpy.CopyRaster_management(in_raster= CImgSave,out_rasterdataset=outfilename,config_keyword="", background_value="", nodata_value=None, onebit_to_eightbit="NONE",colormap_to_RGB="", pixel_type="4_BIT")
			
			#######convsendrasters.append([outfilename, outfilenamewithprefixandsuffix])
		else:
                        ConvRasterResult=None
                        ConvKMLResult=None
			ptdt("No Conversion Requested")
                        
	
	return RasterResult,ConvRasterResult,KMLResult,ConvKMLResult

	
def GWRFunction(LocPts,TempPts,RLoc,TTimes,coutdir,ctempdir,InputDir,TempStation,Datadict,HVal,LVal,TempDir,mth,var,PreName,minstr,Spref,Conv,ConvEq,ConvUnits,ConvFolder,ConvName): ######Function to execute GWR, Rasters for English Standar Metrics and converstion to internation metrics if requested
	
	opdt = dt.datetime.utcnow()
	arcpy.CopyFeatures_management(LocPts, TempPts)
	ptdt("CopyStationsAndPredPoints")

	ourFields = arcpy.ListFields(TempPts)
	print "in memory points location field names"
	for afield in ourFields:
		print afield.name

	###FIDToXYLocation = {}
	####rows = arcpy.da.SearchCursor(LocPts, ["FID", "X", "Y"])

	###for row in rows:
		#####FIDToXYLocation[row[0]] = [row[1], row[2]]

	RasterLst=[]
	ConvRasterLst=[]
	KMLLst=[]
        ConvKMLLst=[]
	for num, targetTime in enumerate(TTimes):
		thisIndex = targetTime[2]
		print thisIndex
		outputdirectory = coutdir + "\\o" + minstr + "_" + str(num)
		if not os.path.exists(outputdirectory):
			os.makedirs(outputdirectory)
		intermediatedirectory = ctempdir + "\\o" + minstr + "_" + str(num)
		if not os.path.exists(intermediatedirectory):
			os.makedirs(intermediatedirectory)
		print num, targetTime
		starttime = targetTime[0]
		print starttime
		endtime = targetTime[1]
		print endtime
		modelrunstarttime = dt.datetime.now()
		opdt = dt.datetime.utcnow()
		arcpy.AddField_management(TempStation, "VALUE" + str(num), "FLOAT")
		ptdt("Add VALUE FIELD")

		StationsWithInvalidValues = []
		
                
		opdt = dt.datetime.utcnow()
   		with arcpy.da.UpdateCursor(TempStation, ["STATION_CO", "VALUE" + str(num)]) as rows:
			for row in rows:
				if mth=="Sum":
					stationvalues = [Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if atime <= endtime]
					if all(((avalue > LVal and avalue < HVal) and avalue is not None) for avalue in
						   stationvalues):
						val = sum([Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if
							   atime <= endtime])
						row[1] = val
						rows.updateRow(row)
				elif mth=="Max":
					stationvalues = [Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if atime <= endtime]
					if all(((avalue > LVal and avalue < HVal) and avalue is not None) for avalue in
						   stationvalues):
						val=([Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if atime <= endtime])
						Mval=max(val)
						row[1] = Mval
						rows.updateRow(row)
				elif mth=="Min":
					stationvalues = [Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if atime <= endtime]
					if all(((avalue > LVal and avalue < HVal) and avalue is not None) for avalue in
						   stationvalues):
						val=([Datadict[row[0]][atime] for atime in Datadict[row[0]].keys() if atime <= endtime])
						Mval=min(val)
						row[1] = Mval
						rows.updateRow(row)
				else:
					print stationvalues
					print "row contents ", str(row[1])
					StationsWithInvalidValues.append(row[0])
		del row

		ptdt("Add parameter values.")

		print "Invalid Value Stations: ", str(len(StationsWithInvalidValues))

		cellsize = "0.05"
		explanatory_field = "RASTERVALU"
		kerneltype = "ADAPTIVE"
		bandwidth = "BANDWIDTH_PARAMETER"
		dependent = "VALUE" + str(num)
		distance = ""
		numberofneighbors = 23

		out_featureclass = "in_memory\\out" + str(num)
		out_featureclass = TempDir + "\\outf" + str(num) + ".shp"
		out_prediction_featureclass = "in_memory\\outpp" + str(num)
		out_prediction_featureclass = TempDir + "\\outfc" + str(num) + ".shp"
		out_prediciton_raster = "in_memory\\outpr" + str(num)

		opdt = dt.datetime.utcnow()

		arcpy.GeographicallyWeightedRegression_stats(
			in_features=TempStation,
			dependent_field=dependent,
			explanatory_field=explanatory_field,
			out_featureclass=out_featureclass,
			kernel_type=kerneltype,
			bandwidth_method=bandwidth,
			distance=distance,
			number_of_neighbors=numberofneighbors,
			weight_field="",
			coefficient_raster_workspace="",
			cell_size="",
			in_prediction_locations=TempPts,
			prediction_explanatory_field=explanatory_field,
			out_prediction_featureclass=out_prediction_featureclass
		)
        
		ptdt("GWR done.")
		ptdt(out_prediction_featureclass)
		print "shape 1"
		ptdt(LocPts)
		ptdt(RLoc)
		ptdt(intermediatedirectory)
		ptdt(outputdirectory)
		ptdt(InputDir)
		ptdt(thisIndex)
		ptdt(var)
		ptdt(PreName)
		ptdt(minstr)
		RasterRes,CRasterRes,KMLRes,CKMRes=CreateRasters(LocPts,RLoc,num,out_prediction_featureclass,intermediatedirectory,outputdirectory,InputDir,thisIndex,var,PreName,minstr,Spref,Conv,ConvEq,ConvUnits,ConvFolder,ConvName)

                RasterLst.append(RasterRes)
		ConvRasterLst.append(CRasterRes)
		KMLLst.append(KMLRes)
                ConvKMLLst.append(CKMRes)
	
	return RasterLst,ConvRasterLst,KMLLst,ConvKMLLst
	

def ZipFileCreation(crtoutdir,outnetdir,fprename,minstr,conv,convprename,InitialRasters,ConvRasters,IKML,CKML): ####Final Proces to create a ZipFolder
	
	ptdt(InitialRasters)
	ptdt(ConvRasters)
	ptdt(IKML)
	ptdt(CKML)

	localZipfileName = crtoutdir + "\\" + fprename + "_" + minstr + '.zip'
	networkZipfileName = outnetdir + "\\" + fprename + "_" + minstr + '.zip'

	with zipfile.ZipFile(localZipfileName, 'w') as myzip:
		for i in range (0,len(InitialRasters)):
			myzip.write(InitialRasters[i][0], InitialRasters[i][1])
			myzip.write(IKML[i][0], IKML[i][1])
				
	print localZipfileName
	print networkZipfileName
#copyfile(localZipfileName, networkZipfileName)

	if conv == "TRUE":
		localZipfileName = crtoutdir + "\\" + convprename + "_" + minstr + '.zip'
		networkZipfileName = outnetdir + "\\" + convprename + "_" + minstr + '.zip'
		with zipfile.ZipFile(localZipfileName, 'w') as myzip:
			for j in range (0,len(ConvRasters)):
				myzip.write(ConvRasters[j][0], ConvRasters[j][1])
				myzip.write(CKML[j][0], CKML[j][1])
			print localZipfileName
			print networkZipfileName
    #copyfile(localZipfileName, networkZipfileName)
	
	return localZipfileName,networkZipfileName




def main():

	parser = argparse.ArgumentParser()

	parser.add_argument('--parameter', action="store",type=str,help="Weather Variable",default="SNOW")
	parser.add_argument('--targethoursout', action="store",type=str,help="sequence of Hours in a day", default="3,6,9,12,15,18,21,24")
	# parser.add_argument('--targethoursout',action="store",default='3')
	parser.add_argument('--inputsdir', action="store",type=str,help="Folder for shapefile inputs",
						default="C:\\ProcessDataFiles\\UFDBToRaster\\Data\\ArcMap\\North_America\\SNOW")
	parser.add_argument('--outputsdir', action="store",type=str,help="output folder",default="C:\\ProcessDataFiles\\UFDBToRaster\\Data\\Outputs\\North_America\\Hourly24")
	parser.add_argument('--intermediatedir', action="store",type=str,help="Temp Workspace",default="D:\\ProcessDataFiles\\UFDBToRaster\\Data\\North_America\\Hourly24")
	parser.add_argument('--fileprefixname', action="store",type=str,help="Name of Output file", default="24hour_snowfall_forecast")
	parser.add_argument('--method',action="store",type=str,help="Do Sum for Precip and Max for Temps",default="Sum")
	parser.add_argument('--outputnetworkdirectory', action="store",type=str,help="Sql Directory",default="\\\\ingest-data-03.accu.accuwx.com\\ldmdata\\intforecastimagery24hr\\input")
	# added conversion parameters
	parser.add_argument('--doconversion', action="store", type=str,help="Decide to Convert WxVar to Metric Units",default="False")
	######parser.add_argument('--conversionexp', action="store",type=str,help="Input of Conv. equation", default="{raster} * 2.54")
	parser.add_argument('--conversionunits', action="store", type=str,help="Units conversion will be done in.",default="cm")
	parser.add_argument('--outputconversiondirectory', action="store",type=str, help="Units conversion will be done in.",default="cm")
	parser.add_argument('--convfileprefixname', action="store", type=str,help="prefix for filename",default="24hour_snowfallcm_forecast")
	parser.add_argument('--forecastdatasqlserver', action="store",type=str,help="Sql Server", default="api-sql-qc.accu.accuwx.com")

	args = parser.parse_args()
	argdict = vars(args)

	parameter = argdict["parameter"]
	targetIndex = [int(anhour) for anhour in argdict["targethoursout"].split(',')]
	inputsDirectory = argdict["inputsdir"]
	outputDir = argdict["outputsdir"]
	TempDir = argdict["intermediatedir"]
	fileprefixname = argdict["fileprefixname"]
	method=argdict["method"]
	outnetdirectory = argdict["outputnetworkdirectory"]

	if argdict["doconversion"] == "True" or argdict["doconversion"] == "False":
		doconv = argdict["doconversion"]
		if doconv == "True":
			#####convexpression = argdict["conversionexp"]
			convunits = argdict["conversionunits"]
			outconvdirectory = argdict["outputconversiondirectory"]
			convprefixname = argdict["convfileprefixname"]
		else:
			######convexpression = None
			convunits = None
			outconvdirectory = None
			convprefixname = None
	else:
		sys.exit(0)

	print method

	print targetIndex

	
	ELEVshp,PTSLoc,RasterLoc,elevRaster,Extent,Spref =CreateInputData(inputsDirectory)

	TempStation,TempPtsLoc,QCValH,QCValL,convexpression=CreateTempDataValue(parameter,doconv,convunits)

	TimesRange,StationCodeDict,targetTimes,currentoutdir,currenttempdir,mindttsstr=PullSqlData(targetIndex,parameter,outputDir,TempDir,ELEVshp,TempStation)

	InitialRasters,ConvRasters,InitialKML,ConvKML=GWRFunction(PTSLoc,TempPtsLoc,RasterLoc,targetTimes,currentoutdir,currenttempdir,inputsDirectory,TempStation,StationCodeDict,QCValH,QCValL,TempDir,method,parameter,fileprefixname,mindttsstr,Spref,doconv,convexpression,convunits,outconvdirectory,convprefixname)

	LZipfile,NZipfile=ZipFileCreation(currentoutdir,outnetdirectory,fileprefixname,mindttsstr,doconv,convprefixname,InitialRasters,ConvRasters,InitialKML,ConvKML)


main()








