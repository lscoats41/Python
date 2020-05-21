import pandas as pd
import numpy as np
import argparse
import sklearn as sk
import RegFun as Rg
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
import sys
import os


def createdataframe(tfile,pfile): ######## Process to explore data,create graphs,check relationships, and normalize data by logging if necessary
	
	ModelFile=pd.read_csv(tfile,header=0)

	FinalFile=pd.read_csv(pfile,header=0)

	print(ModelFile.describe())
	
	print(FinalFile.describe())
	
	print("Look at y variable")
	
	DepVar=ModelFile.loc[:,"y"].values.reshape(-1,1)
	
	BinOne=int(input("How many bins for Historgram:"))
	
	clr=input("What color for graph:")
	
	Rg.hist(DepVar,BinOne,clr.lower())
	
	print("Look at x1 variable")

	FirstVar=ModelFile.loc[:,"x1"].values.reshape(-1,1)
	
	BinTwo=int(input("How many bins for Historgram:"))
	
	clrTwo=input("What color for graph:")
	
	Rg.hist(FirstVar,BinTwo,clrTwo.lower())
	
	LogFirstVar=input("Do you want to log this variable, Yes or No:")
	
	try:  ######## Analysis of First Independant Variable and decide to normalize to Test and Predicted set
		if LogFirstVar=="Yes":
			Values=ModelFile.loc[:,"x1"].values
			MinVal=abs(Values.min())
			LogX1=np.log((Values+(MinVal+int(1)))).reshape(-1,1)
			PredValues=FinalFile.loc[:,"x1"].values
			PMinVal=abs(PredValues.min())
			PLogX1=np.log((PredValues+(PMinVal+int(1)))).reshape(-1,1)
			ModelFile.loc[:,"LogX1"]=LogX1
			FinalFile.loc[:,"LogX1"]=PLogX1
			Rg.hist(LogX1,BinTwo,clrTwo.lower())
		else:
			print("No Normalize Variable were added for x1")
	except:
		sys.exit("Wrong Inputs")
		
	print("Look at x2 variable")

	SecondVar=ModelFile.loc[:,"x2"].values.reshape(-1,1)
	
	BinThree=int(input("How many bins for Historgram:"))
	
	clrThree=input("What color for graph:")
	
	Rg.hist(SecondVar,BinThree,clrThree.lower())
	
	LogSecondVar=input("Do you want to log this variable, Yes or No:")
	
	try:######## Analysis of Second Independant Variable and decide to normalize to Test and Predicted set
		if LogSecondVar=="Yes":
			Values=ModelFile.loc[:,"x2"].values
			LogX2=np.log(Values).reshape(-1,1)
			PredValues=FinalFile.loc[:,"x2"].values
			PLogX2=np.log(PredValues).reshape(-1,1)
			ModelFile.loc[:,"LogX2"]=LogX2
			FinalFile.loc[:,"LogX2"]=PLogX2
			Rg.hist(LogX2,BinThree,clrThree.lower())
		else:
			print("No Normalize Variable were added for x2")
	except:
		sys.exit("Wrong Inputs")
		
	print(ModelFile.corr())
	
	ColumNames=[]
	for key in ModelFile.keys():
		ColumNames.append(key)
	
	print(ColumNames)
	
	CList=",".join(ColumNames)
	
	YVar=DepVar
	
	IndepVars=input("Please choose the best two variables of "+CList+" :")#### Choose independant variables for analysis
	try:
		listVars=IndepVars.split()
		print(listVars)
		XVars=ModelFile[listVars]
		FXVars=FinalFile[listVars]
		for vars in listVars:
			print(vars)
			VarX=ModelFile.loc[:,vars].values.reshape(-1,1)
			print("This Shows the realationship with Y")
			Rg.corrscatter(VarX,YVar)
		
	except:
		sys.exit("Wrong Inputs, please type variable exactly as given with a space and no comma")
	
	return ModelFile,YVar,XVars,FinalFile,FXVars
	
	
def CrossValtest(NwFile,Ydata,Xdata,PXdata,Pdict):  ##### attempt training and test model via cross validation to generate predicted values then add to dictionary

	Iter=int(input("How many iterations to attempt cross validation to train model:")) ##### iterations for cv where best results was 100-500 iteration using x1 and logging X2 to produce RMSE 1.285
	
	scores,pred,res=Rg.crossValReg(Xdata,Ydata,Iter)
	
	Rg.predscatter(Ydata,pred)
	
	coefList=[]

	intLst=[]
	
	MaxR2=max(scores)

	for mode in res['estimator']:
		coefList.append(mode.coef_)
		intLst.append(mode.intercept_)
	
	R2dict={}

	for count,cv in enumerate(scores): ####### Find coefficents from best cvscore
		if cv not in R2dict.keys():
			R2dict[cv]={}
			R2dict[cv]=coefList[count][0,0],coefList[count][0,1],intLst[count][0]
		else:
			print("already here")
	
	xblist=np.array(R2dict[MaxR2]).flatten().tolist()
	
	xblist2=[str(xb)for xb in xblist]

	print("The best coefficents from this model is {}".format(",".join(xblist2)))
	
	PredictedVar=(PXdata.iloc[:,0]*R2dict[MaxR2][0])+(PXdata.iloc[:,1]*R2dict[MaxR2][1])+(R2dict[MaxR2][2]) ##### apply best coefficents to Predictedframe for PredValues
	
	Pdict["CrossValidation"]=PredictedVar ###add values to dictionary
	
	return Pdict
	
	

		
def GradientDesc(Pdict,Ytest,Xtest,Xprd):##### attempt training and test model via Gradient Descent  to generate predicted values then add to dictionary

	SplitSet=int(input("How do you want to split your data for trainning and testing:")) ####### By choosing split of 3500 train, 1500 test, learning rate of .01, interations of >=1000 RMSE was 1.284
	
	Lrate=float(input("Please choose learning rate:"))
	
	Loop=int(input("Choose the amount of interations:"))
	
	XtestCp=Xtest.copy()
	XtestCp.loc[:,"Intercept"]=int(1)
	col=XtestCp.shape[1]
	strstp=SplitSet
	XMatrix=np.matrix(XtestCp.values)
	XprdCp=Xprd.copy()
	XprdCp.loc[:,"Intercept"]=int(1)
	XPrdMatrix=np.matrix(XprdCp.values)
	lenTheta=XtestCp.shape[1]
	XGradTrain=XtestCp.iloc[:strstp,:col]
	XMatrixTrain=np.matrix(XGradTrain.values)
	XGradTest=XtestCp.iloc[strstp:,:col]
	XMatrixTest=np.matrix(XGradTest.values)
	YtestCp=Ytest.copy()
	YtestTr=YtestCp[:strstp]
	YtestTs=YtestCp[strstp:]
	GradTheta=np.matrix(np.zeros(lenTheta,dtype=float))
	coeff,mse=Rg.gradientDesc(XMatrixTrain,YtestTr,GradTheta,Lrate,Loop)
	coeff2,mse2=Rg.gradientDesc(XMatrixTest,YtestTs,GradTheta,Lrate,Loop)
	
	print("The coefficients for train set are {} and for test set {} ".format(coeff,coeff2))

	TestPredVal=np.dot(XMatrixTest,coeff2.T)
	
	RMSE=np.sqrt(mean_squared_error(YtestTs,TestPredVal))
	
	R2Test=r2_score(YtestTs,TestPredVal)
	
	print("The RMSE for test set is {}  and Rsquare  is {}".format(RMSE,R2Test))
	
	PredVal=np.dot(XMatrix,coeff2.T)
	
	BigRMSE=np.sqrt(mean_squared_error(YtestCp,PredVal))
	
	BigR2=r2_score(YtestCp,PredVal)
	
	print("The RMSE for whole set is {} and Rsquare  is {}".format(BigRMSE,BigR2))
	
	Pdict["Gradient"]=np.dot(XPrdMatrix,coeff2.T) ####### applying test coefficents of test model to Predicted frame for Predicted Values then add to Dictionary
	
	
	return Pdict
	
	
def exportPredfile(path,Pdict,PFile): ########### export file for TestPrediction.csv

	Name=input("Name of Predicted File:")
	
	NameFile=Name+".csv"
	
	NameFilePath=os.path.join(path,NameFile)
	
	Choice=input("Which model do you choose CrossValidation or Gradient: ") #### Choose which model and for sample decided on Gradient Descent
	
	
	try:
		if Choice in Pdict.keys():
			ColName=input("Name of Column before export:")
			PFile.insert(1,ColName,Pdict[Choice])
			PFile.to_csv(NameFilePath,sep=",",header=True,index=False) #### export file
			
	except:
		sys.exit("Input choice was incorrect")



def main():

	temppath=r"C:\Projects\SportsProject\Data"
	
	exportpath=r"C:\Projects\SportsProject\Data"
	
	TestFile=r"C:\Projects\SportsProject\Temp\PredictiveModeling.csv"
	
	PredFile=r"C:\Projects\SportsProject\Temp\TestCsv.csv"
	
	TstFile,Ytest,Xtest,PrdFile,Xprd=createdataframe(TestFile,PredFile)

	PredDict={}
	
	PredDict=CrossValtest(TstFile,Ytest,Xtest,Xprd,PredDict)

	PredDict=GradientDesc(PredDict,Ytest,Xtest,Xprd)
	
	exportPredfile(temppath,PredDict,PrdFile)

main()
