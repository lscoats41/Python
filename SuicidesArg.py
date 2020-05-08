import pandas as pd
import numpy as np
import argparse
import sklearn as sk
from sklearn import linear_model
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import scale
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
import scipy.stats as sci
import tensorflow as tf
import sys
import os
import skimage
from skimage import io
import matplotlib
from matplotlib import pyplot as pt
import seaborn as sns
from azure.storage.blob import BlockBlobService
from sklearn import datasets




def DescripMan(File): ######## This Beginning process to Import Data, Get Information about data, Rename Columns, and Aggregate Data by Country Year Sex

    RawData=File

    DeathData=pd.read_csv(RawData,header=0)

    DeathData.rename(columns={"suicides_no":"deaths","suicides/100k pop":"deathsRt"," gdp_for_year ($) ":"GDPYear","gdp_per_capita ($)":"GDPCap"},inplace=True)

    DeathData["GDPYear"]=DeathData["GDPYear"].str.replace(",","").astype(float)

    print(DeathData.head(50))
    print(DeathData.info())
    print(DeathData.describe())

    indexlst=[i for i in range(0,len(DeathData["country"]))]

    DeathData.index=indexlst

    SumDeaths=DeathData.groupby(["country","year","sex"])[["deaths","population"]].sum()

    GDP=DeathData.groupby(["country","year","sex"])[["GDPCap","GDPYear"]].mean()

    SumDeaths.index=SumDeaths.index.set_names(["country","year","sex"])

    SumDeaths.reset_index(inplace=True)

    SumDeaths["deathsRt"]=(SumDeaths.loc[:,"deaths"]/SumDeaths.loc[:,"population"])*int(100000)

    GDP.index=GDP.index.set_names(["country","year","sex"])

    GDP.reset_index(inplace=True)

    AggData=pd.merge(SumDeaths,GDP,how="inner",left_on=["country","year","sex"],right_on=["country","year","sex"])

    print(AggData)

    Suicides=[]

    for row in AggData["deathsRt"]:####### Categorize if Country is Suicidal by deaths Rates average.
        if row > AggData["deathsRt"].mean():
            Suicides.append(int(1))
        else:
            Suicides.append(int(0))
    AggData["Suicidal"]=Suicides
        
    Continent=[]

    for country in AggData["country"]:###### Create A Region groups for countries to execute subsets in future
        if country in ["United States","Canada","Mexico"]:Continent.append("North America")
        elif country in ['Albania','Armenia','Bulgaria','Belarus','Azerbaijan','Bosnia and Herzegovina','Croatia','Czech Republic','Estonia','Georgia','Hungary','Latvia','Slovakia','Ukraine','Slovenia','Lithuania','Montenegro','Poland','Romania','Russian Federation','Serbia']:
            Continent.append("Eastern Europe")
        elif country in ['Austria','Belgium','Denmark','Norway','Malta','San Marino','Finland','France','Iceland','Ireland','Germany','Greece','Italy','Luxembourg','United Kingdom','Netherlands','Portugal','Spain','Sweden','Switzerland']:
            Continent.append("Western Europe")
        elif country in ['Antigua and Barbuda','Bahamas','Belize','Barbados','Nicaragua','Costa Rica','Trinidad and Tobago','Grenada','Saint Vincent and Grenadines','Saint Kitts and Nevis','Saint Lucia','El Salvador','Guatemala','Jamaica','Dominica','Puerto Rico','Saint Kitts and Nevis','Cuba']:
            Continent.append("Central America")
        elif country in ['Argentina','Brazil','Chile','Colombia','Ecuador','Aruba','Guyana','Panama','Uruguay','Paraguay','Suriname']:
            Continent.append("South America")
        elif country in ['Oman','Israel','Bahrain','Qatar','Cabo Verde','Kuwait','Mauritius','United Arab Emirates','Cyprus','Seychelles','Turkey','South Africa']:
            Continent.append("Middle East/Africa")  
        elif country in ['Fiji','Kazakhstan','Mongolia','Kiribati','Maldives','Macau','Kyrgyzstan','Uzbekistan','Australia','Republic of Korea','New Zealand','Philippines','Sri Lanka','Thailand','Japan','Turkmenistan','Singapore']:
            Continent.append("Asia Pacific")
        else:
            Continent.append("Other")

    AggData["Region"]=Continent

    return AggData

#### Process functions to transform selected data variable in exploratory function
def RegData(Data,Var,BN): #### No Transforming of variable

    RegVar=Data[Var].values
    RegVar=RegVar.reshape(-1,1)
    bins=int(BN)
    n,bins,patches=pt.hist(RegVar,bins,facecolor="green",alpha=.5)
    pt.title("Regular "+Var)   
    pt.show()

    return RegVar


def NormalTrans(Data,Var,BN): ##### Normalizing Data variable

    NormVar=Data[Var].values
    NormVar=NormVar.reshape(-1,1)
    NormVar=(NormVar-NormVar.mean())/NormVar.std()
    bins=int(BN)
    n,bins,patches=pt.hist(NormVar,bins,facecolor="blue",alpha=.5)
    pt.title("Normalized "+Var) 
    pt.show()

    return NormVar

    

def LogTrans(Data,Var,BN): ##### Log Data Variable

    TestVar=Data[Var].values

    if TestVar.min()<=int(0):
        LogVar=np.log(TestVar + (TestVar.min()+1))
        LogVar=LogVar.reshape(-1,1)
        bins=int(BN)
        n,bins,patches=pt.hist(LogVar,bins,facecolor="red",alpha=.5)
        pt.title("Log "+Var)
        pt.show()
    else:
        LogVar=np.log(TestVar)
        LogVar=LogVar.reshape(-1,1)
        bins=int(BN)
        n,bins,patches=pt.hist(LogVar,bins,facecolor="red",alpha=.5)
        pt.title("Log "+Var)
        pt.show()

    return LogVar


def Explore(ManData,gnd,Area): #### Exploration Process and selection of variables with transformation process

    MData=ManData[(ManData["sex"]==gnd)]


    MDataRg=ManData[(ManData["sex"]==gnd)& (ManData["Region"]==Area)]

    print(MDataRg.head(50))

    Cnts=tuple(MDataRg["country"].unique())

    print("The countries selected in ",Area," are as follows: ",Cnts)

    CorrData=MData[["deaths","deathsRt","GDPCap","GDPYear"]]

    print(CorrData.corr())

    AnalVars=[]

    DepVar=input("Please choose the dependent variable deaths or deathsRt:")

    AnalVars.append(DepVar)

    IndVar=input("Please choose the independent variable GDPCap or GDPYear:")

    AnalVars.append(IndVar)

    CorrVals=sci.pearsonr(MData[AnalVars[0]],MData[AnalVars[1]])

    print(" The Coefficient value between ",DepVar,"and ",IndVar,"is ",CorrVals[0])
    
    print(" The P-value between ",DepVar,"and ",IndVar,"is ",CorrVals[1])

    BinNum=input("How many bins for Histogram Process:")

    LgData={}
    LgData["Suicidal"]=(MData["Suicidal"].values).reshape(-1,1)
    RgData={}
    RgData["Suicidal"]=(MDataRg["Suicidal"].values).reshape(-1,1)

    for var in AnalVars:
        Rvar=RegData(MData,var,BinNum)
        Nvar=NormalTrans(MData,var,BinNum)
        Lvar=LogTrans(MData,var,BinNum)
           
    TransDep=input("Transformed Variable Choices is Regular,Normalized,or Log for dependent variable:")

    TransInDep=input("Transformed Variable Choices is Regular,Normalized,or Log for independent variable:")

    for var in AnalVars:
        if var in ["deaths","deathsRt"] and TransDep=="Regular":
           LgData[var]={}
           LgData[var]=RegData(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=RegData(MDataRg,var,BinNum)
        elif var in ["deaths","deathsRt"] and TransDep=="Normalized":
           LgData[var]={}
           LgData[var]=NormalTrans(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=NormalTrans(MDataRg,var,BinNum)
        elif var in ["deaths","deathsRt"] and TransDep=="Log":
           LgData[var]={}
           LgData[var]=LogTrans(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=LogTrans(MDataRg,var,BinNum)
        elif var not in ["deaths","deathsRt"] and TransInDep=="Regular":
           LgData[var]={}
           LgData[var]=RegData(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=RegData(MDataRg,var,BinNum)
        elif var not in ["deaths","deathsRt"] and TransInDep=="Normalized":
           LgData[var]={}
           LgData[var]=NormalTrans(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=NormalTrans(MDataRg,var,BinNum)
        elif var not in ["deaths","deathsRt"] and TransInDep=="Log":
           LgData[var]={}
           LgData[var]=LogTrans(MData,var,BinNum)
           RgData[var]={}
           RgData[var]=LogTrans(MDataRg,var,BinNum)
        else:
            print("This is not one of the choices and please choose correctly")

            
    
    CorrTran=sci.pearsonr(LgData[IndVar].flatten(),LgData[DepVar].flatten())
     

    print(" The Coefficient value between ",DepVar,"and ",IndVar,"is ",CorrTran[0])
    
    print(" The P-value between ",DepVar,"and ",IndVar,"is ",CorrTran[1])

    CorrTranRg=sci.pearsonr(RgData[DepVar].flatten(),RgData[IndVar].flatten())

    print("The Coefficient value in",Area," between ",DepVar,"and ",IndVar,"is ",CorrTranRg[0])
    
    print("The P-value in",Area," between ",DepVar,"and ",IndVar,"is ",CorrTranRg[1])

    return MData,MDataRg,LgData,RgData,IndVar,DepVar

def LargeReg(Df,Xdf,Ydf,var,gnd): ####### Regression Analysis for Dataset by selected Gender

    BigReg=LinearRegression()
    
    PredSpaceGrph=np.linspace(min(Xdf),max(Xdf)).reshape(-1,1)

    BigReg.fit(Xdf,Ydf)

    print("The Rsquare score for",gnd," Data Analysis:",BigReg.score(Xdf,Ydf))

    print("The Coeficients for Model is",(BigReg.coef_[0,0]),"and intercept is",(BigReg.intercept_[0]))

    BigPred=BigReg.predict(Xdf).flatten()

    BigPredGrph=BigReg.predict(PredSpaceGrph)

    PredVar="Pred"+var

    Diffvar="PredRg"+var

    TransPred=input("Dependent Variable Transformed by Regular,Normalized or Log:")

    if TransPred=="Regular":

        NewDf=Df.copy()

        NewDf.loc[:,PredVar]=BigPred

        NewDf.loc[:,Diffvar]=NewDf.loc[:,var]-NewDf.loc[:,PredVar]
    elif TransPred=="Normalized":

        print(BigPred)

        NormVar=Df[var].values

        NewDf=Df.copy()
        
        NewDf.loc[:,PredVar]=(BigPred*NormVar.std())+ NormVar.mean()

        NewDf.loc[:,Diffvar]=NewDf.loc[:,var]-NewDf.loc[:,PredVar]
        
    elif TransPred=="Log":

        MinValue=BigPred.min()

        NewDf=Df.copy()
        
        NewDf.loc[:,PredVar]=np.exp(BigPred)-(MinValue+1)

        NewDf.loc[:,Diffvar]=NewDf.loc[:,var]-NewDf.loc[:,PredVar]

    pt.plot(PredSpaceGrph, BigPredGrph,color="blue",linewidth=3)

    pt.show()

    Xtr,Xts,Ytr,Yts=train_test_split(Xdf, Ydf, test_size = .40, random_state=42)

    BigReg.fit(Xtr,Ytr)

    Ypred=BigReg.predict(Xts)

    print("Train Test R^2 for",gnd,": ",BigReg.score(Xts, Yts))
    
    rmse = np.sqrt(mean_squared_error(Yts,Ypred))
    
    print("Train Test Root Mean Squared Error for",gnd,": {}",rmse)

    return NewDf

def SmallReg(Df,Xdf,Ydf,var,gnd,area): ##### Regression Analysis for dataset by Gender and Region

    BigReg=LinearRegression()
    
    PredSpaceGrph=np.linspace(min(Xdf),max(Xdf)).reshape(-1,1)

    BigReg.fit(Xdf,Ydf)

    print("The Rsquare score for",gnd,"and ", area," Data Analysis:",BigReg.score(Xdf,Ydf))

    print("Coeficients for ",gnd,"and ",area," is",(BigReg.coef_[0,0]),"and intercept is",(BigReg.intercept_[0]))

    BigPred=BigReg.predict(Xdf).flatten()

    BigPredGrph=BigReg.predict(PredSpaceGrph)

    PredVar="Pred"+var

    Diffvar="PredRg"+var

    TransPred=input("Dependent Variable Transformed by Regular,Normalized or Log:")

    if TransPred=="Regular":

        NewDfRg=Df.copy()

        NewDfRg.loc[:,PredVar]=BigPred

        NewDfRg.loc[:,Diffvar]=NewDfRg.loc[:,var]-NewDfRg.loc[:,PredVar]
        
    elif TransPred=="Normalized":

        print(BigPred)

        NormVar=Df[var].values

        NewDfRg=Df.copy()

        NewDfRg.loc[:,PredVar]=(BigPred*NormVar.std())+ NormVar.mean()

        NewDfRg.loc[:,Diffvar]=NewDfRg.loc[:,var]-NewDfRg.loc[:,PredVar]
        
    elif TransPred=="Log":

        YValues=Df[var].values

        MinValue=YValues.min()

        NewDfRg=Df.copy()
        
        NewDfRg.loc[:,PredVar]=np.exp(BigPred)-(MinValue+1)

        NewDfRg.loc[:,Diffvar]=NewDfRg.loc[:,var]-NewDfRg.loc[:,PredVar]


    pt.plot(PredSpaceGrph, BigPredGrph,color="blue",linewidth=3)

    pt.show()

    return NewDfRg
    

def RegressAnal(SData,SDataRg,LData,RData,Ivar,Dvar,Sx,Rg): ###### Regression analysis for both dataframes

    Xdata=np.empty([SData.shape[0],1])

    Ydata=np.empty([SData.shape[0],1])

    for Mkey in LData.keys():
        if Mkey == Ivar:
            Xdata=LData[Mkey]
        elif Mkey== Dvar:
            Ydata=LData[Mkey]
        else:
            print("This variable ",Mkey," is not a Regression variable")
        
    NewSData=LargeReg(SData,Xdata,Ydata,Dvar,Sx)
            
    RXdata=np.empty([SDataRg.shape[0],1])

    RYdata=np.empty([SDataRg.shape[0],1])
            
    for Mkey in RData.keys():
        if Mkey == Ivar:
            RXdata=RData[Mkey]
        elif Mkey== Dvar:
            RYdata=RData[Mkey]
        else:
            print("This variable ",Mkey," is not a Regression variable")
            
    NewSDataRg=SmallReg(SDataRg,RXdata,RYdata,Dvar,Sx,Rg)

    return NewSData,NewSDataRg


def BigKNReg(Data,Xdf,Ydf,var,Ng,Sx):######K-neighbors classifiers for large datasets

    KN=KNeighborsClassifier(n_neighbors=Ng)

    KN.fit(Xdf,Ydf)

    KNPred=KN.predict(Xdf).flatten()

    print("The confusion matrix for",Sx,"is :")
    print(confusion_matrix(Ydf,KNPred))

    print("The accuracy for",Sx,"is ",accuracy_score(Ydf,KNPred))

    print("The classification report for",Sx,"is: ")
    print(classification_report(Ydf,KNPred))

    PredVar="Pred"+var

    NwData=Data.copy()

    NwData.loc[:,PredVar]=KNPred

    Range=input("What is your range for Neighbor Analysis Loop:")

    neighbors=np.arange(1,int(Range))

    TrainAcc=np.empty(len(neighbors))

    TestAcc=np.empty(len(neighbors))

    
    for i,j in enumerate(neighbors):
        LKN=KNeighborsClassifier(n_neighbors=j)
        TrX,TsX,TrY,TsY=train_test_split(Xdf,Ydf, test_size = .30, random_state=40)
        LKN.fit(TrX,TrY)
        TrainAcc[i]=LKN.score(TrX,TrY)
        TestAcc[i]=LKN.score(TsX,TsY)

    pt.title('k-NN: Varying Number of Neighbors')
    pt.plot(neighbors, TestAcc, label = 'Testing Accuracy')
    pt.plot(neighbors, TrainAcc, label = 'Training Accuracy')
    pt.legend()
    pt.xlabel('Number of Neighbors')
    pt.show()

    return NwData,KNPred

def SmallKNReg(Data,Xdf,Ydf,var,Ng,sx,rg):######K-neighbors classifiers for large datasets and gender

    KN=KNeighborsClassifier(n_neighbors=Ng)

    KN.fit(Xdf,Ydf)

    KNPred=KN.predict(Xdf).flatten()

    print("The confusion matrix for",sx,"and ",rg,"is ",confusion_matrix(Ydf,KNPred))

    print("The accuracy for",sx,"and ",rg,"is ",accuracy_score(Ydf,KNPred))

    print("The classification report for",sx,"and ",rg,"is :")
    print(classification_report(Ydf,KNPred))

    PredVar="Pred"+var

    NwData=Data.copy()

    NwData.loc[:,PredVar]=KNPred

    return NwData,KNPred


def ChiTester(Xdf,Ydf): ### Chi Square Analysis to compare

    ChiTest=sci.chi2_contingency(pd.crosstab(Xdf,Ydf))

    return ChiTest 

def KNRegressAnal(SData,SDataRg,DataLg,DataRg,sex,Rg): #### K-neighbors analysis for two datasets

    Obs="Suicidal"

    Area=SData["Region"].values

    Country=SDataRg["country"].values

    Num=int(input("How Many neighbors for initial test:"))

    Ydata=[]

    Xarray=np.empty([SData.shape[0],1])

    for varkey in DataLg.keys():
        if varkey == Obs:
            Ydata=DataLg[varkey].flatten()
        elif varkey in ["GDPCap","GDPYear"]:
            Xarray=DataLg[varkey]
        else:
            print("This variable ",varkey," will not be used in analysis")

    RvData,PredVal=BigKNReg(SData,Xarray,Ydata,Obs,Num,sex)

    ChiTestBig=ChiTester(Ydata,Area)

    print("Chi Value between ",Obs," and regions for ", sex," is ",ChiTestBig[0],"and PValue is",ChiTestBig[1])

    ChiTestBig=ChiTester(PredVal,Area)

    print("Chi Value between Predicted",Obs," and regions for ", sex," is ",ChiTestBig[0],"and PValue is",ChiTestBig[1])
    
    YRg=[]

    XRg=np.empty([SData.shape[0],1])

    for varkey in DataRg.keys():
        if varkey == Obs:
            YRg=DataRg[varkey].flatten()
        elif varkey in ["GDPCap","GDPYear"]:
            XRg=DataRg[varkey]
        else:
            print("This variable ",varkey," will not be used in analysis")

    RvDataRg,PredValRg=SmallKNReg(SDataRg,XRg,YRg,Obs,Num,sex,Rg)

    ChiTestBig=ChiTester(YRg,Country)

    print("Chi Value between ",Obs," and countries for ", Rg," is ",ChiTestBig[0],"and PValue is",ChiTestBig[1])

    ChiTestBig=ChiTester(PredValRg,Country)

    print("Chi Value between Predicted",Obs," and countries for ", Rg,"is ",ChiTestBig[0],"and PValue is",ChiTestBig[1])


    return RvData,RvDataRg

def ExportFun(BgDf,RgDf,Fldr): ##### Final Process to export two dataframes.

    if not os.path.exists(Fldr):
        os.makedirs(Fldr)

    BigCsv=input("What is the name of Large dataset(no spaces):")

    BgCsvPath=os.path.join(Fldr,BigCsv+".csv")

    BgDf.to_csv(BgCsvPath,sep=",",header=True,index=False)

    RegCsv=input("What is the name of Regional dataset(no spaces):")

    RgCsvPath=os.path.join(Fldr,RegCsv+".csv")

    RgDf.to_csv(RgCsvPath,sep=",",header=True,index=False)




def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--RawFile',action="store",type=str,help="Csv File Location",default="C:\\Projects\MachineLearning\\Temp\\worldsuicides.csv")
    parser.add_argument('--Sex',action="store",type=str,help="male or female",default="male")
    parser.add_argument('--Region',action="store",type=str,help="Choices: North America,Asia Pacific, Eastern Europe, Western Europe, Middle East and Africa",default="North America")
    parser.add_argument('--OutFolder',action="store",type=str,help="Folder for output",default="C:\\Projects\MachineLearning\\Data")
    args=parser.parse_args()
    argdict=vars(args)
    ImportFile=argdict['RawFile']
    Gender=argdict['Sex']
    Region=argdict['Region']
    OutFolder=argdict['OutFolder']
    MapData=DescripMan(ImportFile)
    SubData,SubDataRg,LData,RgData,InVar,DeVar=Explore(MapData,Gender,Region)
    NwDf,NwDfRg=RegressAnal(SubData,SubDataRg,LData,RgData,InVar,DeVar,Gender,Region)
    FinalDf,FinalDfRg=KNRegressAnal(NwDf,NwDfRg,LData,RgData,Gender,Region)
    ExportFun(FinalDf,FinalDfRg,OutFolder)

main()
    
    


    
    





