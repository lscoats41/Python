import pandas as pd
import numpy as np
import sklearn as sk
import os
import argparse
from sklearn import linear_model
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_val_predict
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.naive_bayes import GaussianNB
import matplotlib
from matplotlib import pyplot as pt
import math
from itertools import combinations


def pulldata(folder,fname,cols):  #### Function import data

    heartfolder=folder

    os.makedirs(heartfolder,exist_ok=True)

    heartfile=os.path.join(heartfolder,fname)

    heartData=pd.read_csv(heartfile,header=0)

    print(heartData.isna().sum())

    NewHeartData=heartData.loc[:,cols]

    return NewHeartData


def prepScale(dataPull,size): ##### Function to preprocess and scale data before Regression analysis is performed, also to return training data and test data

    heartImp=SimpleImputer()

    heartImp.fit(dataPull)

    HeartDataNP=heartImp.transform(dataPull)

    cols=dataPull.keys()

    heartDict={}

    for idx,col in enumerate (cols):
        heartDict[col]={}
        heartDict[col]=HeartDataNP[:,idx]
    

    CompHeartData=pd.DataFrame(heartDict)

    XPrep=CompHeartData.iloc[:,:-1].values
    StScaler=StandardScaler()

    StScaler.fit(XPrep)

    Xdata=StScaler.transform(XPrep)

    Ydata=CompHeartData.iloc[:,-1].values

    Xtr,Xts,Ytr,Yts=train_test_split(Xdata,Ydata,test_size=size,random_state=40)

    return Xtr,Xts,Ytr,Yts

def model_evaluation(mod,x,y): ##### Function to evaluate models by scores using KFolds and cross validation score
    
    crossv=RepeatedStratifiedKFold(n_splits=5,n_repeats=3,random_state=1)
    
    scores=cross_val_score(mod,x,y,scoring="roc_auc",cv=crossv,n_jobs=-1,error_score="raise")
    
    return scores

def createModelDict(KnnNum,rndNum,combo): ###### Function to create various Regressin model using KNN, LogisticRegression, RandomForestClassifier, and GaussianNB


    LogModels=[("LogRg",LogisticRegression())]

    KNModels=[("KNN"+str(i),KNeighborsClassifier(n_neighbors=i) )for i in range(1,KnnNum)]

    GB=[("GB",GaussianNB())]

    RandomModel=[("RD"+str(j),RandomForestClassifier(n_estimators=j)) for j in range(10,rndNum,10)]
                    
    EstimatorLst=LogModels+GB+RandomModel+KNModels

    VotingModels=[list(combo) for combo in combinations(EstimatorLst,4)]

    VotingDict={}

    for idx,model in enumerate(VotingModels):
        VotingDict["Model_"+str(idx+1)]={}
        VotingDict["Model_"+str(idx+1)]=VotingClassifier(estimators=model,voting="soft")
        
    return VotingDict
    
def findBestMod(VDict,Xdtr,Xdts,Ydtr,Ydts):  ###### Function to select the best Model using the Highes ROC_AUC score
    
    ModelScoreDict={}
    for key in VDict.keys():
        RocAucScore=model_evaluation(VDict[key],Xdtr,Ydtr)
        ModelScoreDict[key]=np.mean(RocAucScore)
    
    ScoreDict=dict(sorted(ModelScoreDict.items(),key=lambda kv:kv[1]))
    
    BestModel=list(ScoreDict.keys())[-1]

    BestVoting=VDict[BestModel]

    BestVoting=BestVoting.fit(Xdts,Ydts)

    PredictVal=BestVoting.predict(Xdts)

    tn3,fp3,fn3,tp3=confusion_matrix(Ydts,PredictVal).ravel()

    print("{} is the best model and gives a AUC score of {}".format(BestModel,round(ModelScoreDict[BestModel],2)))

    print("Best Voting Ensemble from training model is: {}".format(BestVoting))
    
    print("{} produces from test data  True Negatives:{},False Positives:{}, False Negatives:{}, True Positives: {}".format(BestModel,tn3,fp3,fn3,tp3))

    print("{} produces a Classification report as follows from test data:".format(BestModel))

    print(classification_report(Ydts,PredictVal))

    print("{} produces an accuracy score of {} from test data.".format(BestModel,round(accuracy_score(Ydts,PredictVal),2)))

    return BestVoting

def prepScaleSample(dataPull): ### Function to place in motion the execution of functions that will import and prep the data

    heartImp=SimpleImputer()

    heartImp.fit(dataPull)

    HeartDataNP=heartImp.transform(dataPull)

    cols=dataPull.keys()

    heartDict={}

    for idx,col in enumerate (cols):
        heartDict[col]={}
        heartDict[col]=HeartDataNP[:,idx]
    

    SampleData=pd.DataFrame(heartDict)
    
    SampleDataValues=SampleData.values

    StScaler=StandardScaler()

    StScaler.fit(SampleDataValues)

    DataSample=StScaler.transform(SampleDataValues)

    return DataSample


def applyModel(var,fldr,Vmod,sfile): ###### Fucntion that will utilize previous functions to apply regression models to find and predict target variable Coronary Heart Disease

    folder=fldr

    os.makedirs(folder,exist_ok=True)

    heartfile=os.path.join(folder,sfile)

    NewData=pd.read_csv(heartfile,header=0)

    ColLst=[v for v in var if v!="TenYearCHD"]

    PartData=NewData.loc[:,ColLst]

    ScaleData=prepScaleSample(PartData)

    print(ScaleData)

    VotingClass=Vmod

    predicted=VotingClass.predict(ScaleData)

    NewData["PredictVal"]=predicted

    CsvPath=os.path.join(folder,"PredictedValues.csv")

    NewData.to_csv(CsvPath,index=False)


    

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--folder',action="store",type=str,help="folder for data", default="C:\\Projects\\AzureMLTrainingCodes\\Data")
    parser.add_argument('--file',action="store",type=str,help="Heart Disease File", default="LogisticHeart.csv")
    parser.add_argument('--KeepColumns',nargs="+",action="store",type=str,help="Columns Chosen", default=["male","age","cigsPerDay","totChol","sysBP","diaBP","BMI","heartRate","glucose","TenYearCHD"])
    parser.add_argument('--splitSize',action="store",type=float,help="Size to partion train test data", default=".40")
    parser.add_argument('--neighbors',action="store",type=int,help="Numbers of neighbors in KNN", default="9")
    parser.add_argument('--randomEst',action="store",type=int,help="Numbers of estimators in RandomForestClassifier", default="80")
    parser.add_argument('--combo',action="store",type=int,help="number of models in voting estimators", default="4")
    parser.add_argument('--newfile',action="store",type=str,help="New Heart Disease File", default="RandomLogisticHrt.csv")
    
    args=parser.parse_args()
    argdict=vars(args)
    folder=argdict["folder"]
    HeartFile=argdict["file"]
    Columns=argdict["KeepColumns"]
    TrainSize=argdict["splitSize"]
    Neighbors=argdict["neighbors"]
    RandEst=argdict["randomEst"]
    NumCmb=argdict["combo"]
    Samplefile=argdict["newfile"]

    HeartData=pulldata(folder,HeartFile,Columns)
    TestX,TrainX,TrainY,TestY=prepScale(HeartData,TrainSize)
    DictVote=createModelDict(Neighbors,RandEst,NumCmb)
    VotingMod=findBestMod(DictVote,TestX,TrainX,TrainY,TestY)
    applyModel(Columns,folder,VotingMod,Samplefile)




main()
