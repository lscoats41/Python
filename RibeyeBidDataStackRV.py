import argparse
import pandas as pd
import numpy as np
import sklearn as sk
import collections
from collections import Counter
from datetime import datetime as dt
import datetime as dte
import time
import matplotlib
from matplotlib import pyplot as pt
import warnings as wn
from sklearn.metrics import mean_squared_error,r2_score
from sklearn.svm import SVR
from sklearn.linear_model import LinearRegression,GammaRegressor
from sklearn.model_selection import cross_val_score,cross_val_predict,train_test_split,RepeatedKFold
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import StackingRegressor
import math
import csv
import os
import shutil
import joblib as jb
import json as js
import warnings as wn
from InflationAdj import *
import sqlite3


wn.filterwarnings("ignore")




def normalizedmean(plst,low,high):  ##### function to find a mean within normal distribution by bucket
    
    Goodlst=[p for p in plst if p>=low and p<=high]
    
    return np.mean(Goodlst)
    
def JsonWrite(file,DictKy): #### write dictionary to json file

    J=js.dumps(DictKy)
    
    with open(file,"w") as jfile:
    
        jfile.write(J)
        
    jfile.close()
    
def JsonLoad(file): #### load json file into a dictionary
    
    with open(file,"r") as jfile:
    
        Dict=js.load(jfile)
        
    jfile.close()
    
    return Dict
    
    
def MinMaxRange(dtfrm,Nlst): #### function to find min/max range by NSN/NIIN
    rangeky={}
    
    for ns in Nlst:
       rangeky[ns]={}
       DataSlice=dtfrm.loc[dtfrm["NIIN"]==ns,"Quantity"]
       rangeky[ns]=int(DataSlice.min()),int(DataSlice.max())
    
    return rangeky
    
def OutLiersKey(Dfrm,Nlst,bklst,lnval):  ####function to remove any outliers in prices by buckets in NIIN/NSN
    
    Bkout={}
    for n in Nlst:
        if n not in Bkout.keys():
            Bkout[n]={}
            PData=Dfrm.loc[Dfrm["NIIN"]==n,["Buckets","TruePrice"]]
            PBuckets=PData["Buckets"].unique()
            for bk in bklst:
                if bk in PBuckets:
                    PriceSet=PData.loc[PData["Buckets"]==bk,["TruePrice"]]
                    PriceLst=PriceSet["TruePrice"]
                    if len(PriceLst)>=lnval:
                        if bk not in Bkout[n].keys():
                            Bkout[n][bk]={}
                            HighVal=PriceLst.mean()+(1.5*PriceLst.std())
                            LowVal=PriceLst.mean()-(1.5*PriceLst.std())
                            if LowVal <=0:
                                Bkout[n][bk]=.0001,HighVal
                            else:
                                Bkout[n][bk]=LowVal,HighVal
                        else:
                            HighVal=PriceLst.mean()+(1.5*PriceLst.std())
                            LowVal=PriceLst.mean()-(1.5*PriceLst.std())
                            if LowVal <=0:
                                Bkout[n][bk]=.0001,HighVal
                            else:
                                Bkout[n][bk]=LowVal,HighVal
                    else:
                        if bk not in Bkout[n].keys():
                            Bkout[n][bk]={}
                            Bkout[n][bk]=PriceLst.min(),PriceLst.max()
                else:
                    pass
                
                        
    return Bkout
    
    
def evalModelStack(X,Y,Xt,Yt): ##### function for GLM Regression Model with SVR stacking & CV based on length of data

    Xrows= X.shape[0]
    EstModList=[('glm',GammaRegressor(max_iter=1000)),('svr',SVR(kernel="linear",tol=.0001,max_iter=-1))]
    Linmodel=LinearRegression()
    
    if Xrows >=int(1000):
        model=StackingRegressor(estimators=EstModList,final_estimator=Linmodel,cv=50)
    elif Xrows >= int(500) and Xrows < int(1000):
        model=StackingRegressor(estimators=EstModList,final_estimator=Linmodel,cv=25)
    elif Xrows >= int(100) and Xrows < int(500):
        model=StackingRegressor(estimators=EstModList,final_estimator=Linmodel,cv=15)
    elif Xrows >= int(50) and Xrows < int(100):
        model=StackingRegressor(estimators=EstModList,final_estimator=Linmodel,cv=10)
    else:
        model=StackingRegressor(estimators=EstModList,final_estimator=Linmodel,cv=3)
        
    ####model=GammaRegressor(max_iter=1000)
   
    ###print(Y)
    model.fit(X,Y)
    predRes=model.predict(Xt)
    predScore=model.score(Xt,Yt)
    
    return model,predScore,predRes
    
def findGoodScore(ky,cnd): ##### function to locate NIIN/NSN models that performed well
    NIINScoreGd={}
    
    for k in ky.keys():
        if ky[k][0]>=cnd:
            NIINScoreGd[k]={}
            NIINScoreGd[k]=ky[k]
            
    NIINScoreSrt=dict(sorted(NIINScoreGd.items(),key=lambda kv:kv[1],reverse=True))
    
    BestOrder=list(NIINScoreSrt.keys())[0]
    
    print(BestOrder)
    
    count=0
    
    for niinkys in NIINScoreSrt.keys():
        count+=1
        if count<=5:
            print("NIIN {} has a R2 Score of {}".format(niinkys,round(NIINScoreSrt[niinkys][0],2)))
            
    return NIINScoreSrt

def inverseLog(xlst):  ##### to reverse log quantity values

    inverseVals=[]
    
    for idx,x in enumerate (xlst):
        inverseVals.append(int(round(math.exp(x))))
        np.array(inverseVals)
        
    
    return inverseVals
    
    
def PredictedImages(Xlst,Ylst,Prlst,name,fld,score): #### function to  print pdfs graphs 

    nowtime=dte.datetime.now()
    nowfmt=nowtime.strftime("%Y%m%d%H%M")
    newfld=fld.replace(fld.split("\\")[-1],"")
    pth=os.path.join(newfld,"PredictedImages"+str(nowfmt))
    
    if os.path.exists(pth)==False:
        os.makedirs(pth)
    else:
        pass
        
    
    idx=np.argsort(Xlst)

    Xlst=np.array(Xlst)[idx]

    Prlst=np.array(Prlst)[idx]

    Ylst=np.array(Ylst)[idx]

    GraphPth=os.path.join(pth,"NIIN"+name+".pdf")
    
    MaxY=0
    if max(Prlst) > max(Ylst):
        MaxY=max(Prlst) 
    else:
        MaxY=max(Ylst)
    
    fig=pt.figure(facecolor="white",figsize=(10,8))
    ax=fig.add_subplot(int(111),facecolor='grey',axisbelow=True)
    ax.plot(Xlst,Ylst,"blue",alpha=float(0.5),lw=2,label="Actual Quotes")
    ax.plot(Xlst,Prlst,"green",alpha=float(0.5),lw=2,label="Predicted Quotes")
    ax.set_xlabel("Quantity")
    ax.set_ylabel("Price Bid")
    ax.set_ylim(0,MaxY)
    ax.yaxis.set_tick_params(length=int(0))
    ax.yaxis.set_tick_params(length=int(0))
    ax.grid(b=True,which="major",c="white",lw=2,ls="-")
    legend=ax.legend()
    legend.get_frame().set_alpha(float(.5))
    for spine in ("top","right","bottom","left"):
        ax.spines[spine].set_visible(False)

    pt.suptitle("Actual Bids vs. Predicted Bids for NIIN {} Contract ".format(name),fontsize=12)
    pt.title("R2 Score: {}".format(round(score,2)),fontsize=10)

        ###pt.show()
    fig.savefig(GraphPth)
    
    return GraphPth


def errorfun(valOne,valTwo):  #### Mean Absolute error function

    return abs(valTwo-valOne)
    
def classify(errVal): #### function to categorize error from predicted or bucket mean to actual bid

    if errVal <= .10:
        cat="Pennies"
    elif errVal>.10 and errVal<=.50:
        cat="Cents"
    elif errVal  >.50 and errVal<=1.10:
        cat="Near Dollar"
    else:
        cat="Dollars"
        
    return cat

def CreateDataframe(rwfile,bk,jsfile,jsfileb,cntMin):  #### begin to import raw data, then clean,transform,filter, and created  dataframe

    RawData=pd.read_csv(rwfile,sep=",",header=0)
    
    RawData["NIIN"]=[str(N) for N in  RawData["NIIN"]]
    
    RawData["FSC"]=[str(F) for F in  RawData["FSC"]]
    
    RawData["Year"]=[str(dt.strptime(day,"%d-%b-%y").year) for day in  RawData["Date"]]
    
    DictA=JsonLoad(jsfile)
    
    DictB=JsonLoad(jsfileb)
    
    adjust=Inflate(DictA,DictB)
    
    PriceLst=[]
    
    for ind,price in enumerate(RawData["UnitPrice"]):
        Yr=RawData["Year"][ind]
        newprice=adjust.PriceAdj(Yr,price)
        PriceLst.append(newprice)
        
    RawData["TruePrice"]=PriceLst
    
    RawData=RawData.dropna(subset=["Quantity"])

    Buckets=[str(bk[0])]*len(RawData["Quantity"])

    for idx,qty in enumerate(RawData["Quantity"]):
        for j in range(0,len(bk)-1):
            if qty>bk[j] and qty <=bk[j+1]:
                Buckets[idx]=str(bk[j+1])
            
    RawData["Buckets"]=Buckets
    
    CleanData=RawData.dropna(subset=["Quantity","UnitPrice"])
    
    print(CleanData.describe())
    
    CleanData["QtyString"]=[str(int(q)) for q in CleanData["Quantity"]]
    
    CleanedData=CleanData[~CleanData["QtyString"].isin(["0","999999"])]
    
    print(CleanedData.describe())
    
    NIINCount=Counter(CleanedData["NIIN"])
    
    NIINLst=[nkey for nkey in NIINCount.keys() if NIINCount[nkey]>=cntMin]
    
    
    PartData=CleanedData[CleanedData["NIIN"].isin(NIINLst)]
    
    PartData["NormTotal"]=[math.log(t) for t in PartData["Quantity"]]
    
    print(PartData.keys())

    return PartData
    
def CreateKeys(Data,fldr,limVal): #### Create MinMax, NIIN/NSN Means by buckets,outliers and Dataframe dictionaries
 
    DfKeys={}
    
    NIINmean={}
    
    Modelpath={}
    
    nwfld=fldr.replace(fldr.split("\\")[-1],"")
    
    dictfldr=os.path.join(nwfld,"Dicitonary")
    
    if os.path.exists(dictfldr)==False:
        os.makedirs(dictfldr)
    else:
        pass
    
    
    NIINLst=Data["NIIN"].unique()
    
    BKlst=Data["Buckets"].unique()
    
    MinMxRg=MinMaxRange(Data,NIINLst)
    
    ####print(MinMxRg)
    
    MinMaxFile=os.path.join(dictfldr,"MinMax.json")
    
    JsonWrite(MinMaxFile,MinMxRg)
    
    outliers=OutLiersKey(Data,NIINLst,BKlst,limVal)
       
    nwfld=fldr.replace(fldr.split("\\")[-1],"")
    modfldr=os.path.join(nwfld,"Models")
    
    if os.path.exists(modfldr)==False:
        os.makedirs(modfldr)
    else:
        pass
    

    for NIINkey in NIINLst:
        DfKeys[NIINkey]={}
        NIINmean[NIINkey]={}
        Modelpath[NIINkey]={}
        modelName="Model_"+NIINkey+".sav"
        filename=os.path.join(modfldr,modelName)
        Modelpath[NIINkey]=filename
        dflst=[]
        for bkey in BKlst:
            if bkey in  outliers[NIINkey].keys():
                NIINmean[NIINkey][bkey]={}
                SubData=Data.loc[(Data["NIIN"]==NIINkey)&(Data["Buckets"]==bkey),["QuoteType","FSC","NIIN","Buckets","UnitPrice","Date","Quantity","Year","TruePrice","NormTotal"]]
                SubData["TruePrice_2"]=SubData["TruePrice"]
                PriceLst=SubData["TruePrice_2"].values
                NIINmean[NIINkey][bkey]=normalizedmean(PriceLst,outliers[NIINkey][bkey][0],outliers[NIINkey][bkey][1])
                SubData.loc[(SubData["TruePrice_2"]<outliers[NIINkey][bkey][0])|(SubData["TruePrice_2"]>outliers[NIINkey][bkey][1]),"TruePrice_2"]=NIINmean[NIINkey][bkey]
                if NIINkey in ("174537","12588581"):
                    print(NIINmean[NIINkey][bkey])
                dflst.append(SubData)
            else:
                pass
        
        DfKeys[NIINkey]=pd.concat(dflst)
        
    MeanFile=os.path.join(dictfldr,"NIINMeans.json")
        
    ModelFile=os.path.join(dictfldr,"NIINModels.json")
        
    JsonWrite(MeanFile,NIINmean)
        
    JsonWrite(ModelFile,Modelpath)
       
      
    return DfKeys,NIINmean,MinMxRg,Modelpath


def trainsaveModel(Dkey,pthky,evalNum):  ### function to execute GLM Stacking Model and store model for future predictions
    
    ScoreKey={}

    for key in Dkey.keys():
    
        tframe=Dkey[key]
        
        ModPath=pthky[key]

        TotalNormLst=tframe["NormTotal"].values.reshape(-1,1)

        PriceNormLst=tframe["TruePrice_2"].values
        
        Xtrain,Xtest,Ytrain,Ytest=train_test_split(TotalNormLst,PriceNormLst,test_size=.3,random_state=0)
        
        try:
        
            Model,ModScore,ModPred= evalModelStack(Xtrain,Ytrain,Xtest,Ytest)
        
            jb.dump(Model,ModPath)
        
            InvXtest=inverseLog(Xtest)

            ScoreKey[key]=ModScore,InvXtest,ModPred,Ytest
            
        except:
            
            print("Error in Values for NIIN {}".format(key))
            
            ####print(Xtrain,Xtest)
            
            ####print(Ytrain,Ytest)  
        
    GoodModel=findGoodScore(ScoreKey,evalNum)

    print(len(GoodModel.keys()))
    

    return GoodModel
    
def printGraphs(modky,foldr): #### print graphs to illustrated how model performs
 
    for ky in modky.keys():
    
        R2=modky[ky][0]
        
        IndLst=modky[ky][1]
        
        PrdLst=modky[ky][2]
        
        ActLst=modky[ky][3]

        PredictedImages(IndLst,ActLst,PrdLst,ky,foldr,R2)



def finalizedframe(Dfkey,fldr,Means,pthMod): #### produce final results into a dataframe for export and use by dept.

    framelst=[]

    for kys in Dfkey.keys():
        
        data=Dfkey[kys]
        
        modelpath=pthMod[kys]
        
        loadedMod=jb.load(modelpath)
        
        IndVar=data["NormTotal"].values.reshape(-1,1)
        
        data["Predicted"]=np.round(loadedMod.predict(IndVar),3)
        
        data["PredError"]=list(map(errorfun,data["TruePrice"],data["Predicted"]))
        
        data["PredCatErr"]=list(map(classify,data["PredError"]))
        
        meanlst=[0]*len(data["PredError"])
        
        for idx,bk in enumerate(data["Buckets"]):
            
            meanval=Means[kys][bk]
            
            meanlst[idx]=round(meanval,3)
            
        data["BucketMean"]=meanlst
            
        data["MeanError"]=list(map(errorfun,data["TruePrice"],data["BucketMean"]))
        
        data["MeanCatErr"]=list(map(classify,data["MeanError"]))
        
        framelst.append(data)
        
    FinalDf=pd.concat(framelst)
    
    FinalDf_B=FinalDf.drop(columns=["NormTotal","TruePrice_2"])
    
    FinalDf_B["NIIN"]=FinalDf_B["NIIN"].astype("str")
    
    finalName="BidAnalysis2.csv"
    
    finalpath=os.path.join(fldr,finalName)
    
    FinalDf_B.to_csv(finalpath,index=False,quoting=csv.QUOTE_ALL)
        
            
def CreateDatabase(fldr,dbName,dataky): #### Create a sql lite database to import data into PowerBI for data visualization

    CreateClause="CREATE TABLE IF NOT EXISTS CONTRACT_BIDS ( id INTEGER PRIMARY KEY,QuoteType text,FSC text,NIIN text,Buckets text, UnitPrice float,Date text,Quantity integer,Year integer, TruePrice float,Predict float,PredError float,PredCatErr text,BucketMean float,MeanError float,MeanCatErr text)"

    FetchClause="SELECT * from CONTRACT_BIDS"
    
    DeleteClause="DELETE FROM CONTRACT_BIDS WHERE id <= ?"
    
    InsertClause="INSERT INTO CONTRACT_BIDS VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    
    dbpath=os.path.join(fldr,dbName)
    
    if os.path.exists(dbpath)==False:
    
        conn=sqlite3.connect(dbpath)
        crsor=conn.cursor()
        crsor.execute(CreateClause)
        conn.commit()
        
        for kys in dataky.keys():
            Dframe=dataky[kys]
            Dframe.reset_index(inplace=True)
            for idx,niin in enumerate(Dframe["NIIN"]):
                insRow=Dframe["QuoteType"][idx],Dframe["FSC"][idx],niin,Dframe["Buckets"][idx],Dframe["UnitPrice"][idx],str(Dframe["Date"][idx]),Dframe["Quantity"][idx],int(Dframe["Year"][idx]),Dframe["TruePrice"][idx],Dframe["Predicted"][idx],Dframe["PredError"][idx],Dframe["PredCatErr"][idx],Dframe["BucketMean"][idx],Dframe["MeanError"][idx],Dframe["MeanCatErr"][idx]
                crsor.execute(InsertClause,insRow)
                conn.commit()
    else:
        
        conn=sqlite3.connect(dbpath)
        crsor=conn.cursor()
        crsor.execute(FetchClause)
        rows=crsor.fetchall()
        MaxId=int(len(rows)+1)
        crsor.execute(DeleteClause,(MaxId,))
        conn.commit()
        
        for kys in dataky.keys():
            Dframe=dataky[kys]
            Dframe.reset_index(inplace=True)
            for idx,niin in enumerate(Dframe["NIIN"]):
                insRow=Dframe["QuoteType"][idx],Dframe["FSC"][idx],niin,Dframe["Buckets"][idx],Dframe["UnitPrice"][idx],str(Dframe["Date"][idx]),Dframe["Quantity"][idx],int(Dframe["Year"][idx]),Dframe["TruePrice"][idx],Dframe["Predicted"][idx],Dframe["PredError"][idx],Dframe["PredCatErr"][idx],Dframe["BucketMean"][idx],Dframe["MeanError"][idx],Dframe["MeanCatErr"][idx]
                crsor.execute(InsertClause,insRow)
                conn.commit()
        
        
        
        


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--RawFile',action="store",type=str,help="Csv File Location",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Temp\\RibeyePriceData.csv")
    parser.add_argument('--Buckets',nargs="+",action="store",type=int,help="Bucket List", default=[100,500,1000,5000,10000,50000,100000,500000,1000000,5000000,10000000,50000000])
    parser.add_argument('--lengthLimit',action="store",type=int,help="Mininmum of length to find outliers",default="5")
    parser.add_argument('--OutFolder',action="store",type=str,help="Folder for output",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Data")
    parser.add_argument('--Rscore',action="store",type=float,help="Mininmum R2 score to be accepted",default=".50")
    parser.add_argument('--CountMin',action="store",type=int,help="Mininmum count of NIIN Frequency",default="20")
    parser.add_argument('--PPI333',action="store",type=str,help="PPI333 File in Json",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Data\\PPIData333.json")
    parser.add_argument('--PPI332',action="store",type=str,help="PPI332 File in Json",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Data\\PPIData332.json")
    parser.add_argument('--DbaseName',action="store",type=str,help="Name of Database",default="ContractBid.db")
    args=parser.parse_args()
    argdict=vars(args)
    ImportFile=argdict['RawFile']
    Buckets=argdict['Buckets']
    LmLen=argdict['lengthLimit']
    OutFolder=argdict['OutFolder']
    Rscore=argdict['Rscore']
    CntMin=argdict["CountMin"]
    jfilePPI333=argdict["PPI333"]
    jfilePPI332=argdict["PPI332"]
    dbaseName=argdict["DbaseName"]
    PData=CreateDataframe(ImportFile,Buckets,jfilePPI333,jfilePPI332,CntMin)
    DataKys,MeanKy,MinMaxKy,pthkey=CreateKeys(PData,OutFolder,LmLen)
    ModWins=trainsaveModel(DataKys,pthkey,Rscore)
    printGraphs(ModWins,OutFolder)
    finalizedframe(DataKys,OutFolder,MeanKy,pthkey)
    CreateDatabase(OutFolder,dbaseName,DataKys)
    

main()
    
