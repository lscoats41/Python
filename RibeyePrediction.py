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

def BucketsCreation(lst,val): #### function to create buckets

    bucket=str(100)

    for j in range(0,len(lst)-1):
        if val>lst[j] and val<=lst[j+1]:
            bucket=str(lst[j+1])
            
    return bucket



def GoodNIINS(rwFile,fldr,DictFile): #### function to find NIINS that have models stored for predictions

    RngDict=JsonLoad(DictFile)
    
    NIINKeys=RngDict.keys()
    
    DataFile=pd.read_csv(rwFile,sep=",",header=0)

    DataFile["NIIN"]=[str(n) for n in DataFile["NIIN"]]
    
    NiinLst=DataFile["NIIN"].unique()
    
    Goodlst=[n for n in NiinLst if n in NIINKeys]
    
    Badlst= [n for n in NiinLst if n not in NIINKeys]
    
    BadDataFile=DataFile[DataFile["NIIN"].isin(Badlst)]
    
    Badfile=os.path.join(fldr,"sparseNIINS.csv")
    
    BadDataFile.to_csv(Badfile,index=False)
    
    return Goodlst,RngDict
    
    
def DFrameCreation(gdlst,rgDict,bklst,frq): #### Create dataframe with randomly generated quanitities using the json file with minmax range

    DataFrameKey={}

    for gdn in gdlst:
        DataFrameKey[gdn]={}
        Frame={}
        QtyMin=rgDict[gdn][0]
        QtyMax=rgDict[gdn][1]
        Frame["NIIN"]=[gdn]*frq
        Frame["Quantity"]=np.random.randint(int(QtyMin),int(QtyMax),int(frq))
        Frame["Buckets"]=[BucketsCreation(bklst,qval) for qval in Frame["Quantity"]]
        Frame["NormTotal"]=[math.log(q) for q in Frame["Quantity"]]
        DataTable=pd.DataFrame(Frame)
        DataFrameKey[gdn]=DataTable
        
    return DataFrameKey
    
    
def ExtractModMean(Dkey,ModDict,MeanDict): #### import NIIN/NSN model to predict bids

    Mdpth=JsonLoad(ModDict)
    
    AvgBuckets=JsonLoad(MeanDict)
    
    framelst=[]
    
    for key in Dkey.keys():
    
        datatble=Dkey[key]
        
        modelpath=Mdpth[key]
        
        loadedMod=jb.load(modelpath)
        
        Var= datatble["NormTotal"].values.reshape(-1,1)
        
        datatble["Predicted"]=np.round(loadedMod.predict(Var),3)
       
        bklst=[int(b) for b in AvgBuckets[key].keys()]
        
        srtbklst=sorted(bklst)
        
        MeanLst=[0]*len(datatble["Buckets"])
        
        for idx,bk in enumerate(datatble["Buckets"]):
            if bk in AvgBuckets[key].keys():
                MeanLst[idx]=round(AvgBuckets[key][bk],3)
            else:
                for i in range(0,len(srtbklst)-1):
                    if int(bk)>srtbklst[i] and int(bk)<=srtbklst[i+1]:
                        AvgVal=(AvgBuckets[key][str(srtbklst[i])]+AvgBuckets[key][str(srtbklst[i+1])])/2
                        MeanLst[idx]=round(AvgVal,3)
        
        datatble["BucketMeans"]=MeanLst
        
        PredLstRv=[]
        
        for idx,price in enumerate(datatble["Predicted"]): #### Used to remove negative values
            if price<float(0):
                PredLstRv.append(datatble["BucketMeans"][idx])
            else:
                PredLstRv.append(price)
        
        datatble["Predicted"]=[p for p in PredLstRv]
               
        
        framelst.append(datatble)
    
    return framelst
    
def ExportDataFrame(flst,fldr,dName): #### Export results for usage
     
    FinalFrame=pd.concat(flst)
    
    FinalFrameRV=FinalFrame.drop(columns=["NormTotal"])
    
    FileName=dName+".csv"
    
    Filepath=os.path.join(fldr,FileName)
    
    FinalFrameRV.to_csv(Filepath,index=False)
    




def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--RawFile',action="store",type=str,help="Csv File Location",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Temp\\RibeyePriceData.csv")
    parser.add_argument('--Buckets',nargs="+",action="store",type=int,help="Bucket List", default=[100,500,1000,5000,10000,50000,100000,500000,1000000,5000000,10000000,50000000])
    parser.add_argument('--OutFolder',action="store",type=str,help="Folder for output",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Data")
    parser.add_argument('--MinMax',action="store",type=str,help="NIIN MinMax Range File in Json",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Dicitonary\\MinMax.json")
    parser.add_argument('--BKMeans',action="store",type=str,help="BucketMeans File in Json",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Dicitonary\\NIINMeans.json")
    parser.add_argument('--ModelPths',action="store",type=str,help="File paths for models in Json",default="C:\\Projects\\ForcastBidding\\RibeyeProject\\Dicitonary\\NIINModels.json")
    parser.add_argument('--Iters',action="store",type=int,help="Number of iterations per contract",default="50")
    parser.add_argument('--CsvName',action="store",type=str,help="Name of Exported File",default="PredictNIINbids")
    args=parser.parse_args()
    argdict=vars(args)
    ImportFile=argdict['RawFile']
    Buckets=argdict['Buckets']
    OutFolder=argdict['OutFolder']
    MinMaxDict=argdict["MinMax"]
    MeansDict=argdict["BKMeans"]
    ModelPthDict=argdict["ModelPths"]
    Freq=argdict["Iters"]
    DFrameName=argdict["CsvName"]
    NIINLst,MinMxDic=GoodNIINS(ImportFile,OutFolder,MinMaxDict)
    DfrKey=DFrameCreation(NIINLst,MinMxDic,Buckets,Freq)
    datalst=ExtractModMean(DfrKey,ModelPthDict,MeansDict)
    ExportDataFrame(datalst,OutFolder,DFrameName)
    

main()