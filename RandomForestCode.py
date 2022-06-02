import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime as dte
from sklearn.ensemble import RandomForestRegressor as RFG
from sklearn.impute import SimpleImputer as SPI
from sklearn.metrics import mean_squared_error
import collections
from collections import Counter
import json as js
import joblib as jb
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from matplotlib import pyplot as pt
import warnings as wn




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
    
def splitdata(df,numlen):  ##### function to split data for training and testing
    
    traindata=df.iloc[:numlen,:]
    testdata=df.iloc[numlen:,:]
    
    return traindata,testdata
        
        
        
def FindBestScore(dictSc):
        
    resultdict=dictSc
    
    Sortdict=dict(sorted(resultdict.items(),key=lambda kv:kv[1]))
        
    kylst=list(Sortdict.keys())
        
    BestEst=kylst[0]
        
    return BestEst
    
def ForestPipe(trX,trY,tsX,tsY,MxRng): ####### pipeline function for RandomForestRegressor and StandardScaler for normalization to create a score and model dictionary
    limit=int(MxRng)+1
    tempscore={}
    tempmodel={}
    
    for i in range(10,limit,10):
        tempscore[str(i)]={}
        tempmodel[str(i)]={}
        RgForest=make_pipeline(StandardScaler(),RFG(n_estimators=i,criterion="mse"))
        RgForest.fit(trX,trY)
        Predlst=RgForest.predict(tsX)
        NewPredlst=Predlst.reshape(-1,1)
        score=np.sqrt(mean_squared_error(tsY,NewPredlst))
        tempscore[str(i)]=score    ##### store the RMSE(root meean square error)
        tempmodel[str(i)]=RgForest
        
    return tempscore,tempmodel
        

def PredictedImages(Ylst,Prlst,Rg,name,fld,score): #### function to  print pdfs graphs 
    
    Xlst=[x+1 for x in range(0,len(Prlst))]
    newfld=fld.replace(fld.split("\\")[-1],"")
    pth=os.path.join(newfld,"PredictedImages"+Rg)
    
    if os.path.exists(pth)==False:
        os.makedirs(pth)
    else:
        pass
        
    

    Xlst=np.array(Xlst)

    Prlst=np.array(Prlst)

    Ylst=np.array(Ylst)

    GraphPth=os.path.join(pth,Rg+"_"+name+".pdf")
    
    MaxY,MinY=0,0
    
    if max(Prlst) > max(Ylst):
        MaxY=max(Prlst) 
    else:
        MaxY=max(Ylst)
    
    if min(Prlst) > min(Ylst):
        MinY=min(Ylst)
    else:
        MinY=min(Prlst)
        
    wn.filterwarnings("ignore")
	
    fig=pt.figure(facecolor="white",figsize=(10,8))
    ax=fig.add_subplot(int(111),facecolor='grey',axisbelow=True)
    ax.plot(Xlst,Ylst,"blue",alpha=float(1.0),lw=2,label="Actual Readings")
    ax.plot(Xlst,Prlst,"green",alpha=float(1.0),lw=2,label="Predicted Readings ")
    ax.set_xlabel("Hours")
    ax.set_ylabel(name)
    ax.set_ylim(MinY,MaxY)
    ax.set_xlim(1,max(Xlst))
    ax.yaxis.set_tick_params(length=int(1))
    ax.yaxis.set_tick_params(length=int(1))
    ax.grid(b=True,which="major",c="white",lw=2,ls="-")
    legend=ax.legend()
    legend.get_frame().set_alpha(float(1.0))
    for spine in ("top","right","bottom","left"):
        ax.spines[spine].set_visible(False)

    pt.suptitle("Actual Readings vs. Predicted Readings for {} in Location {} ".format(name,Rg),fontsize=12)
    pt.title("RMSE Score: {}".format(round(score,2)),fontsize=10)

        ###pt.show()
    fig.savefig(GraphPth)
    
    return GraphPth
    

def SaveModel(mod,Loc,wxvar,fldr): #### Function to store model for future predictions

    newfld=fldr.replace(fldr.split("\\")[-1],"")
    pth=os.path.join(newfld,"Model"+Loc)
    
    if os.path.exists(pth)==False:
        os.makedirs(pth)
    else:
        pass
        
    modelpth=os.path.join(pth,wxvar+".sav")
    
    jb.dump(mod,modelpth)
    
    return modelpth
    
def PredictFunc(mdl,Xarray,varlst,ft): ##### function to exectute best model to predict future variables hours ahead

    lenvar=ft+1
    
    newpred=mdl.predict(Xarray)
    
    varlst.append(round(newpred[0],3))
    
    if len(varlst)<lenvar:
    
        newXarray=[newpred]
        
        PredictFunc(mdl,newXarray,varlst,ft)
        
    else:
    
        pass
        
    return varlst
    
    
    
def CreateDataframe(Jsfile): ###### Create a dataframe for each location from original json file and store in a dictionary

    WxDict=JsonLoad(Jsfile)
    
    timelst=[dt.fromtimestamp(tm).strftime("%m/%d/%Y %H:%M:%S") for tm in WxDict["time"]]
    
    del(WxDict["time"])
    
    LocDataFrame={}
    
    for loc in WxDict.keys():
        LocDataFrame[loc]={}
        dictloc=WxDict[loc]
        dictloc["time"]=timelst
        dftable=pd.DataFrame(dictloc)
        LocDataFrame[loc]=dftable
        
    return LocDataFrame, timelst
    
def CreateWxDataframe(DtFramekey): ###### Create 16 dataframes to each location for wxvar and future wxvar index by time

    WxDFkey={}
    
    for lkey in DtFramekey.keys():
        WxDFkey[lkey]={}
        DfLoc=DtFramekey[lkey]
        DfLoc=DfLoc.set_index("time")
        for col in DfLoc.keys():
            WxDFkey[lkey][col]={}
            tempdf={}
            tempdf[col]=DfLoc[col].values
            newdf=pd.DataFrame(tempdf)
            newdf[col+"_shift"]=newdf[col].shift(-1)
            finaldf=newdf.dropna()
            WxDFkey[lkey][col]=finaldf
            
    return WxDFkey
    
    
    
def RegressionProc(Dfwx,EstMx,part): ##### Process to execute pipeline function that will create two dictionaries for model and score of rmse

    Scorekey={}
    
    Modelkey={}
    
    for wxloc in Dfwx.keys():
        Scorekey[wxloc]={}
        Modelkey[wxloc]={}
        for wxvar in Dfwx[wxloc].keys():
            tabledf=Dfwx[wxloc][wxvar]
            splitnum=round(len(tabledf)*float(part))
            Scorekey[wxloc][wxvar]={}
            Modelkey[wxloc][wxvar]={}
            trdata,tsdata=splitdata(tabledf,splitnum)
            trdataX,trdataY=trdata.iloc[:,:-1].values,trdata.iloc[:,-1].values
            tsdataX,tsdataY=tsdata.iloc[:,:-1].values,tsdata.iloc[:,-1].values
            score,model=ForestPipe(trdataX,trdataY,tsdataX,tsdataY,EstMx)
            Scorekey[wxloc][wxvar].update(score)
            Modelkey[wxloc][wxvar].update(model)
            
    return Scorekey,Modelkey
            
    
def PrintSaveModel(scoredt,modeldt,vardf,part,lmt,fldr): ###### Locate the best model by using FindBestScore function to get the model with the lowest rmse

    BestModel={}
    
    for coord in vardf.keys():
        BestModel[coord]={}
        for var in vardf[coord].keys():
            BestModel[coord][var]={}
            dtframe=vardf[coord][var]
            numsplit=round(len(dtframe)*float(part))
            estdict=scoredt[coord][var]
            bestEst=FindBestScore(estdict)
            score=scoredt[coord][var][bestEst]
            RegFun=modeldt[coord][var][bestEst]
            train,test=splitdata(dtframe,numsplit)
            testX,testY=test.iloc[-lmt:,:-1].values,test.iloc[-lmt:,-1].values
            Predlst=RegFun.predict(testX)
            gpth=PredictedImages(testY,Predlst,coord,var,fldr,score)
            BestModel[coord][var]=SaveModel(RegFun,coord,var,fldr)
            
    return BestModel        
    
    

def CreateFutureWxVar(ModDict,loc,var,rd,ft):

    ModelPth=ModDict[loc][var]
    
    RgModel=jb.load(ModelPth)
    
    Xvar=[[rd]]
    
    lstvar=[rd]
    
    ftpred=PredictFunc(RgModel,Xvar,lstvar,ft)
    
    return ftpred
    

def ExportFutureData(plst,tmlst,fld,rg,wxvar):
    
    futureDict={}
    
    fthours=len(plst)
    
    lastime=dt.strptime(tmlst[-1],"%m/%d/%Y %H:%M:%S")
    
    futurefile=os.path.join(fld,"Future"+rg+wxvar+".csv")
    
    futureDict["DateTime"]=[(lastime+dte.timedelta(hours=i+1)).strftime("%m/%d/%Y %H:%M:%S") for i in range(0,fthours)]
    
    futureDict[wxvar]=plst
    
    futureData=pd.DataFrame(futureDict)
    
    futureData.to_csv(futurefile,index=False)
    
    
    
    









def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--RawFile',action="store",type=str,help="Json File Location",default="C:\\Projects\\TimeSeries\\Temp\\wxregion.json")
    parser.add_argument('--MaxEst',action="store",type=int,help="Maximum number for estimators in RandomForestRegression",default="100")
    parser.add_argument('--Partition',action="store",type=float,help="Percent to split for training and testing",default=".70")
    parser.add_argument('--GraphLimit',action="store",type=int,help="The last observations for Pdf Graph",default="50")
    parser.add_argument('--Hours',action="store",type=int,help="How many hours in the future",default="5")
    parser.add_argument('--Location',action="store",type=str,help="Wx station locations (pt0,pt1,pt2,pt3)",default="pt0")
    parser.add_argument('--WxVariable',action="store",type=str,help="Wx Variables(lrad,rad,d_rad,d_prate,d_snod,prate,ptype,rh,snod,cloud_cover,t,rt,vbdsf,vddsf,wdir,wspd)",default="t")
    parser.add_argument('--WxReading',action="store",type=float,help="Present Wx Variable Reading", default="5.00")
    parser.add_argument('--OutFolder',action="store",type=str,help="Folder for output",default="C:\\Projects\\TimeSeries\\Data")
    
    args=parser.parse_args()
    argdict=vars(args)
    ImportFile=argdict['RawFile']
    Est=argdict['MaxEst']
    Perc=argdict['Partition']
    Glimit=argdict['GraphLimit']
    Future=argdict['Hours']
    Locale=argdict['Location']
    WxVar=argdict['WxVariable']
    Reading=argdict['WxReading']
    OutFolder=argdict['OutFolder']
 
    DFkey,TimeLst=CreateDataframe(ImportFile)
    WxVarDF=CreateWxDataframe(DFkey)
    ScoreDict,ModelDict=RegressionProc(WxVarDF,Est,Perc)
    BestModDict=PrintSaveModel(ScoreDict,ModelDict,WxVarDF,Perc,Glimit,OutFolder)
    try:
        LstPred=CreateFutureWxVar(BestModDict,Locale,WxVar,Reading,Future)
        ExportFutureData(LstPred,TimeLst,OutFolder,Locale,WxVar)
    except Exception as e:
        print(e)
    
    

main()