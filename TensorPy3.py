import pandas as pd
import numpy as np
import sklearn as sk
from sklearn import linear_model
from sklearn.impute import SimpleImputer
from sklearn import metrics
import scipy.stats as sci
import tensorflow as tf
from tensorflow import keras
import sys
import os
import skimage
from skimage import io
import matplotlib
from matplotlib import pyplot as pt


PowerFile=r"C:\Projects\MachineLearning\Temp\powerconsumption.txt" #### Import File path

PowerFile2=r"C:\Projects\MachineLearning\Temp\powerconsumption2.csv" #### Export File path

File=pd.read_csv(PowerFile,sep=";",header=0)


print(File.head(50))


for i in range(2,8): #### Process to replace ? for NAN
    File.iloc[:,i]=File.iloc[:,i].str.replace("?","NAN").astype(float)


print(File.describe())


imp=SimpleImputer(missing_values=np.nan,strategy="mean") #### Applied Inputer to replace missing value with mean

imp=imp.fit(File.iloc[:,2:9])

File.iloc[:,2:9]=imp.transform(File.iloc[:,2:9])


print(File.describe())

VarNames=[]

Rename={} ##### Process to create key to rename variable to make it easier to name
for key in File.keys():
    print("The column name is ",key)
    Rename[key]={}
    NewVar=str(input("What is the new name:"))
    Rename[key]=NewVar
    VarNames.append(key)

print(Rename)

File.rename(columns=Rename,inplace=True)

##### Create Fourth Variable and exit out when renaming variables are incorrectly
try:
    File["SMeterFour"]=(File.loc[:,Rename[VarNames[2]]]*float(16.667))-File.loc[:,Rename[VarNames[-3]]]-File.loc[:,Rename[VarNames[-2]]]-File.loc[:,Rename[VarNames[-1]]]
except:
    sys.exit("Column Names were not entered Correctly")
    
print(File.corr())

#### Select Independent and  Dependent Variables

Y_data=File.iloc[:,-5]

print(Y_data)

Y_dataTp=tf.reshape(np.array(Y_data),(1,2075259))


X_data=File.iloc[:,-4:]

##### General Linear Regression Step for metrics

LinModel=linear_model.LinearRegression()

LinModel.fit(X_data,Y_data)

PredName=input("Name of Predicted Value:")

PredValues=LinModel.predict(X_data).flatten()

PredTest=LinModel.predict(X_data)

print("R Square Score:",LinModel.score(X_data,Y_data))

print("Mean Square Error:",metrics.mean_squared_error(Y_data,PredValues))

NewFile=File.copy()

NewFile.loc[:,PredName]=PredValues

NewFile.loc[:,"RegressVal"]=Y_data-NewFile.loc[:,PredName]

ErrorValues=NewFile.loc[:,"RegressVal"]**2

print(ErrorValues.mean())


###### Export dataframe with predicted values and regress values to a csv


NewFile.to_csv(PowerFile2,sep=",",index=False)

Params=[]

Slopes=LinModel.coef_


####### Initiate Tensor Flow regression analysis
##### Create a list for parameters containg coefficients and intercept

Intercept=LinModel.intercept_

Params.append(Intercept)

for slope in Slopes:
    Params.append(slope)

Params=tf.Variable(Params,np.float32)

print(Intercept,Slopes)
print(Params)



####VarOne=np.array(X_data.iloc[:,:1],np.float32)
###VarTwo=np.array(X_data.iloc[:,1:2],np.float32)
###VarThree=np.array(X_data.iloc[:,2:3],np.float32)
####VarFour=np.array(X_data.iloc[:,3],np.float32)

###def LinRegress(slp,var1=VarOne,var2=VarTwo,var3=VarThree,var4=VarFour):
    ###return slp[0]+var1*slp[1]+var2*slp[2]+var3*slp[3]+var4*slp[4]

####def loss_functionMse(Slp,Yvar=FuncVar,Xvar1=VarOne,Xvar2=VarTwo,Xvar3=VarThree,Xvar4=VarFour):
    ####TensorPred=LinRegress(Slp,Xvar1,Xvar2,Xvar3,Xvar4)

    #####return keras.losses.mse(Yvar,TensorPred)

#### Initiate Keras and optimizer while defining Linear Regression and Loss functions 

opt=keras.optimizers.Adam()

MSEList=[]

for batch in pd.read_csv(PowerFile2,chunksize=10000): ####### tensor flow in batches where variables are converted into numpy array for tensor flow processs
    VarOne=np.array(batch.iloc[:,-6],np.float32)
    VarTwo=np.array(batch.iloc[:,-5],np.float32)
    VarThree=np.array(batch.iloc[:,-4],np.float32)
    VarFour=np.array(batch.iloc[:,-3],np.float32)
    FuncVar=np.array(batch.iloc[:,-7],np.float32)

    def LinRegress(slp,var1=VarOne,var2=VarTwo,var3=VarThree,var4=VarFour):
        return slp[0]+var1*slp[1]+var2*slp[2]+var3*slp[3]+var4*slp[4]

    def loss_functionMse(Slp,Yvar=FuncVar,Xvar1=VarOne,Xvar2=VarTwo,Xvar3=VarThree,Xvar4=VarFour):
        TensorPred=LinRegress(Slp,Xvar1,Xvar2,Xvar3,Xvar4)

        return keras.losses.mse(Yvar,TensorPred)

    MSEval=opt.minimize(lambda:loss_functionMse(Params),var_list=[Params])
    print(loss_functionMse(Params).numpy())
    MSEList.append(loss_functionMse(Params).numpy())


MSEMean=tf.reduce_mean(np.array(MSEList,np.float32))

print(MSEMean)
    


