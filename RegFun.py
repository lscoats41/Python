import numpy as np
import sklearn as sk
from sklearn import linear_model
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_val_predict
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import StandardScaler
import matplotlib
from matplotlib import pyplot as pt
import math



def hist(data,bns,color):##### Call Histogram of variable of interest
	
	n,bins,patches=pt.hist(data,bns,facecolor=color,alpha=.05)
	
	Title=input("Name of Graph:")
	
	pt.title(Title)

	pt.show()
	
	
def corrscatter(Xdt,Ydt):###### Analyze a Target variable to independent variable by a scatterplot
	
	fig,ax=pt.subplots()
	
	ax.scatter(Xdt,Ydt,edgecolors=(0,0,0))

	ax.plot([Xdt.min(),Xdt.max()],[Ydt.min(),Ydt.max()])
	
	Xlabel=input("What is the X axis:")

	ax.set_xlabel(Xlabel)
	
	Ylabel=input("What is the Y axis:")

	ax.set_ylabel(Ylabel)

	pt.show()
		

	

def predscatter(Xdt,Ydt):###### Analyze Predicted Variables to Observed Variables
	
	fig,ax=pt.subplots()
	
	ax.scatter(Xdt,Ydt,edgecolors=(0,0,0))

	ax.plot([Xdt.min(),Xdt.max()],[Xdt.min(),Xdt.max()],'k--',lw=5)

	ax.set_xlabel("Measured")

	ax.set_ylabel("Predicted")

	pt.show()
	
	
def ComputeError(X,Y,Theta,parameter):#### Execute Function return Error in gradient descent
    inner=np.power(((X*Theta.T)-Y),2,dtype=float)
    MeanError=np.sum(inner)/(2*len(X))
    return MeanError
	
	
	

def gradientDesc(K,Z,Err,Lrt,freq):###### Execute Function for gradient descent
    temp=np.matrix(np.zeros(Err.shape,dtype=float))
    param=int(Err.ravel().shape[1])
    NewError=np.zeros(freq)
    oldError=1
    TestError=0
    PredList=[]

    for i in range(freq):
        error=(K*Err.T)-Z
        if math.isclose(oldError,TestError,abs_tol=.000001)==False:
            LastFreq=i
            for j in range(param):
                Term=np.multiply(error,K[:,j])
                temp[0,j]=Err[0,j]-((Lrt/len(K))*np.sum(Term))
            Err=temp
            NewError[i]=ComputeError(K,Z,Err,param)
            if i > 0:
                oldError=NewError[i-1]
                TestError=NewError[i]
        else:
            break
                     
    
    return Err,NewError

	

def crossValReg(X,Y,iter):##### Call cross validation to train and test model

	Reg=LinearRegression()
	
	cvscores=cross_val_score(Reg,X,Y,cv=iter)
	
	predicted=cross_val_predict(Reg,X,Y,cv=iter)
	
	results=cross_validate(Reg,X,Y,cv=iter,return_estimator=True)
	
	Rsme=np.sqrt(mean_squared_error(Y,predicted))
	
	AvgCv=r2_score(Y,predicted)

	print("Through {} iterations, the R2 score is {} and Root Mean Squared Scores is {} ".format(iter,AvgCv,Rsme))
	
	return cvscores,predicted,results
	
	
	

	
	
	
	

	
