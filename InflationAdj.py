import argparse
import pandas as pd
import numpy as np
import sklearn as sk
import collections
from collections import Counter
from datetime import datetime as dt
import datetime as dte
import matplotlib
from matplotlib import pyplot as pt
from itertools import combinations
from itertools import product
import warnings as wn
from sklearn.metrics import mean_squared_error
import math
import os
import json



def AvgAdj(Dict,YR):
    
    return ((sum(Dict[YR].values()))/(len(Dict[YR])))
        
        
def CalcAdj(ValA,ValB):
    
    return ((ValA-ValB)/ValB)
        


class Inflate:

    def __init__(self,Dict1,Dict2):
    
        self.wgt333=.80
        
        self.wgt332=.20
        
        self.Dict333=Dict1
        
        self.Dict332=Dict2
        
        MinYearOne=list(Dict1.keys())[0]
        
        self.MinYrOne=MinYearOne
        
        MaxYearOne=list(Dict1.keys())[-1]
        
        self.MaxYrOne=MaxYearOne
        
        MinYearTwo=list(Dict2.keys())[0]
        
        self.MinYrTwo=MinYearTwo
        
        MaxYearTwo=list(Dict2.keys())[-1]
        
        self.MaxYrTwo=MaxYearTwo
    
        self.floor333=sum(Dict1[MinYearOne].values())/len(Dict1[MinYearOne])
        
        self.ceiling333=sum(Dict1[MaxYearOne].values())/len(Dict1[MaxYearOne])
        
        self.floor332=sum(Dict2[MinYearTwo].values())/len(Dict2[MinYearTwo])
        
        self.ceiling332=sum(Dict2[MaxYearTwo].values())/len(Dict2[MaxYearTwo])
        
        
    
    def PriceAdj(self,yr,price):
    
        DictA=self.Dict333
        
        DictB=self.Dict332
        
        MaxValA=self.ceiling333
        
        MaxValB=self.ceiling332
    
        if (int(yr) < int(self.MinYrTwo)) and (int(yr) < int(self.MinYrOne)) :
        
            Val333=self.floor333
            
            Val332=self.floor332
            
        elif (int(yr)< int(self.MinYrOne)) and (int(yr)>= int(self.MinYrTwo)):
        
            Val333 =self.floor333
            
            Val332=AvgAdj(DictB,yr)
            
        else:
            Val332=AvgAdj(DictB,yr)
            
            Val333=AvgAdj(DictA,yr)
        
        Adj333=CalcAdj(MaxValA,Val333)
            
        Adj332=CalcAdj(MaxValB,Val332)
        
        CalcWt333=Adj333*self.wgt333
        
        CalcWt332=Adj332*self.wgt332
        
        NewPrice=price*(1+(CalcWt333+CalcWt332))
        
        return NewPrice
        
        
        
        