import pandas as pd
import numpy as np
import argparse
import sklearn as sk
import RegFun as Rg
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
import sys
import os



LotteryChoices=[114,113,112,111,99,89,79,69,59,49,39,29,19,9,6,4]

initialProb= [float(choice/1000) for choice in LotteryChoices]

print(initialProb)

LenProb=len(initialProb)

def findremaining(prob,rest,leftovers):
        for i in range(12):
                actvalues=[j for j, e in enumerate(prob) if e != 0]
                teams=min(actvalues)
                rest[i][teams]=rest[i][teams]+leftovers
                prob[teams]=0

        return  rest
		
		
def NextRoundProb(IProb,CondProb,NxtRdProb,X):
	RemoveProb=IProb[:X]+[0.]+IProb[X+1:]
	SumProb=sum(RemoveProb)
	NewCondProb=[(CondProb[X]*z)/(SumProb) for z in RemoveProb]
	NxtRdProb=[x+y for x,y in zip(NxtRdProb,NewCondProb)]

	return RemoveProb,NewCondProb,NxtRdProb


remainprobs=16*[12*[0]]



RoundTwo=[0]*LenProb

RoundThree=[0]*LenProb

RoundFour=[0]*LenProb

RoundFive=[0]*LenProb






for i in range(0,LenProb):
	RemoveOne,condProbOne,RoundTwo=NextRoundProb(initialProb,initialProb,RoundTwo,i)
	for j in range(0,LenProb):
		if j!=i:
			RemoveTwo,condProbTwo,RoundThree=NextRoundProb(RemoveOne,condProbOne,RoundThree,j)
		for k in range(0,LenProb):
			if (k!=j) and (k!=i):
				RemoveThree,condProbThree,RoundFour=NextRoundProb(RemoveTwo,condProbTwo,RoundFour,k)
			else:
				print("Got Here")
			for l in range(0,LenProb):
				if (l!=k) and (l!=j) and (l!=i):
					RemoveFour,condProbFour,RoundFive=NextRoundProb(RemoveThree,condProbThree,RoundFive,l)
                                        
                                                                        
          
        



                
             
						
	
print(condProbOne)

print(RoundTwo)

print(RoundThree)

print(RoundFour)

print(RoundFive)
