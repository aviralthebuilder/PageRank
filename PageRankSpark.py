#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.memory=8G pyspark-shell'

spark = (SparkSession
         .builder
         .master("local[*]")
         .getOrCreate())


# In[24]:


# the first step is to read in the files

initialData = sc.textFile("web-Google.txt")
initialData = initialData.map(lambda x: (x.split("\t")[0], x.split("\t")[1]))
print(initialData.collect())


# In[28]:


#second step is to create a links rdd with all the outgoing links from a page
links = initialData.reduceByKey(lambda x,y: str(x) + " "+ str(y))
print(links.collect())


# In[33]:


#third step is to initialize a ranks rdd with all the ranks and the ranks it will give to 
#the outgoing links
ranks = links.map(lambda x: (x, 1)).map(lambda x: (x[0][0], x[0][1],1, 1 / len(x[0][1].split(" ")))) 
print(ranks.collect())


# In[2]:


def define(arr, rank):
    temp = []
    arrTemp = arr.split(" ")
    for num in arrTemp:
        temp.append((num, rank))
    return temp


# In[44]:


#producing the different ranks each page would get
trial = ranks.flatMap(lambda x: define(x[1],x[3]))
print(trial.collect())


# In[51]:


#adding it all up and introducing the damping constant
rankedSum = trial.reduceByKey(lambda x,y: x + y).map(lambda x: (x[0], x[1] * 0.85)).map(lambda x: (x[0], x[1] + 0.15) )
print(rankedSum.collect())


# In[59]:


#it is time to update the ranks
combine = ranks.join(rankedSum).sortBy(lambda a: a[0], ascending = True)
print(combine.collect())


# In[60]:


combine = combine.map(lambda x: (x[0], x[1][0], 1 , x[1][1]))
print(combine.collect())


# In[61]:


combine = combine.map(lambda x: (x[0], x[1], x[2] , x[3] / len(x[1].split(" "))))
print(combine.collect())


# In[62]:


t = ranks.sortBy(lambda a: a[0], ascending = True)
print(t.collect())


# In[3]:


import numpy as np


# In[4]:


#this was the basic one iteration of the PageRank. Now we can do it for an x amount of 
#iterations with constant updates

#rdd here is the file which has just been created by reading the file
def PageRank(rdd, iterations):
    links = rdd.reduceByKey(lambda x,y: str(x) + " "+ str(y))
    ranks = links.map(lambda x: (x, 1))                  .map(lambda x: (x[0][0], x[0][1],1, 1 / len(x[0][1].split(" ")))) 
    for i in np.arange(0, iterations):
        trial = ranks.flatMap(lambda x: define(x[1],x[3]))
        rankedSum = trial.reduceByKey(lambda x,y: x + y)                          .map(lambda x: (x[0], x[1] * 0.85))                          .map(lambda x: (x[0], x[1] + 0.15))
                              
        combined = ranks.join(rankedSum)                         .sortBy(lambda a: a[0], ascending = True)                         .map(lambda x: (x[0], x[1][0], 1 , x[1][1]))

        ranks = combined.map(lambda x: (x[0], x[1], x[2] , x[3] / len(x[1].split(" "))))
    return ranks                                                                                            
        


# In[5]:


initial = sc.textFile("web-Google.txt")
initial = initial.map(lambda x: (x.split("\t")[0], x.split("\t")[1]))


# In[6]:


tri = PageRank(initial, 10)


# In[7]:


tri.collect()


# In[ ]:




