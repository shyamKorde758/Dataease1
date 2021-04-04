#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark
findspark.init()


# In[8]:


import pyspark
from pyspark.sql import SparkSession
spark =SparkSession.builder.getOrCreate()
df = spark.sql("select 'spark' as hello")
df.show()


# In[9]:


df=spark.read.csv("startup.csv",inferSchema=True,header=True)
df1=spark.read.parquet("consumerInternet.parquet")
df3=df.unionAll(df1)
df3.printSchema()


# In[12]:


#How many startups are there in Pune City?

df3.createOrReplaceTempView("df3")

PuneStartups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE City = 'Pune'")
PuneStartups.show()


# In[14]:


#How many startups in Pune got their Seed/ Angel Funding?

Startups = spark.sql("SELECT COUNT(Startup_Name) FROM df3 WHERE Investmentntype like 'Seed%%' AND City = 'Pune'")
Startups.show()


# In[15]:


#What is the total amount raised by startups in Pune City? Hint - use regex_replace to
get rid of null

PuneCityAmount = spark.sql("SELECT SUM(regexp_replace(Amount_in_USD, 'N/A', '00')) AS Amount FROM df3 WHERE City = 'Pune'")
PuneCityAmount.show()


# In[17]:


#What are the top 5 Industry_Vertical which has the highest number of startups in India?
TopFive = spark.sql("SELECT Industry_Vertical, COUNT(Startup_Name) FROM df3 GROUP BY Industry_Vertical ORDER BY COUNT(Startup_Name) DESC LIMIT 5")
TopFive.show()


# In[ ]:


#Find the top Investor(by amount) of each year.
Top = spark.sql("SELECT Investors_Name,DATE, FROM DF3 EXTRACT(YEAR FROM DATE) ORDER BY Amount_in_USD ") 
SELECT MAX(SALARY) FROM EMPLOYEE.
WHERE SALARY<(SELECT MAX(SALARY) FROM EMPLOYEE)


# In[ ]:





# In[ ]:





# In[ ]:




