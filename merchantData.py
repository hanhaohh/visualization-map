
# coding: utf-8

# In[16]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import text


# In[37]:

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
engine = create_engine('postgresql://appcard@localhost:6427/AppCardDB')
Session = scoped_session(sessionmaker(bind=engine))
s = Session()
result = s.execute(" select mb.country, mb.state, m.name, mb.name,                    mb.address_1, mb.address_2, mb.city, mb.state, mb.zip from merchant_branches as mb join                   merchant_branches_activation_date as mbad on mbad.branch_id = mb.branch_id join merchants m on                   m.merchant_id = mbad.merchant_id where mbad.active = true;")
res=result.fetchall()


# In[145]:

df = pd.DataFrame(res)
store_data = pd.DataFrame(df.groupby(1).size()[1:])


# In[159]:

states = (pd.read_csv("ab.csv",sep='\t'))


# In[190]:

final_data = states.join(store_data,on="State Abbreviation").fillna(0)
final_data = final_data.drop("State Abbreviation",1)


# In[191]:

final_data.index = final_data["State Name"]
final_data = final_data.drop("State Name",1)


# In[192]:

ind = ["".join(i.split(' ')) for i in final_data.index ]


# In[193]:

final_data.index=ind
final_data=final_data[:]
final_data


# In[210]:

final_data.columns=["number"]
final_data.index.name="state"
li=[]
for i in final_data["number"]:
    li.append((int(i)))
final_data["number"]=li


# In[213]:

final_data.to_csv("store.csv")





