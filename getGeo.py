
# coding: utf-8

# In[1]:

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import text
import time,json
import urllib2,urllib


# In[13]:

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
engine = create_engine('postgresql://appcard@localhost:6427/AppCardDB')
Session = scoped_session(sessionmaker(bind=engine))
s = Session()
result = s.execute(" select mb.merchant_id ,mb.country, mb.state, m.name, mb.name, mbad.activation_date,                   mb.address_1, mb.address_2, mb.city, mb.zip from merchant_branches as mb join                   merchant_branches_activation_date as mbad on mbad.branch_id = mb.branch_id join merchants m on                   m.merchant_id = mbad.merchant_id where mbad.active = true;")
res=result.fetchall()


# In[14]:

df = pd.DataFrame(res).fillna('')
df


# In[16]:

df["more"] = df[6]+df[8]+','+df[2]
li=[]
df1 = df[df['more']!=',']
df1


# In[18]:

def func(d):
    params={}
    time.sleep(2)
    params[ 'sensor' ] = "false"
    params[ 'address' ] = d
    params = urllib.urlencode( params )
    url = "http://maps.googleapis.com/maps/api/geocode/json?%s" % params
    a=urllib2.urlopen(url).read()
    aj = json.loads(a)["results"]
    if aj:
        lat= aj[0]["geometry"]['location']
        print lat,d
        li.append(lat)
    else:
        print d
        li.append(' ')

df1['more'].apply(func)


# In[19]:

lat =[]
lng=[]
for i in li:
    if i is not ' ':
        lat.append(i['lat'])
        lng.append(i['lng'])
    else:
        lat.append(' ')
        lng.append(' ')


# In[23]:

df1['lat']=lat
df1['lng']=lng


# In[24]:

df1.to_csv("store_full",sep='\t',index=False)


# In[ ]:



