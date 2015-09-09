from flask import Flask

app = Flask(__name__)

from elasticsearch import Elasticsearch
import reverse_geocoder as rg
from flask import Flask,request,abort
from flask_restful import Resource, Api
import json
from collections import Counter
import numpy as np
from itertools import tee, izip
from geopy.distance import vincenty
import datetime
import pandas as pd


app = Flask(__name__)


def price_quantile(cdfs,city,price):
    temp=filter(lambda x: x['city']==city,cdfs)
    df=temp[0]['data']
    df['key']=df['key'].astype(float)
   
    df=df.sort('key')
    df=df.reset_index(drop=True)
    return df[df['key'] >= price]['quantile'].iloc[0]

def window(iterable, size):
    iters = tee(iterable, size)
    for i in xrange(1, size):
        for each in iters[i:]:
            next(each, None)
    return izip(*iters)

@app.route('/')
def home():
	return "Hello"

@app.route('/cluster_analyze',methods=['POST'])
def analyze_clusters():
	#if not request.json:
	#	abort(400)

	clusters=json.loads(request.data)['ids']	


	q={ 
	    "size":5000,
	    "query":{
	        
	    "terms" : {
	        "_id" : clusters,
	        
	    }
	},
	    "aggregations": {
	        "forces": {
	            "terms": {"field": "city"},
	            "aggregations": {
	                "prices": {
	                    "terms": {"field": "rate60"}
	                }
	            }
	        }
	    }
	   }

	es = Elasticsearch(['es_url'])
	res = es.search(body=q,index="memex_ht", doc_type='ad')
	geo=filter(lambda x: 'latitude' in x['_source'].keys(),res['hits']['hits'])
	geopts=map(lambda x:(float(x['_source']['latitude']),float(x['_source']['longitude'])),geo)
	ethnicity=filter(lambda x: 'ethnicity' in x['_source'].keys(),res['hits']['hits'])
	ethnicity=map(lambda x: str(x['_source']['ethnicity']),ethnicity)
	city=filter(lambda x: 'city' in x['_source'].keys(),res['hits']['hits'])
	city=map(lambda x: str(x['_source']['city']),city)
	ethnicity_all=dict(Counter(ethnicity))
	prices=filter(lambda x: 'rate60' in x['_source'].keys() and 'city' in x['_source'].keys(),res['hits']['hits'])
	prices=filter(lambda x: x['_source']['rate60']!='',prices)
	time=filter(lambda x: 'posttime' in x['_source'].keys(),geo)
	time_dist=map(lambda x: (x['_source']['latitude'],x['_source']['longitude'],datetime.datetime.strptime(x['_source']['posttime'], "%Y-%m-%dT%H:%M:%S").date()),time)



	imps=[] #implied travel speed
	imps2=[] #average distance between multiple posts at exact timestamp
	for item in window(sorted(time_dist,key=lambda item:item[2]),2):
	    dist=vincenty((item[0][0],item[0][1]),(item[1][0],item[1][1])).miles
	    #dist=100
	    time=abs(item[1][2]-item[0][2]).total_seconds()/3600.00
	    try:
	        imps.append(dist/time)
	    except ZeroDivisionError:
	        if dist != 0:
	            imps2.append(dist)
	        else:
	            pass


	if len(ethnicity_all)>1:
	    eth="More than one"
	else:
	    	eth="One"	

	if len(geopts)>0:
		results = rg.search(geopts) # default mode = 2
		countries=set(map(lambda x: x['cc'],results))
		states=set(map(lambda x: x['admin1'],results))
		cities=set(map(lambda x: x['name'],results))
		if len(countries)>1:
			location="International"
		elif len(countries)==1 and len(states)>1:
			location="National"
		else:
			location="Local"
	else:
		location="No information"

	    
	    

	q2={
	         "size":5000,
	    "query":{
	        
	    "terms" : {
	        "city" : list(set(city)),
	        
	    }
	},
	    "aggregations": {
	        "forces": {
	            "terms": {"field": "city"},
	            "aggregations": {
	                "prices": {
	                    "terms": {"field": "rate60"}
	                }
	            }
	        }
	    }
	}

	pres = es.search(body=q2,index="memex_ht", doc_type='ad')
	quantiles=pres['aggregations']['forces']['buckets']
	df2=pd.DataFrame(quantiles)    
	    
	hist=[]
	for i,city in enumerate(df2['key']):
	   df=pd.DataFrame(dict(df2['prices'][df2['key']==city]).values()[0]['buckets'])
	   df[['key','doc_count']]=df[['key','doc_count']].astype(float)
	   df.sort('key',inplace=True)
	   df['doc_count']=df['doc_count']/df['doc_count'].sum()
	   norm_cumul = 1.0*np.array(df['doc_count']).cumsum() 
	   df['quantile']=norm_cumul
	   hist.append({'city':city,'data':df})

	pq=[]
	raw=[]
	for item in map(lambda x:(x['_source']['city'],x['_source']['rate60']),prices):
	    try:
	        pq.append(price_quantile(hist,item[0],float(item[1])))
	        raw.append(float(item[1]))
	    except:
	        pass


	return json.dumps({'avg_price_quantile':np.mean(pq),'loc':location,'ethnicity':eth,'price_var':np.std(raw),\
		'mean_price':np.mean(raw),'implied_speed':np.mean(imps),'avg_dist_sim_posts':np.mean(imps2),'no_cities':len(cities)})




if __name__ == "__main__":
	app.run(debug=True,host='0.0.0.0')
