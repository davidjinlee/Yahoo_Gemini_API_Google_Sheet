#!/usr/bin/python

import pandas as pd
import pygsheets
from yahoo_oauth import OAuth2
import json
import time
import datetime
import requests

#Get Dates 
today = datetime.datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.datetime.today() - datetime.timedelta(31)).strftime('%Y-%m-%d')

#Authoize pygsheets
gc = pygsheets.authorize(service_file=FOLDER_PATH_TO_GOOGLE_JSON_FILE)
sheet_name = "GOOGLE_SHEET_NAME"

#Function To Find Uniques In List
def f7(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

#Authoize Yahoo API
oauth = OAuth2(None, None, from_file=FOLDER_PATH_TO_YAHOO_JSON_FILE)

if not oauth.token_is_valid():
    oauth.refresh_access_token()

response = oauth.session.get("https://api.admanager.yahoo.com/v1/rest/advertiser/")
data = response.content.decode("utf-8")
jdata = json.loads(data)

#Stuff for Dataframe
columns = []
data = []

#Pull report
for j in jdata['response']:

	#Get Advertiser Data
	advertiser_id = j['id']
	report_date_from = start_date
	report_date_to = today
	payload = {"cube": "performance_stats",
	           "fields": [
	               {"field": "Day"},
	               {"field": "Impressions"},
	               {"field": "Clicks"},
	               {"field": "Spend"},
	               {"field": "CTR"},
	               {"field": "Average CPC"},
	               {"field": "Average Position"},
	               {"field": "Campaign Name"},
	               {"field": "Campaign ID"},
	               {"field": "Device Type"},
	               {"field": "Advertiser Name"},
	               {"field": "Advertiser ID"}
	           ],
	           "filters": [
	               {"field": "Advertiser ID", "operator": "=", "value": advertiser_id},
	               {"field": "Day", "operator": "between", "from": report_date_from, "to": report_date_to}
	           ]}

	response = oauth.session.post("https://api.admanager.yahoo.com/v1/rest/reports/custom?reportFormat=json", json=payload)

	jdata = json.loads(response.content.decode("utf-8"))
	job_id = jdata['response']['jobId']

	#Add Lag Time For Report To Fully Generate
	time.sleep(30)

	url = "https://api.admanager.yahoo.com/v1/rest/reports/custom/{}?advertiserId={}".format(job_id, advertiser_id)
	response = oauth.session.get(url)

	
	#Append Extracted Dimensions/Values To List for DataFrame
	rdata = json.loads(response.content.decode("utf-8"))
	if 'status' in rdata['response'] and rdata['response']['status'] == 'completed':
	    report = requests.get(rdata['response']['jobResponse'])
	    jreport = report.json()
	    fields = jreport['fields']
	    rows = jreport['rows']
	    for field in fields:
	    	columns.append(field['fieldName'])
	    for row in rows:
	    	data.append(row)

#Create DataFrame
df = pd.DataFrame(data=data,columns=f7(columns))

#Upload to G-Sheet
sheet = gc.open(sheet_name)
worksheet = sheet[0]
worksheet.clear()
worksheet.set_dataframe(df,(1,1))
print("Complete")

