#################### The below code gets metric details of clusters of the last info_days for clusterStates(default=['WAITING'])

import boto3
from datetime import datetime
from datetime import timedelta
import pandas as pd
import json
from math import ceil

timenow=datetime.now()
info_days=30
timenow_minus_info_days=timenow-timedelta(days=info_days)
def cluster_process (response):
    output=[]
    for cluster in response.get('Clusters', []):
        curr_out={}
        curr_out["ClusterId"]=cluster.get('Id',"")
        curr_out["NormalizedInstanceHours"]=cluster.get('NormalizedInstanceHours',None)
        timeline=cluster.get('Status',{}).get('Timeline',{})
        curr_out["ReadyDateTime"]=timeline.get("ReadyDateTime",None)
        curr_out["CreationDateTime"]=timeline.get("CreationDateTime",None)
        curr_out["EndDateTime"]=timeline.get("EndDateTime",None)
        curr_out["State"]=cluster.get("Status",{}).get("State","")
        curr_out["runtime"]=None
        curr_out["idle%"]=None
        curr_out["idle% in last 12hours"]=get_idle_stat(timenow-timedelta(hours=12),timenow,curr_out["ClusterId"])
        curr_out["idle% in last 3hours"]=get_idle_stat(timenow-timedelta(hours=3),timenow,curr_out["ClusterId"])
        end=curr_out["EndDateTime"] if bool(curr_out["EndDateTime"]) else timenow
        if bool(curr_out["EndDateTime"]) and bool(curr_out["ReadyDateTime"]):
            curr_out["runtime"]=end-curr_out["ReadyDateTime"]
        if not bool(curr_out["EndDateTime"]) and bool(curr_out["ReadyDateTime"]):
            curr_out["runtime"]=end-curr_out["ReadyDateTime"].replace(tzinfo=None)
        if bool(curr_out["ReadyDateTime"]):
            curr_out["idle%"]=get_idle_stat(curr_out["ReadyDateTime"],end,curr_out["ClusterId"])
        output.append(curr_out)
    marker=response.get('Marker', None)
    return output,marker
    
    
def trail_process(response):
    output=[]
    for event in response.get('Events',[]):
        curr_out={}
        curr_out["Username"]=event.get('Username',"")
        event_dict=json.loads(event.get('CloudTrailEvent','{}'))
        responseElements=event_dict.get('responseElements',{})
        curr_out["ClusterId"]=responseElements.get("jobFlowId","") if bool(responseElements) else "No responseElements"
        output.append(curr_out)
    token=response.get('NextToken',None)
    return output,token
    
    
def get_idle_stat(start,end,clusterid):
    client= boto3.client('cloudwatch')
    period=((end.replace(tzinfo=None)-start.replace(tzinfo=None)).days+4)*86400
    response = client.get_metric_statistics(
        Namespace='AWS/ElasticMapReduce',
        MetricName='IsIdle',
        Dimensions=[
            {
                "Name": "JobFlowId",
                "Value": clusterid
            }
        ],
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=['Average',],
    )
    if len(response["Datapoints"])==1:
        return response["Datapoints"][0]["Average"]
    return ""
    
    
emrclient = boto3.client('emr')
clusterStates=['WAITING'] # 'STARTING'|'BOOTSTRAPPING'|'RUNNING'|'WAITING'|'TERMINATING'|'TERMINATED'|'TERMINATED_WITH_ERRORS],
final_output=[]
marker=""
while True:
    response = emrclient.list_clusters(CreatedAfter=timenow_minus_info_days,ClusterStates=clusterStates,Marker=marker) if bool(marker) else emrclient.list_clusters(CreatedAfter=timenow_minus_info_days,ClusterStates=clusterStates
    output,marker=cluster_process(response)
    final_output=final_output+output
    if not bool(marker):
        break
cluster_df=pd.DataFrame(final_output).sort_values(by=['runtime'], ascending=False)


trailclient = boto3.client('cloudtrail')
lookupAttributes=[{ 'Attributekey': 'EventName',# 'EventId'|'EventName'|'ReadOnly'|'Username'|'ResourceType'|'ResourceName'|'EventSource'|'AccessKeyId',
            'AttributeValue': 'RunJobFlow'}]
final_output=[]
token=""
while True:
    response = trailclient.lookup_events(StartTime=timenow_minus_info_days,LookupAttributes=lookupAttributes,NextToken=token) if bool(token) else trailclient.lookup_events(StartTime=timenow_minus_info_days,LookupAttributes=lookupAttributes)
    output,token=trail_process(response)
    final_output=final_output+output
    if not bool(token):
        break
trail_df=pd.DataFrame(final_output)


merged_left = pd.merge(left=cluster_df, right=trail_df, how='left', left_on='ClusterId', right_on='ClusterId').sort_values(by=['NormalizedInstanceHours',], ascending=False)
cols=['Username','ClusterId','NormalizedInstanceHours','runtime','State','idle%', 'idle% in last 12hours',
    'idle% in last 3hours', 'ApproxCost', 'ReadyDateTime', 'CreationDateTime', 'EndDateTime', ] # ApproxCost
merged_left=merged_left[cols]
merged_left.to_csv('./emr_status_details_{}.csv'.format(timenow.strftime("%m-%d-%Y_%H:%M:%S")),index=False)