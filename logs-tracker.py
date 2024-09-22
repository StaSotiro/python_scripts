from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
import pandas as pd
import logging
import os
from datetime import timedelta

query = "StorageBlobLogs | limit 10" 

download_file_path = "./results/logs.csv"
credential = DefaultAzureCredential()

def runQuery(client):
    response = client.query_workspace (query=query, timespan=timedelta(days=1), workspace_id='d892cfd4-7868-4ce8-88cc-6b2fa7dced9b')
    
    logs_df=None
    # Check if the query succeeded
    if response.status == LogsQueryStatus.SUCCESS:
        # Process the result
        
        for table in response.tables:
            resp = pd.DataFrame(data=table.rows, columns=table.columns)
            
            if logs_df is None:
                logs_df = resp.copy()
            else:
                logs_df = logs_df.append(resp)
        return logs_df
    else:
        print("Query failed", response.status)
        return None

# Main function
if __name__ == "__main__":
    try:
        client = LogsQueryClient(credential)
        logs_df = runQuery(client)
        
        if logs_df is not None:
            logs_df.to_csv(download_file_path, sep=',', header=True, index=False)
            print("Wrote data")
        else:
            print("Error")
    except Exception as e: 
        print("Error ", e)
    
