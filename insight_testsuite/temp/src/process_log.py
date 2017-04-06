#!/home/bks4line/anaconda2/bin/python
# Author: Karthik Balasubramanian


# All imports here

import pandas as pd
import numpy as np
import os
import sys


# creating global variables for input and output directory

input_output_list =  sys.argv[1:]
input_file = input_output_list[0]
hosts_file = input_output_list[1]
hours_file = input_output_list[2]
resources_file= input_output_list[3]
blocked_file = input_output_list[4]

def clean_log_file(log_file):

    """ 
    This function creates a DataFrame 
    that is used throughout the program to develop all the features.

    Args:
        log_file (text file): unstructured log file

    Returns:
        pandas DataFrame: The Dataframe is a tabular version of the file provided,
        removing unnecessary columns

    """

    http_requests = ['POST','GET','PUT','DELETE','HEAD','CONNECT']
    columns_dict = {'host':[],'timestamp':[],'zone':[],'request_type':[],'resource':[],'http_version':[],'response':[],'bytes':[]}
    with open(input_file) as f:
        content = f.readlines()

    for each_line  in content:
        each_line_list =  each_line.split(' - - ')
#     host
        columns_dict['host'].append(each_line_list[0])
    #   date
        all_other = each_line_list[1]
        start_date_char = all_other.index('[')+len('[')
        end_date_char = all_other.index(']',start_date_char)
        date_and_time_zone = all_other[start_date_char:end_date_char]
        date = date_and_time_zone.split()[0]
        time_zone = date_and_time_zone.split()[1]
        columns_dict['timestamp'].append(date)
        columns_dict['zone'].append(time_zone)
        #     lets get response and bytes details 
        all_other = all_other[end_date_char+1:]
        all_other = all_other.strip()
        
    #     lets get resources first
        
        start_resource_char = all_other.index('"')+len('"')
        end_resource_char =  all_other.index('"',start_resource_char)
        resource =  all_other[start_resource_char:end_resource_char]
        all_other = all_other[end_resource_char+1:].strip()
        response_bytes = all_other.split(" ")
    #     print resource_response_bytes
        columns_dict['response'].append(response_bytes[0])
        columns_dict['bytes'].append(response_bytes[1])
        resource = resource.strip()
        resource = resource.split()

        if len(resource)>3:
            resource_set = set(resource)
            is_http_request =  set(http_requests)
            val =list(resource_set.intersection(is_http_request))
            if len(val)>0:
                columns_dict['request_type'].append(val[0])
                resource.pop(resource.index(val[0]))
                http_version = set(resource)
                has_version_check =  set(['HTTP/1.0'])
                http_val =list(http_version.intersection(has_version_check))
                if len(http_val)>0:
                    columns_dict['http_version'].append(http_val[0])
                    resource.pop(resource.index(http_val[0]))
                    columns_dict['resource'].append(" ".join(resource))
                else:
                    columns_dict['resource'].append(" ".join(resource))
                    columns_dict['http_version'].append('HTTP/1.0')
            
        elif len(resource)==3:
            columns_dict['request_type'].append(resource[0])
            columns_dict['resource'].append(resource[1])
            columns_dict['http_version'].append(resource[2])
        elif len(resource)==2:
    #         there can be either
            resource_set = set(resource)
            is_http_request =  set(http_requests)
            val =list(resource_set.intersection(is_http_request))
            if len(val)>0:
                columns_dict['request_type'].append(val[0])
                resource.pop(resource.index(val[0]))
                if 'HTTP/1.0' in resource:
                    columns_dict['http_version'].append('HTTP/1.0')
                    columns_dict['resource'].append(' ')
                else:
                    columns_dict['resource'].append(resource[0])
                    columns_dict['http_version'].append('HTTP/1.0')
                
        elif len(resource)==1:
            if resource[0] in http_requests:
                columns_dict['request_type'].append(resource[0])
                columns_dict['resource'].append(' ')
                columns_dict['http_version'].append('HTTP/1.0')
            elif resource[0]=='HTTP/1.0':
    #              i am considering default as HEAD
                columns_dict['request_type'].append('HEAD')
                columns_dict['resource'].append(' ')
                columns_dict['http_version'].append('HTTP/1.0')
            else:
    #              i am considering default as HEAD
                columns_dict['request_type'].append('HEAD')
                columns_dict['resource'].append(resource[0])
                columns_dict['http_version'].append('HTTP/1.0')



    log_df = pd.DataFrame(columns_dict)
    log_df.timestamp = pd.to_datetime(log_df.timestamp, format='%d/%b/%Y:%H:%M:%S')
    return log_df





def top_10_active_hosts(log_df):
    
    
    """ 
    This functions finds the top ten active hosts from the log DataFrame 
    provided. It will write the output the file provided.

    Args:
        log_df (DataFrame): tabular Dataframe of logfile

    Returns:
        Returns None. The top ten active hosts as the name suggested in the
        function, will be written to an output file which is declared as
        global variable called "hosts_file"

    """
    # group by the count of occurence of unique hosts
    #  and convert into another dataframe
    most_active_host =  pd.DataFrame({'visit_count':log_df.groupby(log_df.host.str.strip()).size()}).reset_index()
    # sort by visit count in descending order
    most_active_host = most_active_host.sort_values(by ='visit_count',ascending=False).reset_index(drop=True)[:10]
    most_active_host.to_csv(sep=',', index=False,header=False,path_or_buf=hosts_file)

def top_10_resources_consuming_bandwidth(log_df):
    
    """ 
    This functions finds the top ten resources which consume most of the bandwidth.
    To Identify the resources we have to further clean and segregate new columns.
    The passed dataframe is directly changed as it will not affect the working of other
    features. 

    Args:
        log_df (DataFrame): tabular Dataframe of logfile

    Returns:
        Returns None. Top Ten Resources that consumes the most bandwidth 
        as the name suggested in the function, will be written to an output 
        file which is declared as global variable called "resources_file"

    """
    #  there are many bytes with - as values, replacing them to 0
    log_df.bytes = log_df.bytes.str.replace('-','0')
    #  converting the bytes to numeric datatype
    #  'coerce' if the value could not be parsed, then it will be set as NaN
    log_df.bytes = pd.to_numeric(log_df.bytes,errors='coerce')
    #  filling the values by 0
    log_df.bytes = log_df.bytes.fillna(0)

    
    # grouping the resource based on byte utilised.
    most_used_resource = pd.DataFrame({'total_bandwidth':log_df.bytes.groupby(log_df.resource).sum()}).reset_index()
    most_used_resource.total_bandwidth = pd.to_numeric(most_used_resource.total_bandwidth)
    #  ordering top ten elements in the resources
    most_used_resource = most_used_resource.sort_values(by='total_bandwidth',ascending=False)[:10]
    most_used_resource.total_bandwidth = most_used_resource.total_bandwidth.astype(int)
    #  converting the index.
    most_used_resource =most_used_resource.reset_index(drop=True)
    # converting to csv
    most_used_resource.resource.to_csv(sep = ',',index=False, header=False,path=resources_file)









def top_10_busiest_60_min_period(log_df):
    """ 
    This functions finds the top ten busiest one hour period which experiences most of the traffic.
    It is identified by number of visits in a particular hour by any host.
    The passed dataframe is directly changed as it will not affect the working of other
    features. 

    Args:
        log_df (DataFrame): tabular Dataframe of logfile

    Returns:
        Returns None. Top Ten busiest one hour period from the tabular data
        is written to an output file which is declared as global variable 
        called "hours_file"

    """
    # lets first sort the values by Timestamp.
    log_df = log_df.sort_values(by='timestamp',ascending=True)
    # Appending a counter column and indexing the sorted timestamp.
    time_stamp_count_df = pd.DataFrame({'count':np.ones(len(log_df))},index=log_df.timestamp)
    #  now lets group all the hosts that access the website at the same time
    #  i.e grouping by time stamps
    time_stamp_count_df = pd.DataFrame({'count': time_stamp_count_df.groupby(time_stamp_count_df.index).size() },index = time_stamp_count_df.index.unique())
    #  now lets have a sliding window of 60 minutes and count the traffic with in the given
    #  hour frame for each timestamp data point
    #  we use shift operator to make a left-closed rolling window.
    new_time_stamp_count_df =time_stamp_count_df.rolling(window='60T').sum().shift(freq='-3599s')
    new_time_stamp_count_df = new_time_stamp_count_df.sort_values(by='count',ascending=False)[:10]
    new_time_stamp_count_df.insert(0,'time_stamp',new_time_stamp_count_df.index)
    new_time_stamp_count_df = new_time_stamp_count_df.reset_index(drop=True)
    new_time_stamp_count_df.time_stamp = new_time_stamp_count_df.time_stamp.apply(lambda x: x.strftime('%d/%b/%Y:%H:%M:%S'))
    new_time_stamp_count_df.time_stamp = new_time_stamp_count_df.time_stamp.apply(lambda x: x+" -0400")
    new_time_stamp_count_df['count'] = new_time_stamp_count_df['count'].astype(int)

    #  write to csv file

    new_time_stamp_count_df.to_csv(sep = ',',index=False, header=False,path_or_buf=hours_file)





def get_time_windows(time_series):

    """ 
    This functions takes in a time_Series list as input and returns a list of timeseries
    tuples, where the first element in each tuple is a start time
    and last element in each tuple in the end time.

    This function also takes into consideration of consecutive log failures within
    log time window, which is 300 seconds.

    if consecutive log failures happens within this window, they are ignored.

    Args:
        time_series (list): a time series list

    Returns:
        Returns list[timeseries(start,end)] with which we accumulate the dataframes
        which is finally written to blocked_hosts file

    """
    
    final_ts_window_list = []
    
    for i in time_series:
        if len(final_ts_window_list)==0:
            final_ts_window_list.append((i+pd.Timedelta('1s'),i+pd.Timedelta('301s')))
            
        else:
            if i >final_ts_window_list[-1][1]:
                final_ts_window_list.append((i+pd.Timedelta('1s'),i+pd.Timedelta('301s')))
    return final_ts_window_list






def get_blocked_requests_for_all_hosts(df,blocked_final_df):


    """ 
    This function returns blocked requests for all hosts, one by one and appends to a dataframe
    It uses the helper function to identify the time window within which the blocked hosts
    are available. loops through the host data frame to accumulate the requests which are within
    five minute window


    Args:
    df (DataFrame): This data frame is sent from the blocked_hosts method, it contains the
    hosts which have failed atleast 3 times

    blocked_final_df (DataFrame) : the DataFrame to which we will be appending all the acummulated
    host requests after three consecutive log failure 
        

    Returns:
        Returns Dataframe : returns all the blocked requests for the hosts.
    """

    for each_host in (df.host.unique()):
        
        #  get df of a particular host
        each_host_df =  df[df.host==each_host]
        #  set index to timestamp
        each_host_df.index = each_host_df.timestamp
        #  create a new df with a count column initialized to 1
        count_time_stamp_window =  pd.DataFrame({'count':np.ones(len(each_host_df))},index=each_host_df.index)
        #  run a sliding window of 20 seconds
        count_time_stamp_window =  count_time_stamp_window.rolling('20s').sum()
        #  concatenate the sliding window count and the host data
        each_host_df_with_rc = pd.concat([each_host_df,count_time_stamp_window],axis=1)
        #  get only the HTTP Reply code and count
        get_consecutive_hosts =  each_host_df_with_rc.loc[:,['response','count']]
        #  convert count to integer
        get_consecutive_hosts['count'] = get_consecutive_hosts['count'].astype(int)
        # check for HTTP Request type value = '401' and failure attempts counting to 3 
        get_consecutive_hosts =  get_consecutive_hosts.loc[(get_consecutive_hosts['response'] == '401') & get_consecutive_hosts['count'].isin([1,2,3])]
#         check if 3rd value is available
        sample = get_consecutive_hosts[get_consecutive_hosts['count']==3]
        
        if sample.empty:
            continue
        else:
            ts_windows = get_time_windows(sample.index.tolist())
            for start_window,end_window in ts_windows:
                # concatenate multiple windows of 5 minute blocking request
                #  if available
                blocked_final_df = pd.concat([blocked_final_df,each_host_df[start_window:end_window]])
        
    return blocked_final_df
            















def blocked_hosts(log_df):

    """ 
    This functions finds 3 consecutive blocking requests with error code '401' from a particular
    host and logs in 5 minute active of the host after that failure period. This function has 2 helper
    functions.
    1. get_time_windows -> gets activity of the particular host in the next five minute
       time window
    2. get_all_time_stamps_for_a_host -> get all the timestamps for a particular host
       defined by a time window by get_time_windows function

    We first filter host which have failed atleast three times in any time order
    and then we will compute on those hosts to check if they are consecutive failure

    Args:
        log_df (DataFrame): tabular Dataframe of logfile

    Returns:
        Returns None. activity of the blocked host is written in a text file called
        defined in a variable called 'blocked_file'

    """

    #  get all the failed attempts logged

    failed_attemps = log_df[log_df['response']=='401']

    #  get all the failed attempts by host 
    failed_attempts_by_host = pd.DataFrame({"attemps":failed_attemps.groupby(failed_attemps.host).size()})

    #  get failed attempts by host who had failed more than 3 times
    failed_attempts_by_host = failed_attempts_by_host[failed_attempts_by_host.attemps>=3]

    #  make a copy of logdf
    final_df = log_df.copy()
    #  filter the df to contain only the hosts who had failed more than 3 times
    final_df = final_df[final_df.host.isin(failed_attempts_by_host.index.tolist())]
    #  reset index
    final_df =final_df.reset_index(drop=True)
    #  sort by host name and then by time

    final_df = final_df.sort_values(by=['host','timestamp'],ascending=[True,True])
    #  create an empty dataFrame to accumulate activity log of all the hosts after
    #  consecutive log failures

    blocked_df =  pd.DataFrame()
    #  send the empty dataframe to fill in the helper method
    blocked_df = get_blocked_requests_for_all_hosts(final_df,blocked_df)
    blocked_df = blocked_df.sort_values(by='host',ascending=True)
    blocked_df = blocked_df.reset_index(drop=True)
    blocked_df.host = blocked_df.host.apply(lambda x: x + ' - - ')
    blocked_df.timestamp = blocked_df.timestamp.apply(lambda x: x.strftime('%d/%b/%Y:%H:%M:%S'))
    blocked_df.timestamp = blocked_df.timestamp.apply(str)
    blocked_df.timestamp = blocked_df.timestamp.apply(lambda x: "["+x+" -0400]")

    blocked_df.host = blocked_df.host + blocked_df.timestamp

    blocked_df['full_resource_path'] = blocked_df['request_type'] + ' ' + blocked_df['resource'] + ' '+blocked_df['http_version']

    blocked_df = blocked_df[['host','full_resource_path','response','bytes']]
    # blocked_df.drop(['request_type','resource','http_version','timestamp','zone'],axis=1,inplace=True)
    blocked_df.to_csv(sep=' ',path_or_buf=blocked_file,header=False,index=False)
    filename =  blocked_file
    f = open(filename,'r')
    filedata = f.readlines()
    f.close()


    f2 = open(blocked_file, 'w')
    for f in filedata:
        f_upper = f[:47].replace('"','')
        f_lower = f[47:] 
        f2.write(f_upper+f_lower)
    f2.close()
    


print "==================== Cleaning ===================="
cleaned_df = clean_log_file(input_file)
print "==================== Feature 1 ===================="
top_10_active_hosts(cleaned_df)
print "==================== Feature 2 ===================="
top_10_resources_consuming_bandwidth(cleaned_df)
print "==================== Feature 3 ===================="
top_10_busiest_60_min_period(cleaned_df)
print "==================== Feature 4 ===================="
blocked_hosts(cleaned_df)
