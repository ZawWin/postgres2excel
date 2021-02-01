# -*- coding: utf-8 -*-


import sqlalchemy as sa
from pathlib import Path
import pandas as pd
from datetime import date

def get_all_sites(usr, pwd):
    """
    Setting up connections to connect to multiple postgres sites.

    In this example, I have a master database, 'shields'. I am extracting the location_id and location columns from the master database to set up the connection strings of each location.

    Return: 
        location_df : Master database table that has locationid, location name and region < dataframe >
        site_lists : the list of location ids < list type >
        site_locations: the list of the name of locations < list type >
    
    """
    shields = 'lxdbcmnp:5454/shrxprod'
    conn_string = 'postgresql://{0}:{1}@{2}'.format(usr,pwd,shields)
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    location_query = 'select distinct locationid, lower(location) as location, location_name_tableau as reportname, region as Region from shrx_location_info a left join location_region b on a.locationid = b.location_id order by location;'
    location_df = pd.read_sql(location_query,conn)
    location_df.loc[location_df['locationid']==106551,['location']]='baystate'
    location_df.loc[location_df['locationid']==201602,['location']]='cne'
    location_df.loc[location_df['locationid']==201501,['location']]='university'
    location_df.loc[location_df['locationid']==201909,['location']]='uhs'
    site_conn_string = 'postgresql://{}:{}@lxdb{}p:5454/{}prod'
    location_df['conn_string']=location_df.apply(lambda x:site_conn_string.format(usr,pwd,x['location'],x['location']),axis=1)
    location_df['conn_status']=''
    
    for index,row in location_df.iterrows():
        try:
            conn = sa.create_engine(row['conn_string']).connect()
            location_df.loc[index,'conn_status'] = 'Connected'
        except Exception as e:
            location_df.loc[index,'conn_status'] = 'Failed'
            print('Fail to connect to '+row['location']+'. Please review the connection string for the site '+ row['location']+"\n")
            
    site_lists = location_df.loc[location_df['conn_status']=='Connected']['locationid'].tolist()
    site_locations = location_df.loc[location_df['conn_status']=='Connected']['location'].tolist()
    
    print ('These are all sites that will be running. \n'+str(site_locations))
    
    return location_df,site_lists, site_locations


def exec_sql(usr, pwd,l, location_df, f):
    """
    Read the input sql file.
    param : usrname, password, location_id, location_df, input_file
    """

        
    file = open(Path(f))
    conn_str = location_df[location_df['locationid']==l]['conn_string'].item()
    engine = sa.create_engine(conn_str)
    conn= engine.connect()
    conn.execution_options(isolation_level='AUTOCOMMIT')
    s = sa.text(file.read())
    result = conn.execute(s)
    name = result.keys()
    
    df = pd.DataFrame(result.fetchall(), columns = name)
    site = location_df[location_df['locationid']==l]['location'].item()
    print(site + ' Completed.')
    return df

def gather_input():
    """
    

    Returns
    -------
    usr : str
        Username
    pwd : str
        Password
    f : str
        Input File 
    write_out : str
        Y/N to whether the data should be written out to a file
    file_type : str
        File type of the output file (.excel or .csv only)
    output_folder : str
        The file path of the output file. If empty, output will be written to the same folder as the python file.
    output_file : str
        The name of the output file
    all_sites : str
        Y/N answer to whether all sites need to be run or not.
    individual: str
        Y/N answer to whether multiple excel files need to be created per site
    """
    today = str(date.today())
    usr = input(f'\nPlease enter your username: ')
    pwd = input(f'\nPlease enter your password: ')
    f = input (f'\nPlease enter the complete path of the SQL file you want to run, including the file name with extension(.sql): ')
    write_out = input(f'\nDo you need to write the result out to a file? (Enter Y/N only): ')
    file_type=''
    output_folder=''
    output_file = ''
    if write_out=='Y':
        file_type = input(f'\nDo you want to write it to excel or csv file? (Enter excel/csv only): ')
        output_folder = input(f'\nEnter the folder you would like to write the output to: ')
        output_file = input (f'\nEnter your output file name (no .xlsx/.csv needed): ')
        output_file = output_file+'-'+today
        
    all_sites= input(f'\nAre you running this for all sites? (Enter Y/N only): ')
    individual = input(f'\nDo you want to create an individual report for each site: (Enter Y/N only): ')
    
    return usr, pwd, f, write_out, file_type, output_folder, output_file, all_sites, individual

def exec_sql_multiple_sites():
    """
    Steps
    1. Gathering user input (function: gather_input)
    2. Getting the connection strings for all sites (function: get_all_sites)
    3. Executing SQL based on user input criteria (function: exec sql)

    Returns
    1. The resulting dataframe write out to excel.
        If empty, it will be written out as empty dataframe.
    2. Error Log written out, if there is any error.
    """
    empty_df = pd.DataFrame(data = {'': ['No data available.']})  
    error_df = pd.DataFrame()
    confirm= 'N'
    while (confirm != 'Y'):
        usr, pwd,input_f, write_out, file_type, output_folder, output_file, all_sites, individual = gather_input()
        confirm = input (f'\nPlease confirm all your inputs are correct. (Enter Y/N only): ')
    location_df, site_lists, site_locations= get_all_sites(usr,pwd)
    
    final_df=pd.DataFrame()
    if (all_sites=='Y'):
        for s in site_lists:
            s_name = location_df[location_df['locationid']==s]['reportname'].item()
            region = location_df[location_df['locationid']==s]['region'].item()
            try:
                df = exec_sql(usr, pwd, s, location_df, input_f)
                if (len(df)==0):
                    df = empty_df
                df['site'] = s_name
                df['region'] = region
                if (individual == 'N'):
                    final_df = final_df.append(df)
                else:
                    individual_file = s_name+'-'+output_file
                    if file_type=='csv':
                        df.to_csv(str(Path(Path(output_folder)/Path(individual_file)))+'.csv', sep='|', index=False)
                    elif file_type == 'excel':
                        df.to_excel(str(Path(Path(output_folder)/Path(individual_file)))+'.xlsx', index=False)
            except Exception as e:
                print(f'\nError with '+s_name+'. Please check the error log at the end of the run.')
                err = str(e)
                err_df = pd.DataFrame(data={'Locaton':[s_name], 'ErrorMsg':[err]})
                error_df = error_df.append(err_df)
    else:
        site_check = input('Do you know the location id of the site(s) you want to run this SQL for? (Y/N only) : ')
        if site_check == 'Y':
            sites = input('Please enter site ID separated by space. You can enter more than one site. :')
            sites = sites.split()
            for s in sites:
                site = int(s)
                s_name = location_df[location_df['locationid']==site]['reportname'].item()
                region = location_df[location_df['locationid']==site]['region'].item()
                try:
                    df = exec_sql(usr, pwd, site,location_df, input_f)
                    if (len(df)==0):
                        df = empty_df
                    df['site'] = s_name
                    df['region'] = region
                    if (individual == 'N'):
                        final_df = final_df.append(df)
                    else:
                        individual_file = s_name+'-'+output_file
                        if file_type=='csv':
                            df.to_csv(str(Path(Path(output_folder)/Path(individual_file)))+'.csv', sep='|', index=False)
                        elif file_type == 'excel':
                            df.to_excel(str(Path(Path(output_folder)/Path(individual_file)))+'.xlsx', index=False)
                except Exception as e:
                    print(f'\nError with '+s_name+'. Please check the error at the end of the run.')
                    err = str(e)
                    err_df = pd.DataFrame(data={'Locaton':[s_name], 'ErrorMsg':[err]})
                    error_df = error_df.append(err_df)
        else:
            print ('No worry. We will ask you each site to confirm.')
            for s in site_lists:
                s_name = location_df[location_df['locationid']==s]['reportname'].item()
                region = location_df[location_df['locationid']==s]['region'].item()
                confirm_site = input('Do you want to run for '+s_name+
                                     '? (Y/N only): ')
                if (confirm_site=='Y'):
                    try:
                        df = exec_sql(usr, pwd,s,location_df,input_f)
                        if (len(df)==0):
                            df = empty_df
                        df['site'] = s_name
                        df['region'] = region
                        if (individual == 'N'):
                            final_df = final_df.append(df)
                        else:
                            individual_file = s_name+'-'+output_file
                            if file_type=='csv':
                                df.to_csv(str(Path(Path(output_folder)/Path(individual_file)))+'.csv', sep='|', index=False)
                            elif file_type == 'excel':
                                df.to_excel(str(Path(Path(output_folder)/Path(individual_file)))+'.xlsx', index=False)
                            
                    except Exception as e:
                        print(f'\nError with '+s_name+'. Please check the error at the end of the run.')
                        err = str(e)
                        err_df = pd.DataFrame(data={'Locaton':[s_name], 'ErrorMsg':[err]})
                        error_df = error_df.append(err_df)
                else:
                    print ('Skipping '+s_name)
    
    
    if (individual == 'N'):
        if file_type=='csv':
            final_df.to_csv(str(Path(Path(output_folder)/Path(output_file)))+'.csv', sep='|', index=False)
        elif file_type == 'excel':
            final_df.to_excel(str(Path(Path(output_folder)/Path(output_file)))+'.xlsx', index=False)
            
    if (len(error_df)>0):
        error_df.to_csv(str(Path(Path(output_folder)/'ErrorLog-'))+str(date.today())+'.csv', sep='|', index=False)

    return print('The run completed.')


def main():
    exec_sql_multiple_sites()
    
if __name__ == '__main__':
    main()
