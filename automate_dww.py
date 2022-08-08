# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 12:40:13 2022

@author: seanm
"""

from concurrent.futures import ThreadPoolExecutor , ProcessPoolExecutor
import pandas as pd
import pickle
import datetime
import fsspec
import sys , os
import traceback
import requests
import bs4
import html5lib

def override_where():
    """ overrides certifi.core.where to return actual location of cacert.pem"""
    # change this to match the location of cacert.pem
    return os.path.abspath(os.getcwd() + "/cacert.pem")

def get_monitoring_schedule(hyperlink):
    session = requests.Session()
    session.get('https://www9.state.nj.us/DEP_WaterWatch_public/')
    print(hyperlink + " loaded")
    try: 
        current_html = session.post(hyperlink)
        while ("You tried to reach a WaterWatch page directly from a bookmark (rather than starting from the main search page)" in current_html.text):
            session = requests.Session()
            session.get('https://www9.state.nj.us/DEP_WaterWatch_public/')   
            current_html = session.post(hyperlink)
        return current_html
    except BaseException as err:
        print(type(err),err)
        
def get_settings(lines):
    settings_dict = {}
    for line in lines:
        if '=' in line:
            settings_dict[line.split('=')[0]] = line.split('=')[1].rstrip()
    return settings_dict

def compare_dww(html_list_old,html_list_new,diff_dest,save_differences):
    import traceback


    table_differences = []
    same = 0
    different = 0
    html_dict_old = {html.url:html for html in html_list_old}
    html_dict_new = {html.url:html for html in html_list_new}
    
    for url in html_dict_new.keys():
        print_url = False
        if html_dict_old[url].url == html_dict_new[url].url:
            a_df = pd.read_html(html_dict_old[url].text)
            b_df = pd.read_html(html_dict_new[url].text)
            temp_table = [a_df[1].iloc[1,1] , url]
            ###Break up HTML into tables
            for table_a , table_b in zip(a_df,b_df):
                
                ####Check first if the two tables are just indentical most likely senario
                if table_a.equals(table_b):
                    same += 1
                    continue
                #####Next check if tables are the same length 
                #####if they are make sure the rows didn't just rearrange themselves
                elif len(table_a) == len(table_b):
                    table_aa = table_a.sort_values(by=table_a.columns.tolist()).reset_index(drop=True)
                    table_bb = table_b.sort_values(by=table_b.columns.tolist()).reset_index(drop=True)
                    if table_aa.equals(table_bb):
                        same += 1
                        continue
                else:
                #####Resize tables which are a different length
                    if len(table_a) > len(table_b):
                        table_b = table_b.reindex_like(table_a)
                    else:
                        table_a = table_a.reindex_like(table_b)
                #####Resulting tables will be different
                try:
                    print_url = True
                    different += 1
                    comparison = table_a.compare(table_b, align_axis=0)
                    comparison_str = str(comparison.rename(index={'self':'old','other':'new'}).swaplevel().sort_index())
                    temp_table.append(comparison_str)

                except IndexError:
                    pass
                except BaseException:
                    print(traceback.format_exc())
            if print_url:
                temp_table.append('\n\n\n################')
                table_differences.extend(temp_table)
                [print(x) for x in temp_table]
        else:
            print("\n\n\n\n URLs not equal!! \n\n\n\n")
            print(url)
            table_differences.append("\n\n\n\n URLs not equal!! \n\n\n\n" + url)
    print(same)
    table_differences.append(str(same) + ' tables were identical')
    print(different)
    table_differences.append(str(different) + ' tables were different')
    if bool(save_differences):
        with open(diff_dest + "\\" + str(datetime.datetime.now())[:-10].replace(':',' ') + '.txt', 'w+') as f:
            f.writelines(table_differences)

#is the program compiled?

if hasattr(sys, "frozen"):
    import certifi.core

    os.environ["REQUESTS_CA_BUNDLE"] = override_where()
    certifi.core.where = override_where

    # delay importing until after where() has been replaced
    import requests.utils
    import requests.adapters
    # replace these variables in case these modules were
    # imported before we replaced certifi.core.where
    requests.utils.DEFAULT_CA_BUNDLE_PATH = override_where()
    requests.adapters.DEFAULT_CA_BUNDLE_PATH = override_where()
    
    
    
    
###########Program######################
try:
    k = input("Press enter to start")        
    ###Load defaults from corresponding text file
    try:
        with open('settings_dww_webscraper.txt' , 'r' ) as f:
            lines = f.readlines()
            settings_dict = get_settings(lines)
    except:
        print('File "settings_dww_webscraper.txt" Does not exist')
        input("Press enter to exit")
        sys.exit(1)
    
    #### Query Drinking water watch and pickle results
    print(settings_dict)
    html_list_new_path = settings_dict['pickled_html_dict_path'] +'dww_queries_'+str(datetime.datetime.now())[:-10].replace(':',' ') + '.txt'
    if settings_dict['query_dww'].lower() == "true":
        try:
            hyperlink_df = pd.read_csv(settings_dict['hyperlink_csv_path'])
            print("Loading...")
            html_list_new = map(get_monitoring_schedule,hyperlink_df.hyperlink)
            with open(html_list_new_path, "wb") as f:
                pickle.dump(list(html_list_new),f)
            print("All hyperlinkes have been webscraped!! \nPickle has been saved!")
        except:
            print(traceback.format_exc())
            
            
    #### Compare table differences
    print("Now entering html comparison mode")
    with open(settings_dict['pickled_html_dict_path_old'], 'rb') as f:
        html_list_old = pickle.load(f)
    try:
        html_list_new
        with open(html_list_new_path, 'rb') as f:
            html_list_new = pickle.load(f)
    except NameError:
        with open(settings_dict['pickled_html_dict_path_new'] , 'rb') as f:
            html_list_new = pickle.load(f)
    
    print(html_list_new,html_list_old)
    compare_dww(html_list_old,html_list_new, settings_dict['saved_html_differences_path'],settings_dict['save_differences'])
    r = input("Tell me when to stop")
except BaseException:
    print("There was an error!!!")
    print(traceback.format_exc())
    n = input("Press enter to close")
