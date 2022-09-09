# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 12:40:13 2022

@author: seanm
"""

import pandas as pd
import sys
import pickle
import datetime
import traceback
import requests


def get_dww_endpoint(hyperlink):
    '''
    Parameters
    ----------
    hyperlink : full url to dww endpoint

    Description
    -----------
    1. starts a valid session by going to dww main page, 
    2. then goes to the provided main page 
    3. repeats 1 and 2 if there is an error
    
    Returns
    -------
    current_html : html of the dww endpoint sought after

    '''
    err_msg = '''You tried to reach a WaterWatch page directly from a bookmark 
        (rather than starting from the main search page)'''
    main_dep_page = "'https://www9.state.nj.us/DEP_WaterWatch_public/'"
    session = requests.Session()
    session.get(main_dep_page)
    print(hyperlink + " loaded")
    try: 
        current_html = session.get(hyperlink)
        while (err_msg in current_html.text):
            session = requests.Session()
            session.get(main_dep_page)   
            current_html = session.get(hyperlink)
        return current_html
    except BaseException as err:
        print(type(err),err)
        
def get_settings(lines):
    '''
    Parameters
    ----------
    lines : list(strings) eg. save_differences=True
        
    Description
    -----------
    converts each line into a key,value pair for the settings dict

    Returns
    -------
    settings_dict : dict(setting_name:user_value)

    '''
    settings_dict = {}
    for line in lines:
        if '=' in line:
            settings_dict[line.split('=')[0]] = line.split('=')[1].rstrip()
    return settings_dict

def compare_dww(html_list_old,html_list_new,diff_dest,save_differences):
    '''
    

    Parameters
    ----------
    html_list_old : list(html_pages)
    html_list_new : list(html_pages)
    diff_dest : url to save computed differences between html_list_old
        and html_list_new
    save_differences : boolean 
    
    Description
    -----------
    Pulls table tags from each html page, and saves them to a pandas df
    goes table by table in each html page, making sure no rows have
    been altered, rearranged rows are fine.  If any rows have been altered it    
    prints differences to console and saves them to simple text file if 
    save_differences: True
    
    Returns
    -------
    None.

    '''
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
    
###########Program######################
if __name__ =='__main__':
    try:
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
                html_list_new = map(get_dww_endpoint,hyperlink_df.hyperlink)
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
