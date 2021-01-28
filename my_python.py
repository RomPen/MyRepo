import pandas as pd
import os
from calendar import monthrange
import numpy as np
from datetime import datetime 
from dateutil.relativedelta import relativedelta
import re


def file_search(path = '.', doctype = 'csv', like = [''], strict = False):
    """
    This function creates a list of all files of a certain type, satisfying the criteria outlined
    in like = [...] parameter. The function only searches for files in the specified folder
    of the current working directory that is set by the user.
    
    Parameters
    -----------
    path : string 
        Path to a folder in the current working directory 
        default = '.', i.e. current working directory folder
    doctype : string
        Document format to search for 
        e.g. 'csv' or 'xlsx'
        default = 'csv'
    like : list
        A list of words to filter the file search on 
        default = [''], i.e. no filter
    strict : bool
        Set True to search for filenames containing all words from 'like' list (
        default = False
        
    Returns
    -------
    list
    
    Examples
    -------
    >>> file_search()
    ['Data_AprF_2019.csv',
     'Data_AugF_2019.csv',
     'Data_JulF_2019.csv',
     'Data_JunF_2019_v1.csv',
     'Data_MayF_2019.csv',
     'Data_SepP_2019.csv']
    
    >>> file_search(like = ['F', 'v1']) 
    ['Data_AprF_2019.csv',
     'Data_AugF_2019.csv',
     'Data_JulF_2019.csv',
     'Data_JunF_2019_v1.csv',
     'Data_MayF_2019.csv']
    
    >>> file_search(like = ['F', 'v1'], strict = True)
    ['Data_JunF_2019_v1.csv']
     
    """
    
    if not isinstance(path, str):
        raise ValueError('Please input path as a string')
    elif not isinstance(doctype, str):
        raise ValueError('Please input doctype as a string')
    elif not isinstance(like, list):
        raise ValueError('Please input like as a list')
    elif not isinstance(strict, bool):
        raise ValueError('Please input strict as a bool')
    else:
        pass
    
    list_of_files = []
    
    if strict == False:
        for file in os.listdir(path):
            if (file.split('.')[-1] == doctype) & (any(x in file for x in like)):
                list_of_files.append(file) 
    else:
        for file in os.listdir(path):
            if (file.split('.')[-1] == doctype) & (all(x in file for x in like)):
                list_of_files.append(file) 

    return list_of_files


def compare(x, y, names = ['x','y'], dups = False, same = False, comment = False, highlight = True):
    """
    This function returns a dictionary with:
        
        1. Same values between data frames x and y
        2. Values in x, not in y
        3. Values in y, not in x
        
        (optional):
        (4) Duplicates of x
        (5) Duplicates of y
        (6) Boolean of whether x and y are the same
        (7) Boolean to give comments
        (8) Boolean to show difference highlights
        
    Parameters
    ----------
    x : pandas.DataFrame
        DataFrame #1
    y : pandas.DataFrame
        DataFrame #2
    names : list
        a list of user preferred file names
        e.g. ['File1', 'File2']
        default = ['x','y']
    dups : bool
        True to include duplicates check for each file 
        default = False
    same : bool 
        True to activate. Outputs True if DataFrames are the same 
        default = False
    comment : bool
        True to activate. Prints out statistics of the compariosn results
        e.g. number of same valeus, number of duplicates, number of outliers and whether the DataFrames are the same
        default = False
    highlight : bool
        True to activate. Returns a dataframe highlighting df1/df2 differences value by value. Same values ignored (replaced with '0').
        
    Returns
    -------
    out : dict  

    Examples
    --------

    >>> c = compare(df1, df2, names = ['df1','df2'], dups = True, same = True, comment =True)

    There are 133891 same values
    There are 16531 outliers in df1
    There are 20937 outliers in df2
    There are 48704 duplicates in df1
    There are 0 duplicates in df2
    The DataFrames are not the same

    >>> c = compare(df2, df2, names = ['df2','df2'], dups = True, same = True, comment =True)

    There are 154444 same values
    There are 0 outliers in df2
    There are 0 outliers in df2
    There are 0 duplicates in df2
    There are 0 duplicates in df2
    The DataFrames are the same     
    """
    
    if not isinstance(x, pd.DataFrame):
        raise ValueError('Please input x as a pandas.DataFrame')
    elif not isinstance(y, pd.DataFrame):
        raise ValueError('Please input y as a pandas.DataFrame')
    elif not isinstance(names, list):
        raise ValueError('Please input names as a list')
    elif not isinstance(dups, bool):
        raise ValueError('Please input dups as a bool')
    elif not isinstance(same, bool):
        raise ValueError('Please input same as a bool')
    elif not isinstance(comment, bool):
        raise ValueError('Please input comment as a bool')
    else:
        pass

    dict_temp = {}
    
    try:
        dict_temp['same_values'] = pd.merge(x.drop_duplicates(),y.drop_duplicates(), how = 'inner')
    except:
        print('Unable to identify same values')
    try:
        dict_temp[names[0] + '_not_' + names[1]] = pd.concat([x,dict_temp['same_values']], ignore_index = True).drop_duplicates(keep = False)
        dict_temp[names[1] + '_not_' + names[0]] = pd.concat([y,dict_temp['same_values']], ignore_index = True).drop_duplicates(keep = False)
    except:
        print('Unable to find outliers')
    
    if dups == True:
        try:
            dict_temp[names[0] + '_dups'] = x[x.duplicated() == True]    
            dict_temp[names[1] + '_dups'] = y[y.duplicated() == True]
        except:
            print('Unable to find duplicates')
    if same == True:
        try:
            if (x.shape == y.shape) & (x.shape == dict_temp['same_values'].shape):
                dict_temp['Same'] = True
            else:
                dict_temp['Same'] = False
        except:
            print('Unable to determine whether the Dataframes are the same')
            
    if highlight == True:
      try:
        #df1_not_df2 = dict_comp['df1_not_df2'].copy()
        #df2_not_df1 = dict_comp['df2_not_df1'].copy()

        #print(df1_not_df2[~df1_not_df2.isin(df2_not_df1)].fillna('-'))
        #print(df2_not_df1[~df2_not_df1.isin(df1_not_df2)].fillna('-'))

        df_t = dict_comp['df1_not_df2'].copy()[~dict_comp['df1_not_df2'].copy().isin(dict_comp['df2_not_df1'].copy())].fillna('-')
        df_k = dict_comp['df2_not_df1'].copy()[~dict_comp['df2_not_df1'].copy().isin(dict_comp['df1_not_df2'].copy())].fillna('-')
        
        for i in df_t.columns:
          df_t[i] = df_t[i].astype(str) + '/' + df_k[i].astype(str)
          
        dict_temp['highlight'] = df_t.replace('-/-','-')
      except:
        print('Unable to calculate highlight')
    
    try:     
        if comment == True:
            print('same values: ' + str(dict_temp['same_values'].shape[0]))
            print('outliers in ' + str(names[0]) + ': ' + str(dict_temp[names[0] + '_not_' + names[1]].shape[0]))
            print('outliers in ' + str(names[1]) + ': ' + str(dict_temp[names[1] + '_not_' + names[0]].shape[0]))
            if dups == True:
                print('duplicates in ' + names[0] + ': ' + str(dict_temp[names[0] + '_dups'].shape[0]))  
                print('duplicates in ' + names[1] + ': ' + str(dict_temp[names[1] + '_dups'].shape[0]))
            if same == True:
                if dict_temp['Same'] == True:
                    s = 'the same'
                else:
                    s = 'not the same'
                print('DataFrames are ' + s)           
    except:
        print('Unable to print commentary')
    
    return dict_temp



def add_time(date, days = 0, months = 0, years = 0, time_format_in = '%Y-%m-%d', time_format_out = '%Y-%m-%d'):
    
    return (datetime.strptime(date, time_format_in) + relativedelta(days = int(days), months = int(months), years = int(years))).strftime(time_format_out)


def isPalindrome(string):
    
    s = ''.join(string.lower().split(' '))
    
    if len(s) in [0,1]:
        return True
    else:
        return s[0] == s[-1] and isPalindrome(s[1:-1])
    
def wrap(string, max_width):
    
    return '\n'.join([string[t:t+max_width] for t in range(0, len(string), max_width)])


def module_import(module: str, import_as = None, import_from = None):
  try:
      import_as = module if import_as is None else import_as
      pattern = f"globals()['{import_as}'] = __import__" + (f"('{module}')" if import_from is None else f"('{import_from}', fromlist = ['{module}']).{module}")
      if import_as not in globals().keys():
        exec(pattern + f"\nprint('imported {module}')")
  except Exception as e:
      print(f'Failed: {e}')
      
def fracture_list(l: list, n: int):
    
    assert len(l) >= n, f'Length of list must be greater or equal than number of elements in each split: {n}'
    
    return [l[n*i:n*(i+1)] for i in range(-(-len(l)//n))]
