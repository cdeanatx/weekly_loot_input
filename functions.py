from __future__ import print_function
import time
from datetime import date, timedelta
import os


import numpy as np
import pandas as pd

import gspread_pandas as gsp

# Get date of most recent Tuesday and format it to pull associated loot file
def get_raid_date(date_format = '%m.%d'):
    today = date.today()
    weekday = today.weekday()
    if weekday < 1:
        loot_date = today - timedelta(weekday + 6)
    else:
        loot_date = today - timedelta(weekday - 1)
    
    return loot_date, loot_date.strftime(date_format)

# Get most recent loot file and filter out erroneus lines
def get_loot_file(file_name, folder='T5', file_ext='txt'):
    loot_received_df = pd.read_csv(f'loot_logs/{folder}/{file_name}.{file_ext}', sep='\n', header=None)
    loot_received_df.columns = ['full_lines']
    
    return loot_received_df.loc[loot_received_df['full_lines'].str.match(r'^-.*')]

# Use regex to extract item looted and player received from each line and save to df
def extract_loot_info(df):
    df[['item','player']] = df['full_lines'].str.extract(r'^-\s(.+)\s-\s0\sDKP\s-\s(\w+)')
    
    return df[['item', 'player']]

# Clean loot df by removing untracked items and items that went to roll
def clean_loot_df(df, bad_items):
    
    df = df.loc[~df['item'].isin(bad_items)]

    return df.loc[df['player'] != 'Roll']

# Set up gspread_pandas
def gspread_pandas_setup(user, spread):
    client = gsp.Client(user=user)
    spread = gsp.Spread(user=user, spread=spread)

    return client, spread

# Open sheet from Google and import it to df
def import_sheet(sheet, index, header_rows, start_row, spread):

    start_time = time.time()
    print(f'Pulling {sheet} sheet from Google.', end=" ")
    spread.open_sheet(sheet)
    print(f'Done in {time.time() - start_time:.2f} seconds.')

    return spread.sheet_to_df(index=index, header_rows=header_rows, start_row=start_row)

# Funtion to generate a list of players who "passed" on a specific item. This will return a list of tuples in the format (class, player)
# During refactorization, check final if-statment. It appears to be redundant
def passers(item, player_received, loot_list_df):
    
    # Create a df for the current item being assessed for "passes" sorted in descending order by "Equity"
    df_curr_item = loot_list_df.loc[loot_list_df['Item'] == item].sort_values('Equity', ascending=False)
    
    # Determine the "Equity cutoff" by filtering the current item df on "Player Received"
    # iloc[0] ensures cutoff is accurate in the event that player who received item has the item on their list twice
    try:
        equity_cutoff = int(df_curr_item.loc[df_curr_item['Player'] == player_received].iloc[0]['Equity'])
    
    # If the try-block generates an index error, the player who received the item does not have it on their list
    # The item should have been marked as "Roll" and no pass credits are needed - return NaN
    except IndexError:
        return np.nan
    
    # Filter the current item df based on the equity cutoff
    # Extract players and their classes who will receive pass credit for the current item
    df_pass_curr_item = df_curr_item.loc[df_curr_item['Equity'] >= equity_cutoff]
    pass_credit = df_pass_curr_item.loc[df_pass_curr_item['Player'] != player_received]['Player']
    pass_class = df_pass_curr_item.loc[df_pass_curr_item['Player'] != player_received]['Class']

    # If there are no passes, return NaN
    if pass_credit.empty:
        return np.nan
    
    # Convert players and classes to a list of tuples
    else:
        pass_credit = list(zip(pass_class, pass_credit))

    return pass_credit

# Function to pull each class sheet from Google as df and store in dictionary with key = class_name
def pull_class_sheets_from_google(class_sheet, spread):
    start_time = time.time()
    print(f'Pulling {class_sheet} sheet from Google.', end=" ")
    spread.open_sheet(class_sheet)
    df = spread.sheet_to_df(index=None, header_rows=1, start_row=2)
    print(f'Done in {time.time() - start_time:.2f} seconds.')
    return df

# Function to enter passes for appropriate players 
def enter_passes(class_df_dict, item, list_of_tuples):
    
    # Iterate over each player/class tuple in the list for current item
    for entry in list_of_tuples:
        
        player_class = entry[0]
        player = entry[1]

        # Retrieve current player's class sheet and store it in df
        df = class_df_dict[player_class]

        # Determine starting column of current player's LL and use it to extract player's full LL
        start_col = df.columns.get_loc(player)
        player_list = df.iloc[:, start_col:start_col + 4]

        # Convert eligible columns to numbers and add one to pass column for current item
        player_list = player_list.apply(pd.to_numeric, errors='ignore')
        player_list.loc[player_list[''] == item, 'P'] += 1

        # Store updated LL in class sheet df and update dictionary
        df.iloc[:, start_col:start_col + 4] = player_list
        class_df_dict[player_class] = df

        return class_df_dict

# Function to enter loot received
def enter_loot(class_df_dict, item, player_class_tuple, date_received):
    
    # Save player and class as variables
    player_class = player_class_tuple[1]
    player = player_class_tuple[0]
    
    # Call class df for current player and extract their loot list
    df = class_df_dict[player_class]
    start_col = df.columns.get_loc(player)
    player_list = df.iloc[:, start_col:start_col + 5]

    # Convert data to numbers where applicable then apply date item was received in the appropriate row
    player_list = player_list.apply(pd.to_numeric, errors='ignore')
    try:
        item_index = player_list.loc[(player_list[''] == item) & ((pd.isna(player_list['Date'])) | (player_list['Date'] == ''))].index[0]
    except IndexError:
        print(f'{item} is not in {player}\'s loot list or they have already received the item.\nContinuing to next item.')
        return class_df_dict
    
    player_list.loc[item_index, 'Date'] = date_received

    # Update player's LL in class df and update class df dictionary
    df.iloc[:, start_col:start_col + 5] = player_list
    class_df_dict[player_class] = df

    return class_df_dict

# Function to push updated class dfs to Google Sheet
def push_class_sheets_to_google(class_dict, class_sheet, spread):

    start_time = time.time()
    print(f'Passing {class_sheet} sheet to Google...', end=" ")
    spread.open_sheet(class_sheet)

    # Due to the complexity of the Google Sheet, it can take several minutes to finish computing
    # This try-except block is in place to identify timeout errors, so that failed uploads can be re-tried.
    try:
        spread.df_to_sheet(class_dict[class_sheet], index=False, headers=False, start='A5')
    except:
        print(f'Failed. Moving to next.')
        return class_sheet
    print(f'Done in {time.time() - start_time:.2f} seconds.')
    