# Import dependencies
from __future__ import print_function
import pandas as pd
from gspread_pandas import Spread, Client
import gspread_pandas
import numpy as np
from datetime import date, timedelta
import time

# Get date of most recent Tuesday and format it to pull associated loot file
today = date.today()
weekday = today.weekday()
if weekday <= 1:
    loot_date = today - timedelta(weekday + 6)
else:
    loot_date = today - timedelta(weekday - 1)
date_str = loot_date.strftime("%m.%d")

# Read in loot file and filter out erroneous lines
loot_received_df = pd.read_csv(f'../loot_logs/{date_str}.txt', sep='\n', header=None)
loot_received_df.columns = ['full_lines']
loot_received_df = loot_received_df.loc[loot_received_df['full_lines'].str.match(r'^-.*')]

# Use regex to extract item looted and player received from each line and save to df
loot_received_df[['item','player']] = loot_received_df['full_lines'].str.extract(r'^-\s(.+)\s-\s0\sDKP\s-\s(\w+)')
loot_received_df = loot_received_df[['item', 'player']]

# Remove rows containing items not on loot lists
bad_items = ['Staff of Disintegration', 'Warp Slicer', 'Cosmic Infuser',
            'Phaseshift Bulwark', 'Devastation', 'Infinity Blade',
            'Netherstrand Longbow', 'Nether Spike', 'Staff of Disintegration',
            'Pit Lord\'s Satchel', 'Pattern: Belt of Deep Shadow',
            'Pattern: Belt of the Black Eagle', 'Nether Vortex',
            'Pattern: Boots of the Crimson Hawk']

loot_received_df = loot_received_df.loc[~loot_received_df['item'].isin(bad_items)]
loot_received_df = loot_received_df.loc[loot_received_df['player'] != 'Roll']

# Set up gspread_pandas
config = gspread_pandas.conf.get_config(conf_dir='.', file_name='google_secret.json')
client = Client(user='cd002009@gmail.com', config=config)
spread = Spread(user='cd002009@gmail.com', spread='1QjBqgl7HWWhQv4p3thFiZOfmZaTK3FasPHoE7THBxPc', config=config)

# Open "Data" sheet and import it to df
spread.open_sheet('Data')
df = spread.sheet_to_df(index=None, header_rows=1, start_row=2)

# Open "Roster" sheet, import it to df, and filter down to relevant columns
spread.open_sheet('Roster')
roster_df = spread.sheet_to_df(index=None, header_rows=1, start_row=1)
roster_df = roster_df.iloc[:,6:10]

# Filter df from "Data" to show only relevant columns then filter out Inactive players, drop additional irrelevant columns, and apply numeric values
df_filter = df.iloc[:, 15:22]
df_filter = df_filter.loc[df_filter['Rank'] != 'Inactive']
df_filter = df_filter[['Player', 'Item', 'NumPassed', 'Equity']]
df_filter[['NumPassed', 'Equity']] = df_filter[['NumPassed', 'Equity']].apply(pd.to_numeric)

# Merge "Roster" and "Data" so each player's class is shown. This will be used to call the correct class sheets when updating loot passed/received
df_merge = df_filter.merge(roster_df, how='left', left_on='Player', right_on='Name')
df_merge = df_merge[['Player', 'Class', 'Item', 'NumPassed', 'Equity']]

# Merge df containing weekly loot data with "Roster" to associate players with classes
loot_received_merge_df = loot_received_df.merge(roster_df, how='left', left_on='player', right_on='Name')
loot_received_merge_df = loot_received_merge_df[['item', 'player', 'Class']]

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

    # Convert empty lists to NaN
    if pass_credit == []:
        pass_credit == np.nan

    return pass_credit

# Convert item and player columns to lists and use list comprehension to generate list of players who passed on items
items = list(loot_received_df['item'])
players = list(loot_received_df['player'])
pass_players = [passers(item, player, df_merge) for item, player in zip(items, players)]

# Copy loot df, add passes to new column, reorder/rename/combine columns
loot_entry_df = loot_received_merge_df.copy()
loot_entry_df['pass_player'] = pass_players
loot_entry_df.columns = ['item', 'player_received', 'received_class', 'players_passed']
loot_entry_df['player_received'] = list(zip(loot_entry_df['player_received'], loot_entry_df['received_class']))
loot_entry_df = loot_entry_df[['item', 'player_received', 'players_passed']]

# Separate items with vs without passes
no_passes = loot_entry_df.loc[loot_entry_df['players_passed'].isna()].reset_index(drop=True)[['item', 'player_received']]
passes = loot_entry_df.loc[~loot_entry_df['players_passed'].isna()].reset_index(drop=True)

# Create dict to store class sheets
class_dfs = {'Druid': None, 'Hunter': None, 'Mage': None, 'Paladin': None, 'Priest': None, 'Rogue': None, 'Shaman': None, 'Warlock': None, 'Warrior': None}

# Function to pull each class sheet from Google as df and store in dictionary with key = class_name
def pull_class_sheets_from_google(df_dict, class_sheet):
    start_time = time.time()
    print(f'Pulling {class_sheet} sheet from Google.', end="")
    spread.open_sheet(class_sheet)
    df_dict[class_sheet] = spread.sheet_to_df(index=None, header_rows=1, start_row=2)
    print(f'Done in {time.time() - start_time:.2f} seconds.')

# Iterate over classes and run function to pull and store class sheets in dict
for key in class_dfs.keys():
    pull_class_sheets_from_google(class_dfs, key)

# Create a copy of dict and list of items/players passed
class_dfs_copy = class_dfs.copy()
items = passes['item'].to_list()
pass_player_class = list(passes['players_passed'])

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

# Concatenate the item and player received columns from the passes df to the bottom of the no_passes df
loot_entry_concat = pd.concat([no_passes, passes[['item', 'player_received']]], axis=0, ignore_index=True, join='outer')

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
    player_list.loc[player_list[''] == item, 'Date'] = date_received

    # Update player's LL in class df and update class df dictionary
    df.iloc[:, start_col:start_col + 5] = player_list
    class_df_dict[player_class] = df

# Apply proper formatting to loot_date
loot_date_format = loot_date.strftime('%#m/%#d/%y')

# Enter loot for all items and players
for item, entry in zip(loot_entry_concat['item'], loot_entry_concat['player_received']):
    enter_loot(class_dfs_copy, item, entry, loot_date_format)

# Enter passes for all items and players
for item, player in zip(items, pass_player_class):
    enter_passes(class_dfs_copy, item, player)

# Function to push updated class dfs to Google Sheet
def push_class_sheets_to_google(class_sheet):

    start_time = time.time()
    print(f'Passing {class_sheet} sheet to Google...', end="")
    spread.open_sheet(class_sheet)

    # Due to the complexity of the Google Sheet, it can take several minutes to finish computing
    # This try-except block is in place to identify timeout errors, so that failed uploads can be re-tried.
    try:
        spread.df_to_sheet(class_dfs_copy[class_sheet], index=False, headers=False, start='A5')
    except:
        print(f'Failed. Moving to next.')
        return
    print(f'Done in {time.time() - start_time:.2f} seconds.')

sleep_time = 240

# Loop through class dfs and upload them to Google Sheet
for key in class_dfs_copy.keys():
    
    # Warrior is always the last key, so we don't need to sleep after it to allow spreadsheet to finish its computing
    if key == 'Warrior':
        push_class_sheets_to_google(key)
        break

    push_class_sheets_to_google(key)
    print(f'Next iteration begins in {sleep_time} seconds.')
    time.sleep(sleep_time)