# If authentication error ocurs, delete the file located in:
# ~/.config/gspread_pandas/creds

import sys
import datetime

from functions import *

# Get user input to determine if in testing or production mode
is_test = False
T5 = False
BT = True

# Set flags to determine sections that run
pull = True
push = {
    'Druid': True,
    'Hunter': True,
    'Mage': True,
    'Paladin': True,
    'Priest': True,
    'Rogue': True,
    'Shaman': True,
    'Warlock': True,
    'Warrior': True
}

if BT:
    folder = 'BT'
    if is_test:
        spread = '1689SHfCWtyC_UK8wt261ITTOZCblqwNVHoop85Sgdug'
    else:
        spread = '1NqPhGFaCLmiAN71_80vSidM9ar22QMs1QS5-mBBL5Jc'
elif T5:
    folder = 'T5'
    if is_test:
        spread = '1P8GQ7gf2hfSnH_C8Y2M_jf2AxjHvJG4Xxx5Si_liJQk'
    else:
        spread = '1QjBqgl7HWWhQv4p3thFiZOfmZaTK3FasPHoE7THBxPc'

# Import personal email from environment vars
email = os.environ.get('pers_email')

# Set number of seconds between loot entry iterations
sleep_time = 300

# List of items not tracked by Loot List
bad_items = ['Staff of Disintegration', 'Warp Slicer', 'Cosmic Infuser',
        'Phaseshift Bulwark', 'Devastation', 'Infinity Blade',
        'Netherstrand Longbow', 'Nether Spike', 'Staff of Disintegration',
        'Pit Lord\'s Satchel', 'Pattern: Belt of Deep Shadow',
        'Pattern: Belt of the Black Eagle', 'Nether Vortex',
        'Pattern: Boots of the Crimson Hawk']

# Collect and organize loot data from previous raid
raid_date, raid_date_str = get_raid_date()
loot_file = get_loot_file(raid_date_str)
extracted_loot = extract_loot_info(loot_file)
cleaned_loot = clean_loot_df(extracted_loot, bad_items)

# Set up gspread_pandas
client, spread = gspread_pandas_setup(user=email, spread=spread)

# Only pull sheets if flag is set to True
if pull:
    # Import and filter sheets from Google
    data = import_sheet(sheet='Data', index=None, header_rows=1, start_row=2, spread=spread).iloc[:,15:22]
    data = data.loc[data['Rank'] != 'Inactive'][['Player', 'Item', 'NumPassed', 'Equity']]
    data[['NumPassed', 'Equity']] = data[['NumPassed', 'Equity']].apply(pd.to_numeric)

    roster = import_sheet(sheet='Roster', index=None, header_rows=1, start_row=1, spread=spread).iloc[:,6:10]

# Generate merged dfs
merged_data = data.merge(roster, how='left', left_on='Player', right_on='Name')[['Player', 'Class', 'Item', 'NumPassed', 'Equity']]
loot_received = cleaned_loot.merge(roster, how='left', left_on='player', right_on='Name')[['item', 'player', 'Class']]

# Convert item and player columns to lists and use list comprehension to generate list of players who passed on items
items = list(cleaned_loot['item'])
players = list(cleaned_loot['player'])
pass_players = [passers(item, player, merged_data) for item, player in zip(items, players)]

# Add pass players to loot_received | reorder/rename/combine columns
loot_received['pass_player'] = pass_players
loot_received.columns = ['item', 'player_received', 'received_class', 'players_passed']
loot_received['player_received'] = list(zip(loot_received['player_received'], loot_received['received_class']))
loot_received = loot_received[['item', 'player_received', 'players_passed']]

# Separate items with vs without passes
no_passes = loot_received.loc[loot_received['players_passed'].isna()].reset_index(drop=True)[['item', 'player_received']]
passes = loot_received.loc[~loot_received['players_passed'].isna()].reset_index(drop=True)

# Only pull class dicts if flag set to True
if pull:
    # Create dict to store class sheets
    class_dfs = {'Druid': None, 'Hunter': None, 'Mage': None, 'Paladin': None, 'Priest': None, 'Rogue': None, 'Shaman': None, 'Warlock': None, 'Warrior': None}

    # Iterate over classes and run function to pull and store class sheets in dict
    for key in class_dfs.keys():
        pull_class_sheets_from_google(class_dfs, key, spread)

# Create a copy of dict and list of items/players passed
items = list(passes['item'])
pass_player_class = list(passes['players_passed'])

# Concatenate the item and player received columns from the passes df to the bottom of the no_passes df
loot_entry = pd.concat([no_passes, passes[['item', 'player_received']]], axis=0, ignore_index=True, join='outer')

# Apply proper formatting to loot_date
loot_date_format = raid_date.strftime('%-m/%-d/%y')

# Enter loot for all items and players
for item, entry in zip(loot_entry['item'], loot_entry['player_received']):
    enter_loot(class_dfs, item, entry, loot_date_format)

# Enter passes for all items and players
for item, player in zip(items, pass_player_class):
    enter_passes(class_dfs, item, player)

# Loop through class dfs and upload them to Google Sheet
for key in class_dfs.keys():
    

    # Warrior is always the last key, so we don't need to sleep after it to allow spreadsheet to finish its computing
    if key == 'Warrior':
        push_class_sheets_to_google(class_dfs, key, spread)
        break

    # Only push sheet to google if flag is set to True
    if push[key]:
        push_class_sheets_to_google(class_dfs, key, spread)
        now = datetime.datetime.fromtimestamp(time.time() + 300).strftime('%-I:%M:%S %p')
        print(f'Next iteration begins at {now}.')
        time.sleep(sleep_time)