from glob import glob
import pandas as pd
import os
import shutil

folder_path = 'C:/Users/Hoan/Documents/policeoutcomedata'

street_files_1 = glob('C:/Users/Hoan/Documents/Advanced data management/**/*city-of-london-street.csv', recursive=True)
street_files_2 = glob('C:/Users/Hoan/Documents/Advanced data management/**/*metropolitan-street.csv', recursive=True)


outcome_files_1 = glob('C:/Users/Hoan/Documents/Advanced data management/**/*city-of-london-outcomes.csv', recursive=True)
outcome_files_2 = glob('C:/Users/Hoan/Documents/Advanced data management/**/*metropolitan-outcomes.csv', recursive=True)

street_files = street_files_1 + street_files_2
street_files.sort()

outcome_files = outcome_files_1 + outcome_files_2
outcome_files.sort()

for r in outcome_files:
    shutil.copy(r, folder_path)
# df_list = []
# for r in street_files:
#     df = pd.read_csv(r)
#     df_list.append(df) 
# big_df = pd.concat(df_list, ignore_index=True)
# big_df.to_csv(os.path.join(folder_path, 'street-data.csv'), index=False)


# outcome_files = outcome_files_1 + outcome_files_2
# outcome_files.sort()
# df_list = []
# for r in outcome_files:
#     df = pd.read_csv(r)
#     df_list.append(df) 
# big_df = pd.concat(df_list, ignore_index=True)
# big_df.to_csv(os.path.join(folder_path, 'outcomes-data.csv'), index=False)