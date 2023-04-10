import os
import pandas as pd

# Retrieve list of all files in network and delete unwanted files
root = r"\\kie00geostor\well_documents\LGPU"

list_of_file_paths = []
for path, subdirs, files in os.walk(root):
    for name in files:
        file_path = os.path.join(path, name)
        slash_after_LGPU = file_path.find("U") + 2
        file_path_trimmed = file_path[slash_after_LGPU:]
        list_of_file_paths.append(file_path_trimmed)

list_of_file_paths.pop(0)
list_of_file_paths.pop(0)

# Define names of columns
column_deposit = 'Родовище'
column_well = 'Свердловина'
column_amount_of_files = 'Загальна кількість файлів'
column_amount_2022 = 'Кількість файлів 2022'
column_amount_2021 = 'Кількість файлів 2021'
column_amount_2020 = 'Кількість файлів 2020'
column_year = 'Рік'

df = pd.DataFrame(list_of_file_paths)

df.rename(columns={df.columns[0]: 'Родовище'}, inplace=True)
df[column_well] = 0
df[column_year] = 0

df[column_amount_2020] = 0
df[column_amount_2021] = 0
df[column_amount_2022] = 0

# Traverse through list of files and get deposit name, well number and year
for i in df.index:
    file_name = df.at[i, column_deposit]
    first_slash = file_name.find('\\')
    deposit = file_name[:first_slash]
    file_name = file_name[first_slash + 1:]
    df.at[i, column_deposit] = deposit

    second_slash = file_name.find('\\')
    well = file_name[:second_slash]
    df.at[i, column_well] = well

    for j in range(3):
        first_underscore = file_name.find('_')
        file_name = file_name[first_underscore + 1:]

    year = file_name[:4]
    if year == '2020':
        df.at[i, column_amount_2020] = 1
    elif year == '2021':
        df.at[i, column_amount_2021] = 1
    elif year == '2022':
        df.at[i, column_amount_2022] = 1
    else:
        year = ''

    df.at[i, column_year] = year

# Count amount of files in each year
df[column_amount_of_files] = 0
df[column_amount_of_files] = df.groupby([column_deposit, column_well])[column_amount_of_files].transform('count')
df.drop_duplicates()

df[column_amount_2020] = df.groupby([column_deposit, column_well, column_amount_2020])[column_amount_2020].transform(
    'sum')
df[column_amount_2021] = df.groupby([column_deposit, column_well, column_amount_2021])[column_amount_2021].transform(
    'sum')
df[column_amount_2022] = df.groupby([column_deposit, column_well, column_amount_2022])[column_amount_2022].transform(
    'sum')
df = df.drop_duplicates()

column_result = [column_deposit, column_well, column_amount_of_files, column_amount_2020, column_amount_2021,
                 column_amount_2022]
df = df.groupby([column_deposit, column_well, column_amount_of_files])[
    column_amount_2020, column_amount_2021, column_amount_2022].agg('sum')

df.to_excel('result.xlsx', sheet_name='new_sheet')
