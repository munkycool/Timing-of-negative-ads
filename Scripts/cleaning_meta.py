import pandas as pd

# ask for the file path
file_name = input("Enter the path to the CSV file: ")

data = pd.read_csv(file_name)

# drop duplicates in the 7th column
cleaned_data = data.drop_duplicates(subset=data.columns[7])

cleaned_data.to_csv('cleaned_meta.csv', index=False)


