# Artemis Tack, Iowa State University
# August 2025
#
# This script reads a CSV file containing meta ad library data. It then removes
# duplicate entries based on the 7th column (ad_creative_bodies) and writes the
# cleaned data to a new CSV file.
#
#
#            ____                      ,
#           /---.'.__             ____//
#                '--.\           /.---'
#           _______  \\         //
#         /.------.\  \|      .'/  ______
#        //  ___  \ \ ||/|\  //  _/_----.\__
#       |/  /.-.\  \ \:|< >|// _/.'..\   '--'
#          //   \'. | \'.|.'/ /_/ /  \\
#         //     \ \_\/" ' ~\-'.-'    \\
#        //       '-._| :H: |'-.__     \\
#       //           (/'==='\)'-._\     ||
#       ||                        \\    \|
#       ||                         \\    '
# snd   |/                          \\
#                                    ||
#                                    ||
#                                    \\
#   

import pandas as pd

# ask for the file path
file_name = input("Enter the path to the CSV file: ")

data = pd.read_csv(file_name)

# drop duplicates in the 7th column
cleaned_data = data.drop_duplicates(subset=data.columns[7])

cleaned_data.to_csv('cleaned_meta.csv', index=False)


