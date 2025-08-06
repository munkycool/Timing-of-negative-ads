# Artemis Tack, Iowa State University
# August 2025
#
# This script reads a CSV file containing google-political-ads-creative-stats data,
# filters for VIDEO ads in the US within a specified date range, and writes the 
# Creative ID and Advertiser ID to a new CSV file.
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
#                                     '

import csv
import sys

def read_csv(file_path):
    # Increase the field size limit to handle large fields
    csv.field_size_limit(sys.maxsize)
    
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        data = [row for row in reader]
    return data

def write_csv(file_path, data):
    with open(file_path, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
        
    
def main():    
    input_file = input("Enter the input CSV file path for google-political-ads-creative-stats: ")
    output_file = input("Enter the output CSV file path: ")

    # read data from CSV
    data = read_csv(input_file)
    
    # get start data from the user
    start_date = int(input("Enter the starting date (YYYY-MM-DD): "))

    # get the end date from the user
    end_date = int(input("Enter the ending date (YYYY-MM-DD): "))
    
    # Enter the type of data to filter. Options are VIDEO, IMAGE, TEXT
    ad_type = input("Enter the type of ad to filter (e.g., VIDEO, IMAGE, TEXT): ").upper()

    # Validate ad_type
    valid_ad_types = ['VIDEO', 'IMAGE', 'TEXT']
    if ad_type not in valid_ad_types:
        print(f"Invalid ad type. Please choose from {valid_ad_types}.")
        return
    
    # Enter a limit for the number of rows to print (0 for no limit)
    row_limit = int(input("Enter a limit for the number of rows to print (0 for no limit): "))

    filtered_rows = []
    
    for row in data:
        if row[2] == ad_type and row[3] == 'US':
            # Convert date strings to integers for comparison
            int_start_date = int(row[7].replace('-', ''))
            int_end_date = int(row[8].replace('-', ''))
            if start_date <= int_start_date <= end_date or start_date <= int_end_date <= end_date:
                # Filter only the first and fifth columns
                # Assuming the first column is the creative ID and the fifth is the advertiser ID
                filtered_row = [row[0], row[4]]
                filtered_rows.append(filtered_row)

    write_csv(output_file, data=filtered_rows[:row_limit] if row_limit > 0 else filtered_rows)
    print(f"Written {len(filtered_rows)} rows with columns 0 and 4 to {output_file} from {input_file} from date {start_date} to {end_date}")


if __name__ == "__main__":
    main()
    
    