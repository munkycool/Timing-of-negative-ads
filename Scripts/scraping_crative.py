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
    file_output_name = input("Enter the output file name (without extension): ")
    
    input_file = '/Users/starlight/Documents/Accademia/Timing of negative ads/google-political-ads-transparency-bundle (1)/google-political-ads-creative-stats.csv'
    output_file = '/Users/starlight/Documents/Accademia/Timing of negative ads/google-political-ads-transparency-bundle (1)/{file_output_name}.csv'

    # read data from CSV
    data = read_csv(input_file)
    
    # get start data from the user
    start_date = int(input("Enter the starting date (YYYY-MM-DD): "))

    # get the end date from the user
    end_date = int(input("Enter the ending date (YYYY-MM-DD): "))
    
    

    
    filtered_rows = []
    for row in data:
        if row[2] == 'VIDEO' and row[3] == 'US':
            # Convert date strings to integers for comparison
            int_start_date = int(row[7].replace('-', ''))
            int_end_date = int(row[8].replace('-', ''))
            if start_date <= int_start_date <= end_date or start_date <= int_end_date <= end_date:
                # Filter only the first and fifth columns
                # Assuming the first column is the creative ID and the fifth is the advertiser ID
                filtered_row = [row[0], row[4]]
                filtered_rows.append(filtered_row)

    write_csv(output_file, data=filtered_rows)
    print(f"Written {len(filtered_rows)} rows with columns 0 and 4 to {output_file}")


if __name__ == "__main__":
    main()
    
    