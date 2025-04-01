import csv
import argparse

# CSV to Database Table Mapping
column_mapping = {
    'csv_col1': 'db_colA',
    'csv_col3': 'db_colB',
    'csv_col5': 'db_colC'
}

def generate_sql(csv_file, output_sql):
    with open(csv_file, newline='', encoding='utf-8') as f, open(output_sql, 'w', encoding='utf-8') as out_f:
        reader = csv.DictReader(f)
        for row in reader:
            values = [row[c].replace("'", "''") for c in column_mapping.keys()]
            columns_sql = ', '.join(column_mapping.values())
            values_sql = ', '.join(f"'{v}'" for v in values)
            sql = f"INSERT INTO my_table ({columns_sql}) VALUES ({values_sql});"
            out_f.write(sql + '\n')
    print(f"SQL statements written to {output_sql}")

def main():
    parser = argparse.ArgumentParser(description="Generate SQL INSERT statements from CSV")
    parser.add_argument('--csv', type=str, default='data.csv', help="Path to the input CSV file")
    parser.add_argument('--output', type=str, default='insert_statements.sql', help="Path to the output SQL file")
    args = parser.parse_args()
    generate_sql(args.csv, args.output)

if __name__=="__main__":
    main()