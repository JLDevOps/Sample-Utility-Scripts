import pandas as pd
import argparse

# CSV to Database Table Mapping
column_mapping = {
    'csv_col1': 'db_colA',
    'csv_col3': 'db_colB',
    'csv_col5': 'db_colC'
}

def generate_sql(csv_file, output_sql, table):
    df = pd.read_csv(csv_file)
    df = df[list(column_mapping.keys())]
    df.rename(columns=column_mapping, inplace=True)

    with open(output_sql, 'w', encoding='utf-8') as out_f:
        for _, row in df.iterrows():
            values = []
            for value in row:
                if pd.isnull(value):
                    values.append('NULL')
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    values.append("'" + str(value).replace("'", "''") + "'")

            columns_sql = ', '.join(df.columns)
            values_sql = ', '.join(values)
            sql = f"INSERT INTO {table} ({columns_sql}) VALUES ({values_sql});"
            out_f.write(sql + '\n')
    print(f"SQL statements written to {output_sql}")

def main():
    parser = argparse.ArgumentParser(description="Generate SQL INSERT statements from CSV")
    parser.add_argument('--csv', type=str, default='data.csv', help="Path to the input CSV file")
    parser.add_argument('--output', type=str, default='insert_statements.sql', help="Path to the output SQL file")
    parser.add_argument('--table', type=str, default='table', help="Table Name")

    args = parser.parse_args()
    generate_sql(args.csv, args.output, args.table)

if __name__=="__main__":
    main()