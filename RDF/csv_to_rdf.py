import os
import boto3
import csv
import pandas as pd
from functools import reduce
import numpy as np
from dotenv import load_dotenv
import os
import csv

load_dotenv()


aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')


def get_csv_from_s3(object_key, folder_path="RDF/data", bucket_name="capstone-dp15"):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    s3_client = session.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body'].read().decode('utf-8')
    
    file_path = os.path.join(folder_path, object_key)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as f:
        f.write(file_content)

    print(f"File {object_key} downloaded successfully!")


def merge_csv_files(csv_files, output_csv):
    dataframes = []
    
    for file_path in csv_files:
        df = pd.read_csv(file_path)
        if df.columns.tolist() == ['origin', 'value']:
            meta_name = file_path.split('.')[0]
            df.rename(columns={'value': meta_name}, inplace=True)
        
        dataframes.append(df)

    merged_df = reduce(
        lambda left, right: pd.merge(left, right, on='origin', how='outer'),
        dataframes
    )
    
    merged_df.to_csv(output_csv, index=False)
    print(f"Merged file written to: {output_csv}")

def clean_merged_csv(csv_path):
    df = pd.read_csv(csv_path)
    df = df[df['origin'].notnull()]
    df.to_csv(csv_path, index=False)
    print(f"Cleaned CSV saved at {csv_path}")

def add_years_column(csv_path):
    df = pd.read_csv(csv_path)
    def calculate_years(row):
        try:
            difference=365
            age = int(row['age'])
            years = age // difference
            return years
        except ValueError:
            return None
    df['number_of_years'] = df.apply(calculate_years, axis=1)
    df.to_csv(csv_path, index=False)
    print(f"Years column added to {csv_path}")

def add_percentile_column(csv_path, column_name):
    df = pd.read_csv(csv_path)
    new_column_name = f"{column_name}_percentile"
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    vals = df[column_name].dropna()
    target_percents = np.linspace(0, 100, 11)
    edges = np.percentile(vals, target_percents)
    
    merged_edges = [edges[0]]
    merged_percent = [target_percents[0]]
    for i in range(1, len(edges)):
        if edges[i] != merged_edges[-1]:
            merged_edges.append(edges[i])
            merged_percent.append(target_percents[i])
        else:
            merged_percent[-1] = target_percents[i]

    labels = []
    for i in range(len(merged_edges) - 1):
        lower_pct = int(merged_percent[i])
        upper_pct = int(merged_percent[i+1])
        labels.append(f"{lower_pct}-{upper_pct}%")
    
    df[new_column_name] = pd.cut(df[column_name],
                                 bins=merged_edges,
                                 labels=labels,
                                 include_lowest=True)
    
    df.to_csv(csv_path, index=False)
    print(f"CSV updated with {column_name} quantiles at {csv_path}")

def csv_to_rdf(csv_input_path, turtle_output_path):
    def format_numeric(val):
        try:
            float_val = float(val)
            return str(int(float_val)) if float_val.is_integer() else str(float_val)
        except:
            return None
    def sanitize_origin(origin):
        return origin.strip().replace(" ", "_").replace("/", "_").replace(".", "_").replace(":", "_").replace("-", "_")
    output_dir = os.path.dirname(turtle_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(csv_input_path, 'r', encoding='utf-8') as csvfile, \
         open(turtle_output_path, 'w', encoding='utf-8') as turtlefile:
        reader = csv.DictReader(csvfile)
        turtlefile.write('@prefix ex: <http://example.org/repo/> .\n')
        turtlefile.write('@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n')
        turtlefile.write('@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n')

        for row in reader:
            origin = row.get('origin', '').strip()
            if not origin:
                continue
            origin_id = sanitize_origin(origin)
            turtlefile.write(f'ex:{origin_id} rdf:type ex:Repository .\n')
            for key, predicate in [
                ("approx_loc", "approxLoc"),
                ("unique_developers", "uniqueDevelopers"),
                ("versions", "versions"),
                ("age", "age"),
                ("number_of_years", "numberOfYears")
            ]:
                raw_val = row.get(key, '').strip()
                val = format_numeric(raw_val)
                if val:
                    turtlefile.write(f'ex:{origin_id} ex:{predicate} "{val}"^^xsd:decimal .\n')

            for key, predicate in [
                ("approx_loc_percentile", "approxLocPercentile"),
                ("unique_developers_percentile", "uniqueDevelopersPercentile"),
                ("versions_percentile", "versionsPercentile"),
                ("age_percentile", "agePercentile")
            ]:
                val = row.get(key, '').strip()
                if val:
                    turtlefile.write(f'ex:{origin_id} ex:{predicate} "{val}" .\n')
            turtlefile.write('\n')
    print(f"RDF Turtle file saved to: {turtle_output_path}")
