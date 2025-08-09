from RDF.rdf2neo4j import push_to_neo4j
from RDF.csv_to_rdf import (
    csv_to_rdf, 
    get_csv_from_s3, 
    merge_csv_files, 
    clean_merged_csv, 
    add_percentile_column, 
    add_years_column
)

if __name__ == "__main__":
    # Define the paths for the CSV files and the output Turtle file
    output_csv = "RDF/data/merged.csv"
    turtle_output = "RDF/repo_triples.ttl"
    
    # Download CSV files from S3. Ensure you have AWS credentials configured in your environment
    get_csv_from_s3("locs.csv")
    get_csv_from_s3("devs.csv")
    get_csv_from_s3("versions.csv")
    get_csv_from_s3("age.csv")
    merge_csv_files(["RDF/data/locs.csv", "RDF/data/devs.csv", "RDF/data/versions.csv", "RDF/data/age.csv"], output_csv)
    clean_merged_csv(output_csv)
    
    # Add percentile columns. Ensure the columns exist in the merged CSV and are numeric
    add_percentile_column(output_csv, "approx_loc")
    add_percentile_column(output_csv, "unique_developers")
    add_percentile_column(output_csv, "versions")
    add_percentile_column(output_csv, "age")
    add_years_column(output_csv)
    
    # Convert the merged CSV to RDF Turtle format
    csv_to_rdf(output_csv, turtle_output)
    print("Conversion complete! RDF triples have been written to", turtle_output)
    
    # # Push the data to Neo4j. Ensure Neo4j is running and accessible on the specified URI
    # push_to_neo4j(output_csv)
    # print("Neo4j population complete!")
