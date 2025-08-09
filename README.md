# Mining-Software-Repositories--Backend-Pipeline
Backend data pipeline for mining software repositories, using AWS (S3, Athena) and Python to extract software metrics from 5K+ GitHub repos, transform data into RDF triples, and load into Neo4j for semantic, graph-based analysis and quality-based ranking.

## Requirements
* Python 3

## Setup
* Create a virtual environment:
```
python3 -m venv .venv
```
Activate the virtual environment:
* On macOS/Linux:
```
source .venv/bin/activate
```
*On Windows:
```
.venv\Scripts\activate
```
Install the required packages:
```
pip install -r requirements.txt
```

## Environment Variables
Create a ```.env``` file in the root of the project and add the following variables:
```
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_DEFAULT_REGION=us-east-1
GITHUB_TOKEN=your_github_token
```

##Running the Main Script

