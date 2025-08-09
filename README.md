# Mining-Software-Repositories--Backend-Pipeline
Backend data pipeline for mining software repositories, using AWS (S3, Athena) and Python to extract software metrics from 5K+ GitHub repos, transform data into RDF triples, and load into Neo4j for semantic, graph-based analysis and quality-based ranking.

## Requirements
* Python 3

## Setup
* Create a virtual environment:
```python3 -m venv .venv```

Activate the virtual environment:

On macOS/Linux:
source .venv/bin/activate

On Windows:

