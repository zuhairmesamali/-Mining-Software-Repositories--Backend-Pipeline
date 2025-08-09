import os
from rdflib import Graph
from neo4j import GraphDatabase

def push_to_neo4j(ttl_path):
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    driver = GraphDatabase.driver(neo4j_uri)

    g = Graph()
    g.parse(ttl_path, format="turtle")

    def insert_triple(tx, s, p, o):
        query = """
        MERGE (sub:Resource {uri: $subject})
        MERGE (obj:Resource {uri: $object})
        MERGE (sub)-[r:RELATION {type: $predicate}]->(obj)
        """
        tx.run(query, subject=str(s), predicate=str(p), object=str(o))

    with driver.session() as session:
        for s, p, o in g:
            # Handle literals vs URIs
            if isinstance(o, (str, int, float)):
                query = """
                MERGE (sub:Resource {uri: $subject})
                SET sub.`%s` = $value
                """ % str(p)
                session.run(query, subject=str(s), value=str(o))
            else:
                session.execute_write(insert_triple, s, p, o)

    driver.close()
    print("Neo4j population from TTL complete!")

if __name__ == "__main__":
    ttl_file = "RDF/repo_triples.ttl"
    push_to_neo4j(ttl_file)
