MATCH p=(sn)-[r]->(en)
SET sn.node_id = elementId(sn)
SET en.node_id = elementId(en)
SET r.rel_id = elementId(r)

WITH COUNT(p) as p
MATCH (n)
WHERE NOT EXISTS((n)-[]-())
SET n.node_id = elementId(n)