MATCH p=(sn)-[r]->(en)
SET sn.node_id = ID(sn)
SET en.node_id = ID(en)
SET r.rel_id = ID(r)

WITH COUNT(p) as p
MATCH (n)
WHERE NOT (n)-[]-()
SET n.node_id = ID(n)