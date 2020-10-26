from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['localhost'],port=9042)
#session = cluster.connect('test',wait_for_all_pools=True)
#session.execute('USE test')
#cluster = Cluster(['0.0.0.0'])
session = cluster.connect()

session.execute("CREATE KEYSPACE bikes WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
session.execute('USE bikes')
session.execute("""CREATE TABLE bikes.Summary(attribute text, timestamp timestamp, elements int, min float,
 max float, mean float, std float, PRIMARY KEY (attribute, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);""");
session.execute("""CREATE TABLE bikes.Categorical_summary(attribute text, timestamp timestamp, elements int, 
categories list<text>, values list<float>, PRIMARY KEY (attribute, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);""");
