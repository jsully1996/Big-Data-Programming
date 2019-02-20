import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import Row, SparkSession, functions, types, context

spark = SparkSession.builder.appName('example code').getOrCreate()
sc = spark.sparkContext

path_schema = types.StructType([
types.StructField('node', types.StringType(), False),
types.StructField('source', types.StringType(), False),
types.StructField('distance', types.IntegerType(), False),
])

def get_pathtrace(line):
    path_list = line.split(':')
    if path_list[1] == '':
        return None 
    node_list = path_list[1].split(' ')
    node_list = [x for x in node_list if x]
    results = []
    src = path_list[0]
    for dst in node_list:
        results.append((src, dst))
    return results

def apply_filter(x):
    if x is not None:
        return x

def get_distance(node):
    a = node[0]
    b = node[1]
    c = node[2]
    return (a,(b,c))

def main():
    textinput = sc.textFile(inputs)
    graphrow = Row('node', 'source', 'distance')
    graph_edges_rdd = textinput.map(get_pathtrace).filter(apply_filter).flatMap(lambda x: x).coalesce(1)
    graph_edges = graph_edges_rdd.toDF(['source', 'destination']).cache()
    graph_edges.registerTempTable('NodesTable')

    initial_node = node1
    initial_row = graphrow(initial_node, initial_node, 0)
    knownpaths = spark.createDataFrame([initial_row], schema=path_schema)
    partly_knownpaths = knownpaths
    # ITERATE THE GRAPH - 1 ITERATION = 1 STEP
    for i in range(6):
        partly_knownpaths.registerTempTable('PartlyKnownPathTable')
        newpaths = spark.sql("""
        SELECT destination AS node, t1.source AS source, (distance+1) AS distance FROM
        NodesTable t1
        JOIN
        PartlyKnownPathTable t2
        ON (t1.source = t2.node)
        """).cache()
        newpaths.registerTempTable('NewPathTable')
        knownpaths.registerTempTable('KnownPathTable')
        twin_df = spark.sql("""
        SELECT t1.node AS node, t1.source as source, t1.distance as distance FROM
        NewPathTable t1
        JOIN
        KnownPathTable t2
        ON (t1.node = t2.node)
        """)
        if twin_df.count() != 0:
            newpaths = newpaths.subtract(twin_df)

        partly_knownpaths = newpaths
        knownpaths = knownpaths.unionAll(newpaths)
        knownpaths.write.save(output + '/iteration_' + str(i), format='json')
    knownpaths_map = knownpaths.rdd.map(get_distance).cache()

    paths = []
    paths.append(node2)
    dest = knownpaths_map.lookup(node2) #From knownpaths iterate to reach the source from the destination
    for j in range(6):
        if not dest:
            paths = ['invalid destination']
            break
        parent = dest[0][0]
        paths.append(parent)
        if parent == node1:
            break
        dest = knownpaths_map.lookup(parent)

    paths = reversed(paths)  # Reverse the path to find the correct sequence

    outdata = sc.parallelize(paths).coalesce(1)  # Create rdd to write
    outdata.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    node1 = sys.argv[3]
    node2 = sys.argv[4]
    main()
