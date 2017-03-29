import sys

from UnionFind import UnionFind

data_file = "../mappings/mapped.txt"
output_file = "clusters.txt"
if len(sys.argv) >= 2: data_file = sys.argv[1]
if len(sys.argv) >= 3: output_file = sys.argv[2]

users = UnionFind()

print "Reading mapped data..."
mapped_data = open(data_file, 'r')

print "Going through data..."
for line in mapped_data:
    line = line.replace("\n", "")
    inputs = line.split(',')[1:]
    users.union(*inputs)
print "Done creating clusters..."

print "Getting clusters..."
clusters = users.get_clusters()
cluster_file = open(output_file, "a")

print "Writing clusters..."
for k, v in clusters.iteritems():
    cluster_file.write("%s\n" % ','.join(v))
print "Done!"
