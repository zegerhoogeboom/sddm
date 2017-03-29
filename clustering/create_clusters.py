from UnionFind import UnionFind

family = UnionFind()

print "Reading mapped data..."
mapped_data = open("../mappings/mapped.txt", 'r')

print "Going through data..."
for line in mapped_data:
    line = line.replace("\n", "")
    inputs = line.split(',')[1:]
    family.union(*inputs)
print "Done creating clusters..."

print "Getting clusters..."
clusters = family.get_clusters()
cluster_file = open("clusters.txt", "a")

print "Writing clusters..."
for k, v in clusters.iteritems():
    cluster_file.write("%s\n" % ','.join(v))
print "Done!"
