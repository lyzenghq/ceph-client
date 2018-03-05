import rados

try:
    cluster = rados.Rados(conffile='')
    print "\nlibrados version: " + str(cluster.version())
    print "Will attempt to connect to: " + str(cluster.conf_get('mon initial members'))
except TypeError as e:
    print 'Argument validation error: ', e
    raise e

print "Created cluster handle."

try:
    cluster.connect()
except Exception as e:
    print "connection error: ", e
    raise e

print "Connected to the cluster."
print "\nCluster ID: " + cluster.get_fsid()

print "\n\nCluster Statistics"
print "=================="
cluster_stats = cluster.get_cluster_stats()

for key, value in cluster_stats.iteritems():
        print key, value


print "\n\nPool Operations"
print "==============="

if (not cluster.pool_exists('test')):
    print "\nCreate 'test' Pool"
    print "------------------"
    cluster.create_pool('test')

print "\nPool named 'test' exists: " + str(cluster.pool_exists('test'))
print "\nPools:"
print "-------------------------"
pools = cluster.list_pools()

for pool in pools:
        print pool

'''
print "\nDelete 'test' Pool"
print "------------------"
cluster.delete_pool('test')
print "\nPool named 'test' exists: " + str(cluster.pool_exists('test'))
'''


print "\n\nI/O Context and Object Operations"
print "================================="

print "\nCreating a context for the 'data' pool"
if not cluster.pool_exists('data'):
    raise RuntimeError('No data pool exists')
try:
    ioctx = cluster.open_ioctx('data')
    ioctx.require_ioctx_open()
except IoctxStateError as e:
    print "open I/O Context error: ", e
    raise e

print("\nGet pool usage statistics:")
stats = ioctx.get_stats()
for key in stats:
    print(key+':'+str(stats[key]))

print('\nIoctx last version:' + str(ioctx.get_last_version()))

print "\nWriting object 'hw' with contents 'Hello World!' to pool 'data'."
ioctx.write_full("hw", "Hello World!")
print "Writing XATTR 'lang' with value 'en_US' to object 'hw'"
ioctx.set_xattr("hw", "lang", "en_US")

print "\nWriting object 'bm' with contents 'Bonjour tout le monde!' to pool 'data'."
ioctx.write("bm", "Bonjour tout le monde!")
print "Writing XATTR 'lang' with value 'fr_FR' to object 'bm'"
ioctx.set_xattr("bm", "lang", "fr_FR")

print "\nContents of object 'hw'\n------------------------"
print ioctx.read("hw")

print "\nGetting XATTR 'lang' from object 'hw'"
print ioctx.get_xattr("hw", "lang")

print "\nContents of object 'bm'\n------------------------"
print ioctx.read("bm")

print "\nGetting XATTR 'lang' from object 'bm'"
print ioctx.get_xattr("bm", "lang")

print "\nState of Object 'hw':"
print ioctx.stat('hw')

print '\nIoctx.aio_write()\n------------------------'
ioctx.aio_write('hw', ' Tom?', 11)

print '\nIoctx.aio_append()\n------------------------'
ioctx.aio_append('hw', '\nHow are you?')

print '\nIoctx.aio_flush()\n------------------------'
ioctx.aio_flush()

print '\nIoctx.aio_read()\n------------------------'
def aio_read_oncomplete(completion, data_read):
    print data_read
ioctx.aio_read('hw', 100, 0, aio_read_oncomplete)

print '\nObjects:'
for ob in ioctx.list_objects():
    print str(ob)
print '\n'

object_iterator = ioctx.list_objects()
while True :
    try :
        rados_object = object_iterator.next()
        print "Object Contents:", rados_object.read()
        #print "Object State:", rados_object.stat()
    except StopIteration :
        break


'''
print "\nRemoving object 'hw'"
ioctx.remove_object("hw")

print "Removing object 'bm'"
ioctx.remove_object("bm")
'''

print "\nClosing the connection."
ioctx.close()

print "Shutting down the handle."
cluster.shutdown()

