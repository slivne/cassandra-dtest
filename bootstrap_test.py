import random, time
import tempfile
import subprocess
import re
from dtest import debug, Tester
from tools import new_node, insert_c1c2, query_c1c2
from assertions import assert_almost_equal
from ccmlib.cluster import Cluster
from cassandra import ConsistencyLevel


class TestBootstrap(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def simple_bootstrap_test(self):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)

        keys = 10000

        # Create a single node cluster
        cluster.populate(1, tokens=[tokens[0]]).start(wait_other_notice=True)
        node1 = cluster.nodes["node1"]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={ 'c1' : 'text', 'c2' : 'text' })

        for n in xrange(0, keys):
            insert_c1c2(session, n, ConsistencyLevel.ONE)

        node1.flush()
        initial_size = node1.data_size()

        # Reads inserted data all during the boostrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys-1), ConsistencyLevel.ONE))

        # Boostraping a new node
        node2 = new_node(cluster, token=tokens[1])
        node2.start()
        node2.watch_log_for("Listening for thrift clients...")

        reader.check()
        node1.cleanup()
        time.sleep(.5)
        reader.check()

        size1 = node1.data_size()
        size2 = node2.data_size()
        assert_almost_equal(size1, size2, error=0.3)
        assert_almost_equal(initial_size, 2 * size1)

    def read_from_bootstrapped_node_test(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        version = cluster.version()
        cluster.start()

        node1 = cluster.nodes['node1']
        if version < "2.1":
            node1.stress(['-n', '10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        node4 = new_node(cluster)
        node4.start()

        session = self.patient_cql_connection(node4)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        rows = session.execute('select * from %s limit 10' % stress_table)
        assert len(list(rows)) == 10

    # should be moved in consistent_bootstrap.py or consistency_test.py
    def local_quorum_bootstrap_test(self):
        """Test that CL local_quorum works while a node is bootstrapping. CASSANDRA-8058"""

        cluster = self.cluster
        cluster.populate([1, 1])
        version = cluster.version()
        cluster.start()

        node1 = cluster.nodes['node1']
        if version < "2.1":
            node1.stress(['-n', '2000000', '-t', '15', '-S', '100',
                          '--replication-strategy', 'NetworkTopologyStrategy',
                          '--strategy-properties', 'dc1:1,dc2:1'])
        else: pass
            #cursor = self.patient_cql_connection(node1)
            #cursor.execute("""
            #  CREATE KEYSPACE "Keyspace1" WITH replication =
            #  {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
            #""")
            #node1.stress(['write', 'n=200', '-rate', 'threads=1', '-col', 'size=FIXED(100)'])

        node3 = new_node(cluster, data_center='dc2')
        node3.start(no_wait=True)
        time.sleep(3)

        with tempfile.TemporaryFile(mode='w+') as tmpfile:
            if version < "2.1":
                node1.stress(['-o', 'insert', '-t', '5', '-e', 'LOCAL_QUORUM'],
                             stdout=tmpfile, stderr=subprocess.STDOUT)
            else: pass
                #node1.stress(['write', 'n=1', 'cl=LOCAL_QUORUM', '-rate', 'threads=8'])

            tmpfile.seek(0)
            output = tmpfile.read()

        debug(output)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(output)
        dsds
        self.assertEqual(failure, None, "Error during stress while bootstrapping")
