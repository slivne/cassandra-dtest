import os, glob, re, uuid, subprocess

from ccmlib import common
from dtest import Tester, debug
from tools import since

KEYSPACE = 'ks'

class TestScrub(Tester):

    """ Return the path where the table sstables are located """
    def get_table_path(self, table):
        node1 = self.cluster.nodelist()[0]
        path = ""
        basepath = os.path.join(node1.get_path(), 'data', KEYSPACE)
        for x in os.listdir(basepath):
            if x.startswith(table):
                path = os.path.join(basepath, x)
                break
        return path

    """ Return the path where the index sstables are located """
    def get_index_path(self, table, index):
        path = self.get_table_path(table)
        return os.path.join(path, '.' + index)

    """ Return the sstable files at a specific location """
    def get_sstable_files(self, path):
        ret = []
        debug('Checking sstables in %s' % (path))
        for fname in glob.glob(os.path.join(path,'*.db')):
            bname = os.path.basename(fname)
            debug('Found sstable %s' % (bname))
            ret.append(bname)
        return ret

    """ Return the sstables for a table and the specified indexes of this table """
    def get_sstables(self, table, indexes):
        ret = self.get_sstable_files(self.get_table_path(table))
        assert len(ret) > 0

        for index in indexes:
            index_ret = self.get_sstable_files(self.get_index_path(table, index))
            assert len(index_ret) > 0
            for ir in index_ret:
                ret.append('%s/%s' % (index, ir))

        return sorted(ret)

    """ Launch a nodetool command and check the result is empty (no error) """
    def launch_nodetool_cmd(self, cmd):
        node1 = self.cluster.nodelist()[0]
        response = node1.nodetool(cmd, capture_output=True)[0]
        assert len(response) == 0 # nodetool does not print anything unless there is an error

    """ Launch the standalone scrub """
    def launch_standalone_scrub(self, ks, cf):
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        scrub_bin = node1.get_tool('sstablescrub')
        debug(scrub_bin)

        args = [ scrub_bin, ks, cf]
        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        response = p.communicate()
        debug(response)

    """ Perform a nodetool command on a table and the indexes specified """
    def perform_node_tool_cmd(self, cmd, table, indexes):
        self.launch_nodetool_cmd('%s %s %s' % (cmd, KEYSPACE, table))
        for index in indexes:
            self.launch_nodetool_cmd('%s %s %s.%s' % (cmd, KEYSPACE, table, index))

    """ Flush table and indexes via nodetool, return all sstables """
    def flush(self, table, *indexes):
        self.perform_node_tool_cmd('flush', table, indexes)
        return self.get_sstables(table, indexes)

    """ Scrub table and indexes via nodetool, return all sstables """
    def scrub(self, table, *indexes):
        self.perform_node_tool_cmd('scrub', table, indexes)
        return self.get_sstables(table, indexes)

    """ Launch standalone scrub on table and indexes, return all sstables """
    def standalonescrub(self, table, *indexes):
        self.launch_standalone_scrub(KEYSPACE, table)
        for index in indexes:
            self.launch_standalone_scrub(KEYSPACE, '%s.%s' % (table, index))
        return self.get_sstables(table, indexes)

    def increment_generation_by(self, sstable, generation_increment):
        """ Sets the generation number for an sstable file name """
        return re.sub('\d(?!\d)', lambda x: str(int(x.group(0)) + generation_increment), sstable)

    def increase_sstable_generations(self, sstables):
        """
        After finding the number of existing sstables, this increases all of the
        generations by that amount.
        """
        increment_by = len(set(re.match('.*(\d)[^0-9].*', s).group(1) for s in sstables))
        sstables[:] = [self.increment_generation_by(s, increment_by) for s in sstables]
        debug('sstables after increment %s' % (str(sstables)))

    def create_users(self, cursor):
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(cursor, 'users', columns=columns)

        cursor.execute("CREATE INDEX gender_idx ON users (gender)")
        cursor.execute("CREATE INDEX state_idx ON users (state)")
        cursor.execute("CREATE INDEX birth_year_idx ON users (birth_year)")

    def update_users(self, cursor):
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1978)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1982)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'TX', 1978)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'CA', 1982)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user5', 'ch@ngem3e', 'f', 'TX', 1978)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user6', 'ch@ngem3f', 'm', 'CA', 1982)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user7', 'ch@ngem3g', 'f', 'TX', 1978)")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user8', 'ch@ngem3h', 'm', 'CA', 1982)")

        cursor.execute("DELETE FROM users where KEY = 'user1'")
        cursor.execute("DELETE FROM users where KEY = 'user5'")
        cursor.execute("DELETE FROM users where KEY = 'user7'")

    def query_users(self, cursor):
        ret = cursor.execute("SELECT * FROM users")
        ret.extend(cursor.execute("SELECT * FROM users WHERE state='TX'"))
        ret.extend(cursor.execute("SELECT * FROM users WHERE gender='f'"))
        ret.extend(cursor.execute("SELECT * FROM users WHERE birth_year=1978"))
        assert len(ret) == 8
        return ret

    @since('3.0')
    def test_scrub_static_table(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, KEYSPACE, 1)

        self.create_users(cursor)
        self.update_users(cursor)

        initial_users = self.query_users(cursor)
        initial_sstables = self.flush('users', 'gender_idx', 'state_idx', 'birth_year_idx')
        scrubbed_sstables = self.scrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        self.increase_sstable_generations(initial_sstables)
        self.assertListEqual(initial_sstables, scrubbed_sstables)

        users = self.query_users(cursor)
        self.assertListEqual(initial_users, users)

        # Scrub and check sstables and data again
        scrubbed_sstables = self.scrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')
        self.increase_sstable_generations(initial_sstables)
        self.assertListEqual(initial_sstables, scrubbed_sstables)

        users = self.query_users(cursor)
        self.assertListEqual(initial_users, users)

        # Restart and check data again
        cluster.stop()
        cluster.start()

        cursor = self.patient_cql_connection(node1)
        cursor.execute('USE %s' % (KEYSPACE))

        users = self.query_users(cursor)
        self.assertListEqual(initial_users, users)

    @since('3.0')
    def test_standalone_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, KEYSPACE, 1)

        self.create_users(cursor)
        self.update_users(cursor)

        initial_users = self.query_users(cursor)
        initial_sstables = self.flush('users', 'gender_idx', 'state_idx', 'birth_year_idx')

        cluster.stop()

        scrubbed_sstables = self.standalonescrub('users', 'gender_idx', 'state_idx', 'birth_year_idx')
        self.increase_sstable_generations(initial_sstables)
        self.assertListEqual(initial_sstables, scrubbed_sstables)

        cluster.start()
        cursor = self.patient_cql_connection(node1)
        cursor.execute('USE %s' % (KEYSPACE))

        users = self.query_users(cursor)
        self.assertListEqual(initial_users, users)

    @since('3.0')
    def test_scrub_collections_table(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, KEYSPACE, 1)

        cursor.execute("CREATE TABLE users (user_id uuid PRIMARY KEY, email text, uuids list<uuid>)")
        cursor.execute("CREATE INDEX user_uuids_idx on users (uuids)")

        _id = uuid.uuid4()
        num_users = 100
        for i in range(0, num_users):
            user_uuid = uuid.uuid4()
            cursor.execute(("INSERT INTO users (user_id, email) values ({user_id}, 'test@example.com')").format(user_id=user_uuid))
            cursor.execute(("UPDATE users set uuids = [{id}] where user_id = {user_id}").format(id=_id, user_id=user_uuid))

        initial_users = cursor.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id))
        self.assertEqual(num_users, len(initial_users))

        initial_sstables = self.flush('users', 'user_uuids_idx')
        scrubbed_sstables = self.scrub('users', 'user_uuids_idx')

        self.increase_sstable_generations(initial_sstables)
        self.assertListEqual(initial_sstables, scrubbed_sstables)

        users = cursor.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id))
        self.assertListEqual(initial_users, users)

        scrubbed_sstables = self.scrub('users', 'user_uuids_idx')

        self.increase_sstable_generations(initial_sstables)
        self.assertListEqual(initial_sstables, scrubbed_sstables)

        users = cursor.execute(("SELECT * from users where uuids contains {some_uuid}").format(some_uuid=_id))
        self.assertListEqual(initial_users, users)
