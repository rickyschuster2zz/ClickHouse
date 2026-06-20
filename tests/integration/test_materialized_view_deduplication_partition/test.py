import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True, main_configs=['configs/keep_alive.xml'])
node2 = cluster.add_instance('node2', with_zookeeper=True, main_configs=['configs/keep_alive.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_materialized_view_deduplication_partition(started_cluster):
    # Create source replicated table
    node1.query("""
        CREATE TABLE source_table (
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/source_table', 'node1')
        ORDER BY key;
    """)
    
    node2.query("""
        CREATE TABLE source_table (
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/source_table', 'node2')
        ORDER BY key;
    """)

    # Create target replicated table
    node1.query("""
        CREATE TABLE target_table (
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/target_table', 'node1')
        ORDER BY key;
    """)
    
    node2.query("""
        CREATE TABLE target_table (
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/target_table', 'node2')
        ORDER BY key;
    """)

    # Create Materialized View
    node1.query("""
        CREATE MATERIALIZED VIEW mv_target TO target_table AS
        SELECT key, value FROM source_table;
    """)

    # Insert some data with insert_deduplicate = 1
    node1.query("INSERT INTO source_table SETTINGS insert_deduplicate=1 VALUES (1, 'val1')", query_id="dedup_token_1")

    # Simulate network partition by blocking ZooKeeper
    cluster.pause_container('zoo1')
    
    # Try to insert again (this should fail or retry)
    try:
        node1.query("INSERT INTO source_table SETTINGS insert_deduplicate=1 VALUES (1, 'val1')", query_id="dedup_token_1", timeout=5)
    except Exception:
        pass

    # Restore ZooKeeper
    cluster.unpause_container('zoo1')
    
    # Retry the insert after reconnection
    node1.query("INSERT INTO source_table SETTINGS insert_deduplicate=1 VALUES (1, 'val1')", query_id="dedup_token_1")

    # Check that source table has no duplicates
    res_source = node1.query("SELECT count() FROM source_table")
    assert int(res_source.strip()) == 1

    # Check that target table has no duplicates
    res_target = node1.query("SELECT count() FROM target_table")
    assert int(res_target.strip()) == 1