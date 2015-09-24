from mrsettings import MR_settings
from simulator import MR_cost_model

import psycopg2
import psycopg2.extras

__author__ = 'Jonny Daenen'


def create_settings(exp, opts):

    settings = MR_settings()

    if "021" in exp:
        settings.mapper_memory_mb = 2048
        settings.reducer_memory_mb = 4096
        settings.red_chunk_size_mb = 1024
        if "test" in opts:
            settings.mapper_memory_mb = 1024
            settings.reducer_memory_mb = 1024

    if "022" in exp:
        settings.mapper_memory_mb = 1024
        settings.reducer_memory_mb = 1024


    if "128" in opts:
        settings.red_chunk_size_mb = 128
    if "64" in opts:
            settings.red_chunk_size_mb = 128

    return settings


if __name__ == '__main__':


    conn = psycopg2.connect("dbname=jonny user=jonny")

    cur = conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor)
    insert_cur = conn.cursor()
    cur.execute("SELECT exp, opts, job, cpu_millis, map_millis, red_millis, hdfs_bytes_read, map_output_bytes FROM jobs;")

    # for each job
    print "exp", "opts", "job", \
        "cpu_millis", "map_millis", "red_millis", "hdfs_bytes_read", "map_output_bytes", \
        "map_cost", "red_cost", "total_cost"
    for record in cur:
        cost_model = MR_cost_model(create_settings(record.exp, record.opts))
        cost = cost_model.get_mr_cost(record.hdfs_bytes_read, record.map_output_bytes, False, False, True)

        print record.exp, record.opts, record.job,
        print record.cpu_millis, record.map_millis, record.red_millis,
        print cost[0], cost[1], sum(cost[0:2])
        print cost[2], cost[3], cost[4]

        insert_cur.execute("INSERT INTO cost_estimations (job_id, map_cost, shuffle_cost, merge_cost, red_function_cost, red_cost, total_cost, cost_model) "
                           "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                            (record.job, cost[0], cost[2], cost[3], cost[4], cost[1], sum(cost[0:2]), "test-with-settings"))

    conn.commit()

    cur.close()

    conn.close()



        # get input data (hdfs reads)
        # get intermediate data (map output bytes)

        # get map and red millis
        # get CPU time


        # estimate




