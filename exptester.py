from mrsettings import MR_settings
from simulator import MR_cost_model

import psycopg2
import psycopg2.extras

__author__ = 'Jonny Daenen'


if __name__ == '__main__':

    cost_model = MR_cost_model(MR_settings())

    conn = psycopg2.connect("dbname=jonny user=jonny")

    cur = conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor)
    cur.execute("SELECT job, cpu_millis, map_millis, red_millis, hdfs_bytes_read, map_output_bytes FROM jobs;")

    # for each job
    for record in cur:
        cost = cost_model.get_mr_cost(record.hdfs_bytes_read,record.map_output_bytes,False,False)
        print record.job, record.cpu_millis, record.map_millis, record.red_millis, cost[0], cost[1], sum(cost)


    cur.close()

    conn.close()



        # get input data (hdfs reads)
        # get intermediate data (map output bytes)

        # get map and red millis
        # get CPU time


        # estimate




