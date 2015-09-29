from cost_model2 import MR_cost_model_gumbo
from mrsettings import MR_settings, create_settings
from cost_model1 import MR_cost_model

import psycopg2
import psycopg2.extras

__author__ = 'Jonny Daenen'


def is_correct(cpu1, metric1, cpu2, metric2):
    return (cpu1 < cpu2 and metric1 < metric2) or (cpu1 > cpu2 and metric1 > metric2)


def calculate_speedup(time1, time2):
    return (time1 - time2) / float(time1)


if __name__ == '__main__':


    conn = psycopg2.connect("dbname=jonny user=jonny")

    cur = conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor)
    insert_cur = conn.cursor()
    cur.execute("SELECT exp, opts, job, cpu_millis, map_millis, red_millis, hdfs_bytes_read, map_output_bytes, ASSERT_BYTES_R1, REQUEST_BYTES FROM jobs;")

    jobtimes = {}
    # for each job
    print "exp", "opts", "job", \
        "cpu_millis", "map_millis", "red_millis", "hdfs_bytes_read", "map_output_bytes", \
        "map_cost", "red_cost", "total_cost"
    for record in cur:

        if record.request_bytes == 0 or record.assert_bytes_r1 == 0:
            continue

        # cost_model = MR_cost_model(create_settings(record.exp, record.opts))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read, record.map_output_bytes, False, False, True)

        cost_model = MR_cost_model_gumbo(create_settings(record.exp, record.opts))
        cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(2*1024.0**2), record.hdfs_bytes_read/(2*1024.0**2), record.request_bytes/(1024.0**2), record.assert_bytes_r1/(1024.0**2), True)

        print record.exp, record.opts, record.job,
        print record.assert_bytes_r1, record.request_bytes,
        print record.cpu_millis, record.map_millis, record.red_millis,
        print cost[0], cost[1], sum(cost[0:2])
        print cost[2], cost[3], cost[4]

        jobtimes[record.job] = (record.cpu_millis,sum(cost[0:2]),record.opts)

        # insert_cur.execute("INSERT INTO cost_estimations (job_id, map_cost, shuffle_cost, merge_cost, red_function_cost, red_cost, total_cost, cost_model) "
        #                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        #                     (record.job, cost[0], cost[2], cost[3], cost[4], cost[1], sum(cost[0:2]), "test-v2"))

    conn.commit()

    cur.close()

    conn.close()


    counter = 0
    correct = 0
    speedup_error = 0
    for job1 in jobtimes.keys():
        for job2 in jobtimes.keys():

            (cpu1, metric1, opts1) = jobtimes[job1]
            (cpu2, metric2, opts2) = jobtimes[job2]

            if job1 < job2 and opts1 != opts2:
                counter += 1

                if is_correct(cpu1, metric1, cpu2, metric2):
                    correct += 1

                speedup_error += calculate_speedup(cpu1,cpu2) - calculate_speedup(metric1, metric2)



    print counter, correct, correct / float(counter), speedup_error/float(counter)





