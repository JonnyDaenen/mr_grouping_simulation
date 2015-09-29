from cost_model2 import MR_cost_model_gumbo
from cost_model3 import MR_cost_model_io
from mrsettings import MR_settings, create_settings
from cost_model1 import MR_cost_model

import psycopg2
import psycopg2.extras

__author__ = 'Jonny Daenen'


def generate_parameters():
    costs_local_r = [1,10,100]
    costs_local_w = [1,10,100]
    costs_hdfs_w = [1,10,100,1000,10000]
    costs_hdfs_r = [1,10,100,1000,10000]
    costs_transfer = [1,10,100,1000,10000]
    costs_sort = [0.1,1,10,100,1000,10000]
    costs_red = [0.1,1,10,100,1000]

    return [(lr,lw,hr,hw,t,s,r) for r in costs_red
            for s in costs_sort
            for t in costs_transfer
            for hw in costs_hdfs_w
            for hr in costs_hdfs_r
            for lw in costs_local_w
            for lr in costs_local_r]

def is_correct(cpu1, metric1, cpu2, metric2):
    return (cpu1 < cpu2 and metric1 < metric2) or (cpu1 > cpu2 and metric1 > metric2)


def calculate_speedup(time1, time2):
    return (time1 - time2) / float(time1)

def report(jobtimes):
    counter = 0
    correct = 0
    speedup_error = 0
    speedup_max_error = 0
    speedup_min_error = 10000000
    for job1 in jobtimes.keys():
        for job2 in jobtimes.keys():

            (cpu1, metric1, opts1) = jobtimes[job1]
            (cpu2, metric2, opts2) = jobtimes[job2]

            if job1 < job2 and opts1 != opts2:
                counter += 1

                if is_correct(cpu1, metric1, cpu2, metric2):
                    correct += 1

                error = abs(calculate_speedup(cpu1,cpu2) - calculate_speedup(metric1, metric2))
                speedup_error += error
                speedup_max_error = max(speedup_max_error,error)
                speedup_min_error = min(speedup_min_error,error)



    # print counter, correct, correct / float(counter), speedup_error/float(counter)
    # print "min error:", speedup_min_error
    # print "max error:", speedup_max_error

    return correct / float(counter), speedup_error/float(counter), speedup_min_error, speedup_max_error


def test(params=None):

    conn = psycopg2.connect("dbname=jonny user=jonny")

    cur = conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor)
    insert_cur = conn.cursor()
    cur.execute("SELECT exp, opts, job, cpu_millis, map_millis, red_millis, hdfs_bytes_read, map_output_bytes, ASSERT_BYTES_R1, REQUEST_BYTES FROM jobs;")

    jobtimes = {}
    # for each job
    # print "exp", "opts", "job", \
    #     "cpu_millis", "map_millis", "red_millis", "hdfs_bytes_read", "map_output_bytes", \
    #     "map_cost", "red_cost", "total_cost"
    for record in cur:

        if record.request_bytes == 0 or record.assert_bytes_r1 == 0:
            continue

        # cost_model = MR_cost_model(create_settings(record.exp, record.opts,params))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read, record.map_output_bytes, False, False, True)

        cost_model = MR_cost_model_gumbo(create_settings(record.exp, record.opts,params))
        cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(2*1024.0**2), record.hdfs_bytes_read/(2*1024.0**2), record.request_bytes/(1024.0**2), record.assert_bytes_r1/(1024.0**2), True)

        # cost_model = MR_cost_model_io(create_settings(record.exp, record.opts,params))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(2*1024.0**2), record.hdfs_bytes_read/(2*1024.0**2), record.request_bytes/(1024.0**2), record.assert_bytes_r1/(1024.0**2), True)


        # print record.exp, record.opts, record.job,
        # print record.assert_bytes_r1, record.request_bytes,
        # print record.cpu_millis, record.map_millis, record.red_millis,
        # print cost[0], cost[1], sum(cost[0:2])
        # print cost[2], cost[3], cost[4]

        jobtimes[record.job] = (record.cpu_millis,sum(cost[0:2]),record.opts)

        # insert_cur.execute("INSERT INTO cost_estimations (job_id, map_cost, shuffle_cost, merge_cost, red_function_cost, red_cost, total_cost, cost_model) "
        #                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        #                     (record.job, cost[0], cost[2], cost[3], cost[4], cost[1], sum(cost[0:2]), "test-v2"))

    conn.commit()

    cur.close()

    conn.close()

    return report(jobtimes)




if __name__ == '__main__':

    # correct
    best_correct = None
    best_correct_params = None

    # avg speedup error
    best_speeduperr = None
    best_speeduperr_params = None

    # min error
    best_minerr = None
    best_minerr_params = None

    # max error
    best_maxerr = None
    best_maxerr_params = None

    paramset = generate_parameters()
    i = 0
    for params in paramset:
        result = test(params)
        print result
        i += 1
        if i == 100:
            break

        if result[0] > best_correct:
            best_correct = result[0]
            best_correct_params = params

        if result[1] < best_speeduperr:
            best_speeduperr = result[0]
            best_speeduperr_params = params

        if result[2] < best_minerr:
            best_minerr = result[0]
            best_minerr_params = params

        if result[3] < best_maxerr:
            best_maxerr = result[0]
            best_maxerr_params = params

    print "best_correct: ", best_correct, best_correct_params
    print "best_speeduperr: ", best_speeduperr, best_speeduperr_params
    print "best_minerr: ", best_minerr, best_minerr_params
    print "best_maxerr: ", best_maxerr, best_maxerr_params
