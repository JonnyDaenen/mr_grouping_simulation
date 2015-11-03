from cost_model0 import MR_cost_model_basic
from cost_model2 import MR_cost_model_gumbo
from cost_model3 import MR_cost_model_io
from mrsettings import MR_settings, create_settings
from cost_model1 import MR_cost_model

import psycopg2
import psycopg2.extras

__author__ = 'Jonny Daenen'


def generate_parameters():

    return []

    costs_local_r = [1,10,100]
    costs_local_w = [1,10,100]
    costs_hdfs_w = [1,5,10,50,100,1000,10000]
    costs_hdfs_r = [1,5,10,50,100,1000,10000]
    costs_transfer = [1,5,10,50,100,1000,10000]
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


def reorder(cpu1, cpu2, metric1, metric2):
    if cpu1 < cpu2:
        help = cpu1
        cpu1 = cpu2
        cpu2 = help
        help = metric1
        metric1 = metric2
        metric2 = help
    return cpu1, cpu2, metric1, metric2




def report(jobtimes):


    file = open("output_io.csv", "w")

    counter = 0
    correct = 0
    speedup_error = 0
    speedup_max_error = 0
    speedup_min_error = 10000000
    for job1 in jobtimes.keys():
        for job2 in jobtimes.keys():

            (cpu1, metric1, opts1) = jobtimes[job1]
            (cpu2, metric2, opts2) = jobtimes[job2]

            if job1 < job2 :
                counter += 1

                if is_correct(cpu1, metric1, cpu2, metric2):
                    correct += 1

                # normalize
                (cpu1,cpu2,metric1,metric2) = reorder(cpu1, cpu2, metric1, metric2)

                # calculate speedup error
                error = abs(calculate_speedup(cpu1, cpu2) - calculate_speedup(metric1, metric2))
                speedup_error += error
                speedup_max_error = max(speedup_max_error,error)
                speedup_min_error = min(speedup_min_error,error)

                file.write("paper,"+job1+","+job2+","+str(error)+"\n")

    file.close()

    # print counter, correct, correct / float(counter), speedup_error/float(counter)
    # print "min error:", speedup_min_error
    # print "max error:", speedup_max_error

    return correct / float(counter), speedup_error/float(counter), speedup_min_error, speedup_max_error


def get_data():
    conn = psycopg2.connect("dbname=jonny user=jonny")

    cur = conn.cursor(cursor_factory = psycopg2.extras.NamedTupleCursor)
    cur.execute("SELECT exp, opts, job, cpu_millis, map_millis, red_millis, hdfs_bytes_read, map_output_bytes, ASSERT_BYTES_R1, REQUEST_BYTES FROM jobs ;")  # WHERE exp ='EXP_022'

    jobs = cur.fetchall();

    cur.close()
    conn.close()

    return jobs


def test(job_records, params=None):

    # Calculate costs
    jobtimes = {}
    # for each job
    # print "exp", "opts", "job", \
    #     "cpu_millis", "map_millis", "red_millis", "hdfs_bytes_read", "map_output_bytes", \
    #     "map_cost", "red_cost", "total_cost"
    for record in job_records:

        if record.request_bytes == 0 or record.assert_bytes_r1 == 0:
            continue

        # cost_model = MR_cost_model(create_settings(record.exp, record.opts,params))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(1024.0**2), record.map_output_bytes/(1024.0**2), False, False, True)

        cost_model = MR_cost_model_gumbo(create_settings(record.exp, record.opts,params))
        cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(2*1024.0**2), record.hdfs_bytes_read/(2*1024.0**2), record.request_bytes/(1024.0**2), record.assert_bytes_r1/(1024.0**2), True)

        # cost_model = MR_cost_model_io(create_settings(record.exp, record.opts,params))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(2*1024.0**2), record.hdfs_bytes_read/(2*1024.0**2), record.request_bytes/(1024.0**2), record.assert_bytes_r1/(1024.0**2), True)


        # cost_model = MR_cost_model_basic(create_settings(record.exp, record.opts, params))
        # cost = cost_model.get_mr_cost(record.hdfs_bytes_read/(1024.0**2), record.map_output_bytes/(1024.0**2), False, True)


        # print record.exp, record.opts, record.job,
        # print record.assert_bytes_r1, record.request_bytes,
        # print record.cpu_millis, record.map_millis, record.red_millis,
        # print cost[0], cost[1], sum(cost[0:2])
        # print cost[2], cost[3], cost[4]

        # jobtimes[record.job] = (record.cpu_millis, sum(cost[0:2]), record.opts)
        jobtimes[record.job] = (record.map_millis + record.red_millis, sum(cost[0:2]), record.opts)

        # insert_cur.execute("INSERT INTO cost_estimations (job_id, map_cost, shuffle_cost, merge_cost, red_function_cost, red_cost, total_cost, cost_model) "
        #                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        #                     (record.job, cost[0], cost[2], cost[3], cost[4], cost[1], sum(cost[0:2]), "test-v2"))


    return report(jobtimes)


def print_intermediate():
    return
    print "best_correct: ", best_correct, best_correct_params, best_correct_result
    print "best_speeduperr: ", best_speeduperr, best_speeduperr_params, best_speeduperr_result
    print "best_minerr: ", best_minerr, best_minerr_params, best_minerr_result
    print "best_maxerr: ", best_maxerr, best_maxerr_params, best_maxerr_result


def print_progress(i, paramset):
    if i % 1000 == 0:
            print str(float(i)/len(paramset)) + "% done:", i, "out of", len(paramset)
            print_intermediate()


if __name__ == '__main__':

    jobs = get_data()

    print "normal results: ", test(jobs, None)

    # correct
    best_correct = None
    best_correct_params = None
    best_correct_result = None

    # avg speedup error
    best_speeduperr = 100000
    best_speeduperr_params = None
    best_speeduperr_result = None

    # min error
    best_minerr = 100000
    best_minerr_params = None
    best_minerr_result = None

    # max error
    best_maxerr = 100000
    best_maxerr_params = None
    best_maxerr_result = None

    paramset = generate_parameters()
    print "testing", len(paramset), "parameters"
    print "testing", len(jobs), "jobs"
    i = 0
    for params in paramset:
        result = test(jobs, params)
        # print result
        i += 1
        print_progress(i, paramset)

        if result[0] > best_correct:
            best_correct = result[0]
            best_correct_params = params
            best_correct_result = result

        if result[1] < best_speeduperr:
            best_speeduperr = result[1]
            best_speeduperr_params = params
            best_speeduperr_result = result

        if result[2] < best_minerr:
            best_minerr = result[2]
            best_minerr_params = params
            best_minerr_result = result

        if result[3] < best_maxerr:
            best_maxerr = result[3]
            best_maxerr_params = params
            best_maxerr_result = result

    print "best_correct: ", best_correct, best_correct_params, best_correct_result
    print "best_speeduperr: ", best_speeduperr, best_speeduperr_params, best_speeduperr_result
    print "best_minerr: ", best_minerr, best_minerr_params, best_minerr_result
    print "best_maxerr: ", best_maxerr, best_maxerr_params, best_maxerr_result
