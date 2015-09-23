__author__ = 'Jonny Daenen'

import math



# costs
cost_local_r = 1
cost_local_w = 1
cost_hdfs_w = 330
cost_hdfs_r = 59
cost_transfer = 5

# map settings
map_chunk_size_mb = 128
mapper_memory_mb = 1024
map_merge_factor = 10
map_sort_buffer_mb = 100

# red settings
red_chunk_size_mb = 128
red_memory_mb = 1024
red_merge_factor = 10
red_sort_buffer_mb = 700 # TODO check if this is the correct value


def get_mr_cost(data_input_mb, data_interm_mb, sort_correction_enabled = True, reduce_in_memory = True, ):

    print "input Mbytes:", data_input_mb
    print "intermediate Mbytes:", data_interm_mb
    # map_chunk_size_mb = data_input_mb/10
    # print "map chunck size: ", map_chunk_size_mb

    # map calculation
    map_tasks = math.ceil(data_input_mb / float(map_chunk_size_mb))
    map_pieces =  map_chunk_size_mb / float(map_sort_buffer_mb)
    map_merge_levels = math.ceil(math.log(map_pieces,map_merge_factor)) # do we need max(1,x)?

    print "map tasks: ", map_tasks
    print "map task pieces: ", map_pieces
    print "map tasks merge levels: ", map_merge_levels

    # extra parts are not read every level
    if sort_correction_enabled:
        map_sort_correction_mb = (map_pieces % map_merge_factor) * map_sort_buffer_mb * map_tasks
    else:
        map_sort_correction_mb = 0

    map_in_read_cost = data_input_mb * cost_local_r
    map_out_initial_write_cost = data_interm_mb * cost_local_w
    map_sort_cost = map_merge_levels * (data_interm_mb - map_sort_correction_mb) * (cost_local_r + cost_local_w)
    map_sort_cost += map_sort_correction_mb * (cost_local_r + cost_local_w)

    print "map read cost: ", map_in_read_cost
    print "map write cost: ", map_out_initial_write_cost
    print "map sort cost: ", map_sort_cost



    # +1 is needed when extra dump to disk is done before starting reducer
    # TODO this depends on mapreduce.reduce.input.buffer.percent (when non-zero, the -1 can be dropped)
    if reduce_in_memory:
        red_inmem_correction = 1
    else:
        red_inmem_correction = 0

    # reduce calculation
    red_tasks = math.ceil(data_interm_mb / float(red_chunk_size_mb))
    red_pieces =  red_chunk_size_mb / float(red_sort_buffer_mb)
    red_merge_levels = math.ceil(math.log(red_pieces,red_merge_factor)) + red_inmem_correction

    print "red tasks: ", red_tasks
    print "red task pieces: ", red_pieces
    print "red tasks merge levels: ", red_merge_levels

    # extra parts are not read every level
    if sort_correction_enabled:
        red_sort_correction_mb = (red_pieces % red_merge_factor) * red_sort_buffer_mb * red_tasks
    else:
        red_sort_correction_mb = 0

    red_in_read_cost = data_interm_mb * cost_transfer
    # initial write cost is captured by last write costs that is not needed
    print data_interm_mb - red_sort_correction_mb
    red_sort_cost = red_merge_levels * (data_interm_mb - red_sort_correction_mb) * (cost_local_r + cost_local_w)
    red_sort_cost += red_sort_correction_mb * (cost_local_r + cost_local_w)

    print "red read cost: ", red_in_read_cost
    print "red sort cost: ", red_sort_cost


    total_map = map_in_read_cost + map_out_initial_write_cost + map_sort_cost
    total_red = red_in_read_cost + red_sort_cost
    total = total_map + total_red
    print "TOTAL MAP:", total_map, total_map/total
    print "TOTAL RED:", total_red, total_red/total

    return (int(total_map),int(total_red))


inputs1 = {
    "q1" : [(2222222249, 2723599862),
              (2222222249, 2723599854),
              (2222222249, 4902755271)],

    "q2": [
        (2222222249,2823599854),
        (2222222249,2723599854),
        (4000000053,4902755263)
    ],

    "q3" : [
    (2222222249, 2723599854),
    (2222222249, 2723599854),
    (2666666694, 3568044299),
    ],

    "q4" : [
    (2222222249, 2723599854),
    (2222222249, 2723599854),
    (4444444498, 5547199708),
    ],

    "q5" : [
    (2222222249, 2723599854),
    (2222222249, 2723599854),
    (2222222249, 2723599854),
    (2222222249, 2723599854),
    (4444444498, 5947199708),
    ]
}

inputs2 = {
    "test1" : [(200000000, 200000000),
              (200000000, 200000000),
              (200000000, 500000000)],

    "test10" : [(2000000000, 2000000000),
              (2000000000, 2000000000),
              (2000000000, 5000000000)],

    "test100" : [(20000000000, 20000000000),
              (20000000000, 20000000000),
              (20000000000, 50000000000)],

    "test1000" : [(200000000000, 200000000000),
              (200000000000, 200000000000),
              (200000000000, 500000000000)],



}

# data
data_input_mb = 2222222249/(1024**2)
data_interm_mb = 5 * 1024

inputs = inputs1

results = {}
for key in inputs.keys():
    print  inputs[key]
    results[key] = []

    for (data_input_b, data_interm_b) in inputs[key]:
        mr = get_mr_cost(data_input_b/(1024**2), data_interm_b/(1024**2), False, True)
        results[key].append(mr)
        print mr

print
for key in results.keys():
    nogroup_sum = sum(map(sum,results[key][:-1]))
    group_sum = sum(results[key][-1])

    print key
    print "results:", results[key]
    print nogroup_sum, group_sum
    print (nogroup_sum-group_sum)/float(nogroup_sum), "% better"
    print "="*80

