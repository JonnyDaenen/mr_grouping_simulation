from mrsettings import MR_settings

__author__ = 'Jonny Daenen'

import math


class MR_cost_model:


    def __init__(self, mr_settings):
        self.mr_settings = mr_settings
        self.print_enabled = False


    def report(self, *msg):
        if self.print_enabled:
            print reduce(lambda x,y: str(x) + str(y), msg)

    def get_mr_cost(self, data_input_mb, data_interm_mb, sort_correction_enabled=False, reduce_in_memory=True, extendedResult=False):
        """

        :param data_input_mb: size of file system input
        :param data_interm_mb: size of intermediate output (reducer input)
        :param sort_correction_enabled: True if sort-correction should be applied (experimental)
        :param reduce_in_memory: True when last stage can be performed in-memory
                                (depends on mapreduce.reduce.input.buffer.percent)
        :return: A tuple containing map cost and reduce cost.
        """

        mr_settings = self.mr_settings

        self.report("input Mbytes:", data_input_mb)
        self.report( "intermediate Mbytes:", data_interm_mb)
        # map_chunk_size_mb = data_input_mb/10
        # print "map chunck size: ", map_chunk_size_mb

        # map calculation
        map_tasks = math.ceil(data_input_mb / float(mr_settings.map_chunk_size_mb))
        map_pieces = mr_settings.map_chunk_size_mb / float(mr_settings.map_sort_buffer_mb)
        map_merge_levels = math.ceil(math.log(map_pieces, mr_settings.map_merge_factor))  # do we need max(1,x)?

        self.report("map tasks: ", map_tasks)
        self.report("map task pieces: ", map_pieces)
        self.report("map tasks merge levels: ", map_merge_levels)

        # extra parts are not read every level
        if sort_correction_enabled:
            map_sort_correction_mb = (map_pieces % mr_settings.map_merge_factor) * mr_settings.map_sort_buffer_mb * map_tasks
        else:
            map_sort_correction_mb = 0

        map_in_read_cost = data_input_mb * mr_settings.cost_local_r
        map_out_initial_write_cost = data_interm_mb * mr_settings.cost_local_w
        map_merge_cost = map_merge_levels * (data_interm_mb - map_sort_correction_mb) * (mr_settings.cost_local_r + mr_settings.cost_local_w)
        map_merge_cost += map_sort_correction_mb * (mr_settings.cost_local_r + mr_settings.cost_local_w)

        map_sort_cost = map_pieces * mr_settings.cost_sort

        self.report("map read cost: ", map_in_read_cost)
        self.report("map write cost: ", map_out_initial_write_cost)
        self.report("map merge cost: ", map_merge_cost)
        self.report("map sort cost: ", map_sort_cost)



        # +1 is needed when extra dump to disk is done before starting reducer
        # TODO this depends on mapreduce.reduce.input.buffer.percent (when non-zero, the -1 can be dropped)
        if reduce_in_memory:
            red_inmem_correction = 0
        else:
            red_inmem_correction = 1

        # reduce calculation
        red_tasks = math.ceil(data_interm_mb / float(mr_settings.red_chunk_size_mb))
        red_pieces = mr_settings.red_chunk_size_mb / float(mr_settings.red_sort_buffer_mb)
        red_merge_levels = math.ceil(math.log(red_pieces, mr_settings.red_merge_factor)) + red_inmem_correction

        self.report("red tasks: ", red_tasks)
        self.report("red task pieces: ", red_pieces)
        self.report("red tasks merge levels: ", red_merge_levels)

        # extra parts are not read every level
        if sort_correction_enabled:
            red_sort_correction_mb = (red_pieces % mr_settings.red_merge_factor) * mr_settings.red_sort_buffer_mb * red_tasks
        else:
            red_sort_correction_mb = 0

        red_in_read_cost = data_interm_mb * mr_settings.cost_transfer
        # initial write cost is captured by last write costs that is not needed
        # print data_interm_mb - red_sort_correction_mb # todo fix this
        red_merge_cost = red_merge_levels * (data_interm_mb - red_sort_correction_mb) * (mr_settings.cost_local_r + mr_settings.cost_local_w)
        red_merge_cost += red_sort_correction_mb * (mr_settings.cost_local_r + mr_settings.cost_local_w)

        red_overhead_cost = red_tasks * map_tasks * 2 * mr_settings.cost_transfer
        red_transfer_cost = red_in_read_cost + red_overhead_cost

        red_sort_cost = 0
        red_reduce_cost = 0

        self.report("red read cost: ", red_in_read_cost)
        self.report("red overhead cost: ", red_overhead_cost)
        self.report("red reduce cost: ", red_reduce_cost)
        self.report("red merge cost: ", red_merge_cost)
        self.report("red sort cost: ", red_sort_cost)

        total_map = map_in_read_cost + map_out_initial_write_cost + map_merge_cost
        total_red =  red_transfer_cost + red_merge_cost
        total = total_map + total_red
        self.report("TOTAL MAP:", total_map, total_map / total)
        self.report("TOTAL RED:", total_red, total_red / total)

        result = [int(total_map), int(total_red)]

        if extendedResult:
            result.append(red_transfer_cost)
            result.append(red_merge_cost)
            result.append(red_reduce_cost)
        return result


def calculate_pct(nogroup, group):
    return (nogroup - group) / float(nogroup)


def redistr_mr(mr_tuple):
    (m,r) = mr_tuple
    pct = r/float((m+r))
    r = m * (0.44 / 0.56)
    return m,r


if __name__ == '__main__':
    inputs1 = {
        "q1": [(2222222249, 2723599862),
               (2222222249, 2723599854),
               (2222222249, 4902755271)],

        "q2": [
            (2222222249, 2823599854),
            (2222222249, 2723599854),
            (4000000053, 4902755263)
        ],

        "q3": [
            (2222222249, 2723599854),
            (2222222249, 2723599854),
            (2666666694, 3568044299),
        ],

        "q4": [
            (2222222249, 2723599854),
            (2222222249, 2723599854),
            (4444444498, 5547199708),
        ],

        "q5": [
            (2222222249, 2723599854),
            (2222222249, 2723599854),
            (2222222249, 2723599854),
            (2222222249, 2723599854),
            (4444444498, 5947199708),
        ]
    }

    inputs2 = {
        "test1": [(200000000, 200000000),
                  (200000000, 200000000),
                  (200000000, 500000000)],

        "test10": [(2000000000, 2000000000),
                   (2000000000, 2000000000),
                   (2000000000, 5000000000)],

        "test100": [(20000000000, 20000000000),
                    (20000000000, 20000000000),
                    (20000000000, 50000000000)],

        "test1000": [(200000000000, 200000000000),
                     (200000000000, 200000000000),
                     (200000000000, 500000000000)],

    }

    # data
    data_input_mb = 2222222249 / (1024 ** 2)
    data_interm_mb = 5 * 1024

    inputs = inputs1 # {"q5" : inputs1["q5"]}


    cost_model = MR_cost_model(MR_settings())
    results = {}
    for key in inputs.keys():
        print key, inputs[key]
        results[key] = []

        for (data_input_b, data_interm_b) in inputs[key]:
            mr = cost_model.get_mr_cost(data_input_b / (1024 ** 2), data_interm_b / (1024 ** 2), False, False)
            results[key].append(mr)
            print mr
        print "~" * 80

    print




    for key in results.keys():

        nogroup_mr_sum = reduce(lambda (a,b), (c,d): (a+c, b+d), results[key][:-1])
        group_mr_sum = results[key][-1]
        nogroup_sum = sum(nogroup_mr_sum)
        group_sum = sum(group_mr_sum)

        print key
        print "results:", results[key]
        print "MR:", nogroup_mr_sum, group_mr_sum
        print "Total:", nogroup_sum, group_sum
        print "Map impr %:", calculate_pct(nogroup_mr_sum[0], group_mr_sum[0])
        print "Red impr %:", calculate_pct(nogroup_mr_sum[1], group_mr_sum[1])
        print "Map share %:", nogroup_mr_sum[0] / float(nogroup_sum), group_mr_sum[0] / float(group_sum)
        print "Red share %:", nogroup_mr_sum[1] / float(nogroup_sum), group_mr_sum[1] / float(group_sum)
        print "Total %:", calculate_pct(nogroup_sum, group_sum)

        red_share = nogroup_mr_sum[1] / float(nogroup_sum)
        results[key] = map(lambda mr_tuple: redistr_mr(mr_tuple), results[key])

        nogroup_mr_sum = reduce(lambda (a,b), (c,d): (a+c, b+d), results[key][:-1])
        group_mr_sum = results[key][-1]

        nogroup_mr_sum = redistr_mr(nogroup_mr_sum)
        group_mr_sum = redistr_mr(group_mr_sum)
        nogroup_sum = sum(nogroup_mr_sum)
        group_sum = sum(group_mr_sum)


        print key
        print "results:", results[key]
        print "MR:", nogroup_mr_sum, group_mr_sum
        print "Total:", nogroup_sum, group_sum
        print "Map impr %:", calculate_pct(nogroup_mr_sum[0], group_mr_sum[0])
        print "Red impr %:", calculate_pct(nogroup_mr_sum[1], group_mr_sum[1])
        print "Map share %:", nogroup_mr_sum[0] / float(nogroup_sum), group_mr_sum[0] / float(group_sum)
        print "Red share %:", nogroup_mr_sum[1] / float(nogroup_sum), group_mr_sum[1] / float(group_sum)
        print "Total %:", calculate_pct(nogroup_sum, group_sum)

        print "=" * 80
