from mrsettings import create_settings
from mrsettings import MR_settings

__author__ = 'Jonny Daenen'

import math


class MR_cost_model_gumbo:


    def __init__(self, mr_settings):
        self.mr_settings = mr_settings
        self.print_enabled = True


    def report(self, *msg):
        if self.print_enabled:
            print reduce(lambda x,y: str(x) + " " + str(y), msg)

    def get_map_cost(self, guard_input, guard_interm):

        mr_settings = self.mr_settings

        # read cost
        read_cost = mr_settings.cost_local_r * guard_input

        # sort cost
        mappers = math.ceil(float(guard_input) / mr_settings.map_chunk_size_mb)
        one_map_output_size = guard_interm / mappers
        one_map_sort_chunks = one_map_output_size / mr_settings.map_sort_buffer_mb
        sort_cost = one_map_sort_chunks * mr_settings.map_chunk_size_mb * mr_settings.cost_sort  # TODO is sort cost per chunk or per byte?

        # merge cost
        map_merge_levels = math.ceil(math.log(one_map_sort_chunks, mr_settings.map_merge_factor))
        merge_cost = map_merge_levels * guard_interm * (mr_settings.cost_local_r + mr_settings.cost_local_w)

        # store cost
        store_cost = guard_interm * mr_settings.cost_local_w

        return read_cost, sort_cost, merge_cost, store_cost

    def get_red_cost(self, guard_input, guarded_input, guard_interm, guarded_interm):

        mr_settings = self.mr_settings

        total_interm = guard_interm + guarded_interm

        # transfer cost
        transfer_cost = total_interm * mr_settings.cost_transfer

        # transfer startup penalty
        red_tasks = math.ceil(float(total_interm) / mr_settings.red_chunk_size_mb)
        map_tasks = math.ceil(float(guard_input+guarded_input) / mr_settings.map_chunk_size_mb)
        penalty_cost = red_tasks * map_tasks * mr_settings.cost_transfer

        # merge cost
        red_inmem_correction = 1
        red_pieces = max(1,mr_settings.red_chunk_size_mb / float(mr_settings.red_sort_buffer_mb))
        red_merge_levels = math.log(red_pieces, mr_settings.red_merge_factor) + red_inmem_correction
        merge_cost = red_merge_levels * (total_interm) * (mr_settings.cost_local_r + mr_settings.cost_local_w)

        # reduce cost
        reduce_cost = total_interm * mr_settings.cost_red
        # reduce_cost += guard_interm * mr_settings.cost_hdfs_w

        return transfer_cost, penalty_cost, merge_cost, reduce_cost

    def get_mr_cost(self, guard_input, guarded_input, guard_interm, guarded_interm, extendedResult=False):

        mr_settings = self.mr_settings


        map_guard_cost = self.get_map_cost(guard_input, guard_interm)
        map_guarded_cost = self.get_map_cost(guarded_input, guarded_interm)
        approx_cost = self.get_map_cost(guard_input+guarded_input, guard_interm+guarded_interm)

        sum_cost = map(sum, zip(map_guard_cost,map_guarded_cost))

        self.report("Map guard cost:", map_guard_cost, sum(map_guard_cost))
        self.report("Map guarded cost:", map_guarded_cost, sum(map_guarded_cost))
        self.report("Approx cost:", approx_cost, sum(approx_cost))
        self.report("Sum cost:", sum_cost, sum(sum_cost))

        red_cost = self.get_red_cost(guard_input, guarded_input, guard_interm, guarded_interm)
        self.report("Red cost:", red_cost, sum(red_cost))

        result = [sum(map_guard_cost) + sum(map_guarded_cost), sum(red_cost)]

        if extendedResult:
            result += [red_cost[0] + red_cost[1]]
            result += [red_cost[2]]
            result += [red_cost[3]]

        return result




if __name__ == '__main__':

    inputs = {
        "q1-nogroup": [
            (5333333508, 708888906, 1885623801, 868888898), # Gin, gin, Gout, gout
            (5333333508, 708888906, 1885623801, 868888906),
            (5333333508, 708888906, 1885623801, 868888906)
               ],

        "q1-group": [(5333333508, 708888906*3, 5572290519, 2606666718),
               ],

    }


    cost_model = MR_cost_model_gumbo(create_settings("EXP_021", ""))

    results = {}
    for key in inputs.keys():
        print key, inputs[key]
        results[key] = []

        costs = []
        for (Gin, gin, Gout, gout) in inputs[key]:
            cost = cost_model.get_mr_cost(Gin / (1024**2), gin / (1024**2), Gout / (1024**2), gout / (1024**2), True)
            costs.append(sum(cost[0:2]))
            print "-", cost
        print sum(costs)
        print "~" * 80

    print
