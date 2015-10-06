from mrsettings import MR_settings

__author__ = 'Jonny Daenen'

import math


class MR_cost_model_basic:


    def __init__(self, mr_settings):
        self.mr_settings = mr_settings
        self.print_enabled = False


    def report(self, *msg):
        if self.print_enabled:
            print reduce(lambda x,y: str(x) + str(y), msg)

    def get_mr_cost(self, data_input_mb, data_interm_mb, reduce_in_memory=True, extendedResult=False):
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



        map_in_read_cost = data_input_mb * mr_settings.cost_local_r
        map_out_initial_write_cost = data_interm_mb * mr_settings.cost_local_w
        map_merge_cost = map_merge_levels * (data_interm_mb) * (mr_settings.cost_local_r + mr_settings.cost_local_w)


        self.report("map read cost: ", map_in_read_cost)
        self.report("map write cost: ", map_out_initial_write_cost)
        self.report("map merge cost: ", map_merge_cost)



        if reduce_in_memory:
            red_corr = 0
        else:
            red_corr = 1

        # reduce calculation
        red_tasks = math.ceil(data_interm_mb / float(mr_settings.red_chunk_size_mb))
        red_pieces = mr_settings.red_chunk_size_mb / float(mr_settings.red_sort_buffer_mb)
        red_merge_levels = math.ceil(math.log(red_pieces, mr_settings.red_merge_factor)) + red_corr

        self.report("red tasks: ", red_tasks)
        self.report("red task pieces: ", red_pieces)
        self.report("red tasks merge levels: ", red_merge_levels)


        red_in_read_cost = data_interm_mb * mr_settings.cost_transfer
        # initial write cost is captured by last write cost that is not needed
        red_merge_cost = red_merge_levels * (data_interm_mb) * (mr_settings.cost_local_r + mr_settings.cost_local_w)

        red_transfer_cost = red_in_read_cost
        red_reduce_cost = 0


        self.report("red read cost: ", red_in_read_cost)
        self.report("red reduce cost: ", red_reduce_cost)
        self.report("red merge cost: ", red_merge_cost)

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

