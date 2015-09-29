from mrsettings import create_settings
from mrsettings import MR_settings

__author__ = 'Jonny Daenen'

import math


class MR_cost_model_io:


    def __init__(self, mr_settings):
        self.mr_settings = mr_settings
        self.print_enabled = False


    def report(self, *msg):
        if self.print_enabled:
            print reduce(lambda x,y: str(x) + " " + str(y), msg)


    def get_mr_cost(self, guard_input, guarded_input, guard_interm, guarded_interm, extendedResult=False):

        mr_settings = self.mr_settings

        map_cost = guard_input + guarded_input
        red_cost = guard_interm + guarded_interm

        result = [map_cost, red_cost]

        if extendedResult:
            result += [0]
            result += [0]
            result += [0]

        return result


