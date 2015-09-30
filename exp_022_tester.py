from cost_model2 import MR_cost_model_gumbo
from mrsettings import create_settings

__author__ = 'Jonny Daenen'



if __name__ == '__main__':
    inputs = {
        "q1-nogroup-red128": [
            (1777777804,444444445,1879155417,544444445), # Gin, gin, Gout, gout
            (1777777804,444444445,1879155409,544444445),
               ],

        "q1-group-red128": [(1777777804, 444444445, 3758310826, 644444445),
               ],


        "q2-nogroup-red128": [
            (1777777804,444444445, 1979155409, 544444445), # Gin, gin, Gout, gout
            (1777777804,444444445, 1979155409, 544444445),
               ],

        "q2-group-red128": [(1777777804*2, 444444445, 3858310818, 544444445),
               ],


        "q3-nogroup-red128": [
            (1777777804,444444445, 1879155409, 544444445), # Gin, gin, Gout, gout
            (1777777804,444444445, 1879155409, 544444445),
               ],

        "q3-group-red128": [(1777777804, 444444445*2, 2079155409, 1088888890),
               ],


        "q4-nogroup-red128": [
            (1777777804,444444445,1879155409, 544444445), # Gin, gin, Gout, gout
            (1777777804,444444445,1979155409, 544444445),
               ],

        "q4-group-red128": [(1777777804*2, 444444445*2, 3858310818, 1088888890),
               ],


        "q5-nogroup-red128": [
            (1777777804,444444445,1879155409,544444445), # Gin, gin, Gout, gout
            (1777777804,444444445,1979155409,544444445),
            (1777777804,444444445,1979155409,544444445), # Gin, gin, Gout, gout
            (1777777804,444444445,1879155409,544444445),
               ],

        "q5-group-red128": [(1777777804*2, 444444445*2, 4258310818, 1088888890),
               ],

    }



    results = {}
    for key in inputs.keys():
        print key, inputs[key]
        results[key] = []


        cost_model = MR_cost_model_gumbo(create_settings("EXP_022", key))
        costs = []
        for (Gin, gin, Gout, gout) in inputs[key]:
            cost = cost_model.get_mr_cost(Gin / (1024**2), gin / (1024**2), Gout / (1024**2), gout / (1024**2), True)
            costs.append(sum(cost[0:2]))
            print "-", cost, sum(cost[:2])
        print sum(costs)
        print "~" * 80

    print