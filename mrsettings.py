__author__ = 'Jonny Daenen'

def create_settings(exp, opts, cost_vector = None):


    settings = MR_settings()

    if "021" in exp:
        settings.mapper_memory_mb = 2048
        settings.reducer_memory_mb = 4096
        settings.red_chunk_size_mb = 1024
        if "test" in opts:
            settings.mapper_memory_mb = 1024
            settings.reducer_memory_mb = 1024

    if "022" in exp:
        settings.mapper_memory_mb = 1024
        settings.reducer_memory_mb = 1024


    if "128" in opts:
        settings.red_chunk_size_mb = 128
    if "64" in opts:
            settings.red_chunk_size_mb = 128


    if not(cost_vector is None):
        settings.cost_local_r = cost_vector[0]
        settings.cost_local_w = cost_vector[1]
        settings.cost_hdfs_w = cost_vector[2]
        settings.cost_hdfs_r = cost_vector[3]
        settings.cost_transfer = cost_vector[4]
        settings.cost_sort = cost_vector[5]
        settings.cost_red = cost_vector[6]


    return settings



class MR_settings:
    # cost_local_r = 1/160.0 #1
    # cost_local_w = 1/160.0 #1
    # cost_hdfs_w = 1/63.0 #330
    # cost_hdfs_r = 1/25.0 #59
    # cost_transfer = 1/10.0 #5
    # cost_sort = 1/2 #52
    # cost_red = 0.12

    cost_local_r = 1
    cost_local_w = 1
    cost_hdfs_w = 330
    cost_hdfs_r = 59
    cost_transfer = 5
    cost_sort = 52
    cost_red = 0.12

    # map settings
    map_chunk_size_mb = 128  # hdfs setting (block size) (dfs.blocksize)
    mapper_memory_mb = 1024  # hadoop system setting ()
    map_merge_factor = 10  # global sort setting (mapreduce.task.io.sort.factor)
    map_sort_buffer_mb = 100   # (mapreduce.task.io.sort.mb)

    # red settings
    red_chunk_size_mb = 128  # gumbo/pig setting
    red_memory_mb = 1024  # hadoop system setting
    red_merge_factor = 10  # global sort setting (mapreduce.task.io.sort.factor)
    red_sort_buffer_mb = red_memory_mb * 0.7  # (mapreduce.reduce.shuffle.input.buffer.percent)