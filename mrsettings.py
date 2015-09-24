__author__ = 'Jonny Daenen'


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