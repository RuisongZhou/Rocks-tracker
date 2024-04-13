import os
import sys

work_path = os.getcwd()
os.chdir("../")
print(os.getcwd())
sys.path.insert(0, '.')

from db_bench_option import DEFAULT_DB_BENCH, DEFAULT_L1_SIZE, DEFAULT_DB_SIZE, DEFAULT_ENTRY_COUNT
from db_bench_option import load_config_file
from db_bench_option import set_parameters_to_env
from db_bench_runner import DB_launcher
from db_bench_runner import reset_CPUs
from parameter_generator import HardwareEnvironment
from parameter_generator import StorageMaterial
from db_bench_runner import clean_cgroup

os.chdir(work_path)
if __name__ == '__main__':
    env = HardwareEnvironment()
    parameter_dict = load_config_file('config.json')
    set_parameters_to_env(parameter_dict, env)

    result_dir = "results_nvme_ssd_hybrid/"
    fast_device_size_base_list = parameter_dict["fast_device_size"] 
    ssd_path = parameter_dict["hybrid_storage_paths"]["SATASSD"]
    nvme_path = parameter_dict["hybrid_storage_paths"]["NVMESSD"]

    target_result_dir = result_dir + "exp5_ratio_0.1_fill"
    slow_size = DEFAULT_DB_SIZE
    fast_size = int(slow_size * 0.1)
    runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size": 128,
                "num": int(DEFAULT_DB_SIZE / 128),
                "key_size":8,
                "report_interval_seconds": 1,
                "ycsb_readwritepercent":0,
                "max_background_compactions": 16,  #额外一个flush线程
                "zipf_const": 0,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "wal_bytes_per_sync" : 0,
                "max_background_flushes" : 2
            })
    runner.run()

    for thread_num in [16, 8, 4, 2, 1]:
        target_result_dir = result_dir + "exp5_ratio0.1" + "thread_" + str(thread_num) 
     
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size": 128,
                "num": int(DEFAULT_DB_SIZE / 128),
                "key_size":8,
                "report_interval_seconds": 1,
                "ycsb_readwritepercent":0,
                "max_background_compactions": thread_num + 1,  #额外一个flush线程
                "zipf_const": 0,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "use_existing_db" : True,
                "duration" : 3600
            })
        runner.run()

    reset_CPUs()
    clean_cgroup()