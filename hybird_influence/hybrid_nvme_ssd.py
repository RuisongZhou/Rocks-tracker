import os
import sys

work_path = os.getcwd()
os.chdir("../")
print(os.getcwd())
sys.path.insert(0, '.')

from db_bench_option import DEFAULT_DB_BENCH, DEFAULT_L1_SIZE, DEFAULT_COMPACTION_TRIGGER, DEFAULT_ENTRY_COUNT
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
    for fast_device_size_base in fast_device_size_base_list:    
        target_result_dir = result_dir+ "f" + str(fast_device_size_base)  + "_v128"
        # L0 256M L1 2560M L2 25.6G L3 256000M
        slow_size = DEFAULT_ENTRY_COUNT * (128 + 8)
        fast_size = int(slow_size * fast_device_size_base_list / 100)
        ssd_path = parameter_dict["hybrid_storage_paths"]["SATASSD"]
        nvme_path = parameter_dict["hybrid_storage_paths"]["NVMESSD"]
        print(target_result_dir)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "report_interval_seconds": 1,
                "duration": 7200,       # 120min
                "benchmarks":"fillseq,stats,resetstats,fillrandom,stats",
                "statistics":"true",
            })
        runner.run()

 
    for value_size in [16, 32, 64, 256, 512, 1024]:

        target_result_dir = result_dir + "f1_v" + str(value_size)
        slow_size = DEFAULT_ENTRY_COUNT * (128 + 8)
        fast_size = int(slow_size * 0.1)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":value_size,
                "key_size":8,
                "report_interval_seconds": 1,
                "duration": 7200,       # 120min
                "benchmarks":"fillseq,stats,resetstats,fillrandom,stats",
                "statistics":"true",
            })
        runner.run()

    reset_CPUs()
    clean_cgroup()