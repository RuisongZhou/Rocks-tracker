import os
import sys

work_path = os.getcwd()
os.chdir("../")
print(os.getcwd())
sys.path.insert(0, '.')

from db_bench_option import DEFAULT_DB_BENCH, DEFAULT_L1_SIZE, DEFAULT_COMPACTION_TRIGGER
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

    result_dir = "results_ssd_hdd_hybrid/"
    fast_device_size_base_list = parameter_dict["fast_device_size"] 
    for fast_device_size_base in fast_device_size_base_list:    
        target_result_dir = result_dir+str(fast_device_size_base)
        # L0 256M L1 2560M L2 25.6G L3 256000M
        ssd_size = fast_device_size_base * DEFAULT_L1_SIZE * DEFAULT_COMPACTION_TRIGGER
        hdd_size = 10 * ssd_size
        ssd_path = parameter_dict["storage_paths"][1]["path"]
        hdd_path = parameter_dict["storage_paths"][0]["path"]
        print(target_result_dir)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": ssd_path+":"+str(ssd_size)+","+hdd_path+":"+str(hdd_size),
                "value_size":1000,
                "key_size":16,
                "report_interval_seconds": 1,
                "duration": 1800,       #1800s, 30min
                "benchmarks":"fillrandom,stats",
                "statistics":"true",
            })
        runner.run()

    reset_CPUs()
    clean_cgroup()
