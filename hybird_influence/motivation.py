import os
import sys
import shutil   
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
    ssd_path = parameter_dict["hybrid_storage_paths"]["SATASSD"]
    nvme_path = parameter_dict["hybrid_storage_paths"]["NVMESSD"]
    log_path = parameter_dict["storage_paths"][0]["path"]
    result_dir = "results_nvme_ssd_hybrid/"
    
    nvme_back = "/".join(parameter_dict["hybrid_storage_paths"]["NVMESSD"].split("/")[:-1] + ["backup"])
    
    ssd_back_dir = "/".join([nvme_back, "ssd_backup"])
    nvme_back_dir = "/".join([nvme_back, "nvme_backup"])
    log_back_dir  = "/".join([nvme_back, "log_wal"])
    print("ssd_back_dir: ", ssd_back_dir)

    if not os.path.exists(nvme_back):
        os.mkdir(nvme_back)
    shutil.rmtree(ssd_back_dir,ignore_errors=True)
    shutil.rmtree(nvme_back,ignore_errors=True)
    shutil.rmtree(log_back_dir,ignore_errors=True)


    fast_device_size_base_list = []
    j = 1
    level_size =  64 * 1048576 
    buf_size = 16 * 1048576 * 2
    fast_device_size_base_list.append(level_size + buf_size) # level 0
    for i in range(0, 4):
        fast_device_size_base_list.append(level_size + buf_size) # level 1-4
        level_size *= 8

    for fast_device_size_base in fast_device_size_base_list[::-1]:    
        target_result_dir = result_dir+ "exp_moti" + "pt" + str(fast_device_size_base/1048576)  + "_v192"
        # L0 256M L1 2560M L2 25.6G L3 256000M
        slow_size = DEFAULT_DB_SIZE
        fast_size = fast_device_size_base
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":192,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 200),
                "report_interval_seconds": 1,
                "benchmarks":"fillseq,stats,overwrite,stats",
                "statistics":"true",
                "zipf_const":0,
                "max_background_flushes": 1,
                "level0_slowdown_writes_trigger": 20,
                "level0_stop_writes_trigger": 36,
                "threads": 1,
                "target_file_size_base" : 16 * 1048576,
                "max_bytes_for_level_base" : 64 * 1048576,
                "max_bytes_for_level_multiplier" : 8,
                "ycsb_readwritepercent":0,
            })
        runner.run()
        reset_CPUs()

    # shutil.rmtree(nvme_back_dir)
    # shutil.rmtree(ssd_back_dir)
    clean_cgroup()