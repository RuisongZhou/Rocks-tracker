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

    # load data
    target_result_dir = result_dir + "exp_load"
    slow_size = DEFAULT_DB_SIZE
    fast_size = int(slow_size * 0.1)
    runner = DB_launcher(
        env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
            "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
            "value_size":128,
            "num": int(DEFAULT_DB_SIZE / 128),
            "key_size":8,
            "report_interval_seconds": 1,
            "ycsb_readwritepercent":0,
            "benchmarks":"ycsbfilldb,stats",
            "statistics":"true",
            "use_direct_io_for_flush_and_compaction": "false",
            "disable_wal": "true",
            "max_background_flushes": 4,
            "threads": 1,
        })
    runner.run()
    shutil.copytree(ssd_path, ssd_back_dir)
    shutil.copytree(nvme_path, nvme_back_dir)
    shutil.copytree(log_path, log_back_dir)

    
    for bg_thread in [1, 2, 4, 8, 16]:
        target_result_dir = result_dir + "exp_" + "bg" + str(bg_thread) + "_v128" + "write"
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 128),
                "report_interval_seconds": 1,
                "zipf_const":0,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "use_existing_db" : "true",
                "max_background_compactions":bg_thread,
                "ycsb_readwritepercent":0,
                "duration" : 3600
                # "use_direct_io_for_flush_and_compaction": "true",
                
            })
        runner.run()
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)
        shutil.rmtree(log_path)
        shutil.copytree(log_back_dir, log_path)