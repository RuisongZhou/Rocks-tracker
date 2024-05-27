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
    result_dir = "results_nvme_ssd_hybrid/"
    
    nvme_back = "/".join(parameter_dict["hybrid_storage_paths"]["NVMESSD"].split("/")[-1])
    ssd_back_dir = "/".join([nvme_back, "ssd_backup"])
    nvme_back_dir = "/".join([nvme_back, "nvme_backup"])
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
            "threads": 1
        })
    runner.run()
    shutil.copytree(ssd_path, ssd_back_dir)
    shutil.copytree(nvme_path, nvme_back_dir)
    
    for wkld in ["ycsbwklda", "ycsbwkldb", "ycsbwkldc", "ycsbwkldd", "ycsbwklde", "ycsbwkldf"]:
        target_result_dir = result_dir + "exp1_" + "f1_" + wkld  + "_v128"
        slow_size = DEFAULT_DB_SIZE
        fast_size = int(slow_size * 0.1)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 128),
                "report_interval_seconds": 1,
                "benchmarks": wkld + ",stats",
                "statistics":"true",
                "zipf_const":0.86,
                "use_existing_db" : "true"
            })
        runner.run()  
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)


    slow_size = DEFAULT_DB_SIZE
    fast_size = int(slow_size * 0.1)
    for skewness in [0, 0.4, 0.6, 0.8, 0.99, 1.2]:
        target_result_dir = result_dir + "exp2_" + "f1_s" + str(skewness) + "_v128" + "mix"
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 128),
                "report_interval_seconds": 1,
                "zipf_const":skewness,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "use_existing_db" : "true"
                
            })
        runner.run()
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)

        target_result_dir = result_dir + "exp2_" + "f1_s" + str(skewness) + "_v128" + "write"
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "num": int(DEFAULT_DB_SIZE / 128),
                "key_size":8,
                "report_interval_seconds": 1,
                "zipf_const":skewness,
                "ycsb_readwritepercent":0,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "use_existing_db" : "true"
            })
        runner.run()
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)

        
        target_result_dir = result_dir + "exp2_" + "f1_s" + str(skewness) + "_v128" + "read"
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 128),
                "report_interval_seconds": 1,
                "zipf_const":skewness,
                "ycsb_readwritepercent":100,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "use_existing_db" : "true"
                
            })
        runner.run()
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)
    # clean_cgroup()
    
    shutil.rmtree(nvme_back_dir)
    shutil.rmtree(ssd_back_dir)

    for value_size in [32, 64, 256, 512, 1024]:
        target_result_dir = result_dir + "exp3_" + "f1_v" + str(value_size)
        slow_size = DEFAULT_DB_SIZE
        fast_size = int(slow_size * 0.1)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":value_size,
                "num": int(DEFAULT_DB_SIZE / value_size),
                "key_size":8,
                "report_interval_seconds": 1,
                "benchmarks":"ycsbfilldb,stats,resetstats,ycsbwklda,stats",
                "statistics":"true",
                "zipf_const":0.86,
            })
        runner.run()
        reset_CPUs()

    fast_device_size_base_list = parameter_dict["fast_device_size"] 
    slow_size = DEFAULT_DB_SIZE
    fast_size = int(slow_size * fast_device_size_base_list[0]/100)
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
        })
    runner.run()
    shutil.copytree(ssd_path, ssd_back_dir)
    shutil.copytree(nvme_path, nvme_back_dir)

 
    for fast_device_size_base in fast_device_size_base_list:    
        target_result_dir = result_dir+ "exp4_" + "f" + str(fast_device_size_base)  + "_v128"
        # L0 256M L1 2560M L2 25.6G L3 256000M
        slow_size = DEFAULT_DB_SIZE
        fast_size = int(slow_size * fast_device_size_base / 100)
        runner = DB_launcher(
            env, target_result_dir, db_bench=DEFAULT_DB_BENCH, extend_options={
                "db_path": nvme_path+":"+str(fast_size)+","+ssd_path+":"+str(slow_size),
                "value_size":128,
                "key_size":8,
                "num": int(DEFAULT_DB_SIZE / 128),
                "report_interval_seconds": 1,
                "benchmarks":"ycsbwklda,stats",
                "statistics":"true",
                "zipf_const":0.86,
                "use_existing_db" : "true",
                "max_background_flushes": 4,
                "threads": 1
            })
        runner.run()
        reset_CPUs()
        shutil.rmtree(ssd_path)
        shutil.copytree(ssd_back_dir, ssd_path)
        shutil.rmtree(nvme_path)
        shutil.copytree(nvme_back_dir, nvme_path)

    shutil.rmtree(nvme_back_dir)
    shutil.rmtree(ssd_back_dir)
    clean_cgroup()