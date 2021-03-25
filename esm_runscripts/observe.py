def run_job(config):
    helpers.evaluate(config, "observe", "observe_recipe")
    return config



def init_monitor_file(config):
    # which job am I spying on?
    observe_job = config["general"]["jobtype"]
    actual_job = observe_job.replace("observe_", "")
    called_from = actual_job
    #called_from = config["general"]["last_jobtype"]

    exp_log_path = (
        self.config["general"]["experiment_scripts_dir"] +
        self.config["general"]["expid"] +
        "_" +
        called_from + 
        "_" +
        self.config["general"]["run_datestamp"] +
        "_" +
        str(self.config["general"]["jobid"]) +
        ".log"
    )
    log_in_run = (
        self.config["general"]["thisrun_scripts_dir"] +
        self.config["general"]["expid"] +
        "_called_from_" +
        str(self.config["general"]["jobid"]) +
        ".log"
    )

    if os.path.isfile(exp_log_path):
        os.symlink(exp_log_path, log_in_run)

    monitor_file = config["general"]["logfile"]

    monitor_file.write("observing job initialized \n")
    monitor_file.write(
        "attaching to process " + str(config["general"]["launcher_pid"]) + " \n"
    )
    monitor_file.write("Called from a " + called_from + "job \n")
    return config


#def get_last_jobid(config):
#    # which job am I spying on?
#    thisjob = config["general"]["jobtype"]
#    called_from = thisjob.replace("observe_", "")
#    #called_from = config["general"]["last_jobtype"]
#    last_jobid = "UNKNOWN"
#    if called_from == "compute":
#        with open(config["general"]["experiment_log_file"], "r") as logfile:
#            lastline = [
#                l for l in logfile.readlines() if "compute" in l and "start" in l
#            ][-1]
#            last_jobid = lastline.split(" - ")[0].split()[-1]
#    config["general"]["last_jobid"] = last_jobid
#    return config



def wait_and_observe(config):
    if config["general"]["submitted"]:
        monitor_file = config["general"]["logfile"]
        thistime = 0
        error_check_list = assemble_error_list(config)
        while job_is_still_running(config):
            monitor_file.write("still running \n")
            config["general"]["next_test_time"] = thistime
            config = check_for_errors(config)
            thistime = thistime + 10
            time.sleep(10)
        thistime = thistime + 100000000
        config["general"]["next_test_time"] = thistime
        config = check_for_errors(config)
    return config


def wake_up_call(config):
    monitor_file = config["general"]["logfile"]
    monitor_file.write("job ended, starting to tidy up now \n")
    return config


def assemble_error_list(config):
    gconfig = config["general"]

    thisjob = config["general"]["jobtype"]
    called_from = thisjob.replace("observe_", "")
    if not called_from == "compute":
        config["general"]["error_list"] = []
        return config


    known_methods = ["warn", "kill"]

    stdout = (
        gconfig["thisrun_scripts_dir"]
        + "/"
        + gconfig["expid"]
        + "_compute_"
        + gconfig["run_datestamp"]
        + "_"
        + gconfig["jobid"]
        + ".log"
    )

    error_list = [
        ("error", stdout, "warn", 60, 60, "keyword error detected, watch out")
    ]

    for model in config:
        if "check_error" in config[model]:
            for trigger in config[model]["check_error"]:
                search_file = stdout
                method = "warn"
                frequency = 60
                message = "keyword " + trigger + " detected, watch out"
                if isinstance(config[model]["check_error"][trigger], dict):
                    if "file" in config[model]["check_error"][trigger]:
                        search_file = config[model]["check_error"][trigger]["file"]
                        if search_file == "stdout" or search_file == "stderr":
                            search_file = stdout
                    if "method" in config[model]["check_error"][trigger]:
                        method = config[model]["check_error"][trigger]["method"]
                        if method not in known_methods:
                            method = "warn"
                    if "message" in config[model]["check_error"][trigger]:
                        message = config[model]["check_error"][trigger]["message"]
                    if "frequency" in config[model]["check_error"][trigger]:
                        frequency = config[model]["check_error"][trigger]["frequency"]
                        try:
                            frequency = int(frequency)
                        except:
                            frequency = 60
                elif isinstance(config[model]["check_error"][trigger], str):
                    pass
                else:
                    continue
                error_list.append(
                    (trigger, search_file, method, frequency, frequency, message)
                )
    config["general"]["error_list"] = error_list
    return config


def check_for_errors(config):
    thisjob = config["general"]["jobtype"]
    called_from = thisjob.replace("observe_",  "")
    if not called_from == "compute":
        return config

    new_list = []
    error_check_list = config["general"]["error_list"]
    monitor_file = config["general"]["logfile"]
    time = config["general"]["next_test_time"]
    for (
        trigger,
        search_file,
        method,
        next_check,
        frequency,
        message,
    ) in error_check_list:
        warned = 0
        if next_check <= time:
            if os.path.isfile(search_file):
                with open(search_file) as origin_file:
                    for line in origin_file:
                        if trigger.upper() in line.upper():
                            if method == "warn":
                                warned = 1
                                monitor_file.write("WARNING: " + message + "\n")
                                break
                            elif method == "kill":
                                harakiri = "scancel " + config["general"]["jobid"]
                                monitor_file.write("ERROR: " + message + "\n")
                                monitor_file.write("Will kill the run now..." + "\n")
                                monitor_file.flush()
                                print("ERROR: " + message)
                                print("Will kill the run now...", flush=True)
                                database_actions.database_entry_crashed(config)
                                os.system(harakiri)
                                sys.exit(42)
            next_check += frequency
        if warned == 0:
            new_list.append(
                (trigger, search_file, method, next_check, frequency, message)
            )
    config["general"]["error_list"] = new_list
    return config


def job_is_still_running(config):
    if psutil.pid_exists(config["general"]["launcher_pid"]):
        return True
    return False

