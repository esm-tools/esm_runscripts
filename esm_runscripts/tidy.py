import filecmp
import os
import re
import time

import psutil

from . import coupler, database_actions, helpers
from .filelists import copy_files
from .sim_objects import SimulationSetup


def run_job(config):
    config["general"]["relevant_filetypes"] = ["log", "mon", "outdata", "restart_out","bin", "config", "forcing", "input", "restart_in", "ignore", "unknown"]
    helpers.evaluate(config, "tidy", "tidy_recipe")
    return config


def init_monitor_file(config):
    called_from = config["general"]["last_jobtype"]
    monitor_file = config["general"]["monitor_file"]

    monitor_file.write("tidy job initialized \n")
    monitor_file.write("attaching to process " + str(config["general"]["launcher_pid"]) + " \n")
    monitor_file.write("Called from a " + called_from + "job \n")
    return config


def get_last_jobid(config):
    called_from = config["general"]["last_jobtype"]
    last_jobid = "UNKNOWN"
    if called_from == "compute":
        with open(config["general"]["experiment_log_file"], "r") as logfile:
            lastline = [l for l in logfile.readlines() if "compute" in l and "start" in l][-1]
            last_jobid = lastline.split(" - ")[0].split()[-1]
    config["general"]["last_jobid"] = last_jobid
    return config


def copy_stuff_back_from_work(config):
    config = copy_files(
            config, \
            config["general"]["relevant_filetypes"], \
            "work", \
            "thisrun" \
            )
    return config


def wait_and_observe(config):
    if config["general"]["submitted"]:
        monitor_file = config["general"]["monitor_file"]
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


def tidy_coupler(config):
    if config["general"]["standalone"] == False:
        config["general"]["coupler"].tidy(config)
    return config


def wake_up_call(config):
    called_from = config["general"]["last_jobtype"]
    monitor_file = config["general"]["monitor_file"]
    last_jobid = config["general"]["last_jobid"]
    monitor_file.write("job ended, starting to tidy up now \n")
    # Log job completion
    if called_from != "command_line":
        helpers.write_to_log(config, [
            called_from,
            str(config["general"]["run_number"]),
            str(config["general"]["current_date"]),
            last_jobid,
            "- done"])
    # Tell the world you're cleaning up:
    helpers.write_to_log(config, [
        str(config["general"]["jobtype"]),
        str(config["general"]["run_number"]),
        str(config["general"]["current_date"]),
        str(config["general"]["jobid"]),
        "- start"])
    return config


def assemble_error_list(config):
    gconfig = config["general"]
    known_methods = ["warn", "kill"]
    stdout = gconfig["thisrun_scripts_dir"] + "/" +  gconfig["expid"] + "_compute_" + gconfig["run_datestamp"] + "_" + gconfig["jobid"] + ".log"

    error_list = [("error", stdout, "warn", 60, 60, "keyword error detected, watch out")]

    for model in config:
        if "check_error" in config[model]:
            for trigger in config[model]["check_error"]:
                search_file = stdout
                method = "warn"
                frequency = 60
                message = "keyword " + trigger + " detected, watch out"
                if isinstance(config[model]["check_error"][trigger], dict):
                    if "file" in  config[model]["check_error"][trigger]:
                        search_file = config[model]["check_error"][trigger]["file"]
                        if search_file == "stdout" or search_file == "stderr":
                            search_file = stdout
                    if "method" in  config[model]["check_error"][trigger]:
                        method = config[model]["check_error"][trigger]["method"]
                        if method not in known_methods:
                            method = "warn"
                    if "message" in  config[model]["check_error"][trigger]:
                        message = config[model]["check_error"][trigger]["message"]
                    if "frequency" in  config[model]["check_error"][trigger]:
                        frequency = config[model]["check_error"][trigger]["frequency"]
                        try:
                            frequency = int(frequency)
                        except:
                            frequency = 60
                elif isinstance(config[model]["check_error"][trigger], str) :
                    pass
                else:
                    continue
                error_list.append((trigger, search_file, method, frequency, frequency, message))
    config["general"]["error_list"] = error_list
    return config


def check_for_errors(config):
    new_list = []
    error_check_list = config["general"]["error_list"]
    monitor_file = config["general"]["monitor_file"]
    time = config["general"]["next_test_time"]
    for (trigger, search_file, method, next_check, frequency, message) in error_check_list:
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
            new_list.append((trigger, search_file, method, next_check, frequency, message))
    config["general"]["error_list"] = new_list
    return config


def job_is_still_running(config):
    if psutil.pid_exists(config["general"]["launcher_pid"]):
        return True
    return False


def _increment_date_and_run_number(config):
    config["general"]["run_number"] += 1
    config["general"]["current_date"] += config["general"]["delta_date"]
    return config


def _write_date_file(config): #self, date_file=None):
    monitor_file = config["general"]["monitor_file"]
        #if not date_file:
    date_file = (
        config["general"]["experiment_scripts_dir"]
        + "/"
        + config["general"]["expid"]
        + "_"
        + config["general"]["setup_name"]
        + ".date"
    )
    with open(date_file, "w") as date_file:
        date_file.write(config["general"]["current_date"].output() + " " + str(config["general"]["run_number"]))
    monitor_file.write("writing date file \n")
    return config


def start_post_job(config):
    monitor_file = config["general"]["monitor_file"]
    do_post = False
    for model in config:
        if "post_processing" in config[model]:
            if config[model]["post_processing"]:
                do_post = True

    if do_post:
        monitor_file.write("Post processing for this run:\n")
        command_line_config["jobtype"] = "post"
        command_line_config["original_command"] = command_line_config[
            "original_command"
        ].replace("compute", "post")
        monitor_file.write("Initializing post object with:\n")
        monitor_file.write(str(command_line_config))
        this_post = SimulationSetup(command_line_config)
        monitor_file.write("Post object built; calling post job:\n")
        this_post()
    return config


def all_done(config):
    helpers.write_to_log(config, [
        str(config["general"]["jobtype"]),
        str(config["general"]["run_number"]),
        str(config["general"]["current_date"]),
        str(config["general"]["jobid"]),
        "- done"])

    database_actions.database_entry_success(config)

    return config


def maybe_resubmit(config):
    monitor_file = config["general"]["monitor_file"]
    monitor_file.write("resubmitting \n")
    command_line_config = config["general"]["command_line_config"]
    command_line_config["jobtype"] = "compute"
    command_line_config["original_command"] = command_line_config["original_command"].replace("tidy_and_resubmit", "compute")

    # seb-wahl: end_date is by definition (search for 'end_date') smaller than final_date
    # hence we have to use next_date = current_date + increment
    if config["general"]["next_date"] >= config["general"]["final_date"]:
        monitor_file.write("Reached the end of the simulation, quitting...\n")
        helpers.write_to_log(config, ["# Experiment over"], message_sep="")
    else:
        monitor_file.write("Init for next run:\n")
        next_compute = SimulationSetup(command_line_config)
        next_compute(kill_after_submit=False)
    return config
    




# DONT LIKE THE FOLLOWING PART...
# I wish it was closer to the copy_files routine in filelists,
# but as it is really a different thing - moving everything
# found compared to copying everything in filelists - a second
# implementation might be OK... (DB)

def copy_all_results_to_exp(config):
    monitor_file = config["general"]["monitor_file"]
    monitor_file.write("Copying stuff to main experiment folder \n")
    for root, dirs, files in os.walk(config["general"]["thisrun_dir"], topdown=False):
        if config["general"]["verbose"]:
            print ("Working on folder: " + root)
        if root.startswith(config["general"]["thisrun_work_dir"]) or root.endswith("/work"):
            if config["general"]["verbose"]:
                print ("Skipping files in work.")
            continue
        for name in files:
            source = os.path.join(root, name)
            if config["general"]["verbose"]:
                print ("File: " + source)
            destination = source.replace(config["general"]["thisrun_dir"], config["general"]["experiment_dir"])
            destination_path = destination.rsplit("/", 1)[0]
            if not os.path.exists(destination_path):
                os.makedirs(destination_path)
            if not os.path.islink(source):
                if os.path.isfile(destination):
                    if filecmp.cmp(source, destination):
                        if config["general"]["verbose"]:
                            print ("File " + source + " has not changed, skipping.")
                        continue
                    else:
                        if os.path.isfile(destination + "_" + config["general"]["run_datestamp"]):
                            print ("Don't know where to move " + destination +", file exists")
                            continue
                        else:
                            if os.path.islink(destination):
                                os.remove(destination)
                            else:
                                os.rename(destination, destination + "_" + config["general"]["last_run_datestamp"])
                            newdestination = destination + "_" + config["general"]["run_datestamp"]
                            if config["general"]["verbose"]:
                                print ("Moving file " + source + " to " + newdestination)
                            os.rename(source, newdestination)
                            os.symlink(newdestination, destination)
                            continue
                try:
                    if config["general"]["verbose"]:
                        print ("Moving file " + source + " to " + destination)
                    try:
                        os.rename(source, destination)
                    except: # Fill is still open... create a hard (!) link instead
                        os.link(source, destination)

                except:
                    print(">>>>>>>>>  Something went wrong moving " + source + " to " + destination)
            else:
                linkdest = os.path.realpath(source)
                newlinkdest = destination.rsplit("/", 1)[0] + "/" + linkdest.rsplit("/", 1)[-1]
                if os.path.islink(destination):
                    os.remove(destination)
                if os.path.isfile(destination):
                    os.rename(destination, destination + "_" + config["general"]["last_run_datestamp"])
                os.symlink(newlinkdest, destination)
    return config
