import sys
import os
import six

from . import batch_system
from . import logfiles
from . import helpers

def submit(config):
    if config["general"]["verbose"]:
        six.print_("\n", 40 * "+ ")
    print("Submitting jobscript to batch system...")
    print()
    print(f"Output written by {config['computer']['batch_system']}:")
    if config["general"]["verbose"]:
        six.print_("\n", 40 * "+ ")
        for command in config["general"]["submit_command"]:
            print(command)
    for command in config["general"]["submit_command"]:
        os.system(command)
    return config



def resubmit_batch_or_shell(config, batch_or_shell, cluster = None):
    config = config["general"]["batch"].write_simple_runscript(config, cluster, batch_or_shell)
    if not check_if_check(config):
        config = submit(config)
    return config


def resubmit_SimulationSetup(config, cluster = None):
    monitor_file = logfiles.logfile_handle
    # Jobs that should be started directly from the compute job:

    jobtype = config["general"]["jobtype"]

    monitor_file.write(f"{cluster} for this run:\n")
    command_line_config = config["general"]["command_line_config"]
    command_line_config["jobtype"] = cluster

    monitor_file.write(f"Initializing {cluster} object with:\n")
    monitor_file.write(str(command_line_config))
    # NOTE(PG) Non top level import to avoid circular dependency:
    
    from .sim_objects import SimulationSetup
    cluster_obj = SimulationSetup(command_line_config)
    
    monitor_file.write("f{cluster} object built....\n")
    
    if f"{cluster}_update_{jobtype}_config_before_resubmit" in cluster_obj.config:
        monitor_file.write(f"{cluster} object needs to update the calling job config:\n")
        # FIXME(PG): This might need to be a deep update...?
        config.update(cluster_obj.config[f"{cluster}_update_{jobtype}_config_before_resubmit"])

    print(f"beep {cluster}")
    if not check_if_check(config):

        monitor_file.write(f"Calling {cluster} job:\n")
        cluster_obj(kill_after_submit = False)

    return config



def get_submission_type(cluster, config):
    # Figure out if next job is resubmitted to batch system,
    # just executed in shell or invoked as new SimulationSetup 
    # object

    clusterconf = config["general"]["workflow"]["subjob_clusters"][cluster]

    if clusterconf.get("submit_to_batch_system", False):
        submission_type = "batch"
    elif clusterconf.get("script", False):
        submission_type = "script"
    else:
        if not cluster in ["prepcompute", "tidy", "inspect", "viz"]:
            print (f"No idea how to submit cluster {cluster}.")
            sys.exit(-1)
        submission_type = "SimulationSetup"

    return submission_type





def end_of_experiment(config):
    if config["general"]["next_date"] >= config["general"]["final_date"]:
        monitor_file = logfiles.logfile_handle
        monitor_file.write("Reached the end of the simulation, quitting...\n")
        helpers.write_to_log(config, ["# Experiment over"], message_sep="")
        return True
    return False



def check_if_check(config):
    if config["general"]["check"]:
        print(
            "Actually not submitting anything, this job preparation was launched in 'check' mode (-c)."
        )
        print()
        return True
    else:
        return False



def maybe_resubmit(config):

    jobtype = config["general"]["jobtype"]
    for cluster in config["general"]["workflow"]["subjob_clusters"][jobtype]["next_submit"]:
        if not cluster == config["general"]["workflow"]["first_task_in_queue"]:

            submission_type = get_submission_type(cluster, config)
            if submission_type == "SimulationSetup":
                resubmit_SimulationSetup(config, cluster)
            elif submission_type in ["batch", "shell"]:
                resubmit_batch_or_shell(config, submission_type, cluster)
    
    for cluster in config["general"]["workflow"]["subjob_clusters"][jobtype]["next_submit"]:
        if cluster == config["general"]["workflow"]["first_task_in_queue"]:
            # count up the calendar here, skip job submission if end of
            # experiment is reached. all other clusters will still be 
            # submitted though
            config = _increment_date_and_run_number(config)
            config = _write_date_file(config)

            if end_of_experiment(config):
                continue

            submission_type = get_submission_type(cluster, config)
            if submission_type == "SimulationSetup":
                resubmit_SimulationSetup(config, cluster)
            elif submission_type in ["batch", "shell"]:
                resubmit_batch_or_shell(config, submission_type, cluster)

    return config


def _increment_date_and_run_number(config):
    config["general"]["run_number"] += 1
    config["general"]["current_date"] += config["general"]["delta_date"]

    config["general"]["command_line_config"]["current_date"] = (
            config["general"]["current_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
                )
            )

    config["general"]["command_line_config"]["run_number"] = config["general"]["run_number"] 

    return config


def _write_date_file(config):  # self, date_file=None):
    #monitor_file = config["general"]["logfile"]
    monitor_file = logfiles.logfile_handle

    # if not date_file:
    date_file = (
        config["general"]["experiment_scripts_dir"]
        + "/"
        + config["general"]["expid"]
        + "_"
        + config["general"]["setup_name"]
        + ".date"
    )
    with open(date_file, "w") as date_file:
        date_file.write(
            config["general"]["current_date"].output()
            + " "
            + str(config["general"]["run_number"])
        )
    monitor_file.write("writing date file \n")
    return config
