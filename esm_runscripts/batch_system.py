import os
import sys

import esm_environment
import six

from . import helpers
from .slurm import Slurm

known_batch_systems = ["slurm"]


class UnknownBatchSystemError(Exception):
    """Raise this exception when an unknown batch system is encountered"""


class batch_system:
    def __init__(self, config, name):
        self.name = name
        if name == "slurm":
            self.bs = Slurm(config)
        else:
            raise UnknownBatchSystemError(name)

    def check_if_submitted(self):
        return self.bs.check_if_submitted()

    def get_jobid(self):
        return self.bs.get_jobid()

    def calc_requirements(self, config):
        return self.bs.calc_requirements(config)

    def get_job_state(self, jobid):
        return self.bs.get_job_state(jobid)

    def job_is_still_running(self, jobid):
        return self.bs.job_is_still_running(jobid)

    @staticmethod
    def get_sad_filename(config):
        folder = config["general"]["thisrun_scripts_dir"]
        expid = config["general"]["expid"]
        startdate = config["general"]["current_date"]
        enddate = config["general"]["end_date"]
        return (
            folder
            + "/"
            + expid
            + "_"
            + config["general"]["jobtype"]
            + "_"
            + config["general"]["run_datestamp"]
            + ".sad"
        )

    @staticmethod
    def get_batch_header(config):
        header = []
        this_batch_system = config["computer"]
        if "sh_interpreter" in this_batch_system:
            header.append("#!" + this_batch_system["sh_interpreter"])
        tasks = batch_system.calculate_requirements(config)
        replacement_tags = [("@tasks@", tasks)]
        all_flags = [
            "partition_flag",
            "time_flag",
            "tasks_flag",
            "output_flags",
            "name_flag",
        ]
        conditional_flags = [
            "accounting_flag",
            "notification_flag",
            "hyperthreading_flag",
            "additional_flags",
        ]
        if config["general"]["jobtype"] in ["compute", "tidy_and_resume"]:
            conditional_flags.append("exclusive_flag")
        for flag in conditional_flags:
            if flag in this_batch_system and not this_batch_system[flag].strip() == "":
                all_flags.append(flag)
        for flag in all_flags:
            for (tag, repl) in replacement_tags:
                this_batch_system[flag] = this_batch_system[flag].replace(
                    tag, str(repl)
                )
            header.append(
                this_batch_system["header_start"] + " " + this_batch_system[flag]
            )
        return header

    @staticmethod
    def calculate_requirements(config):
        tasks = 0
        if config["general"]["jobtype"] == "compute":
            for model in config["general"]["valid_model_names"]:
                if "nproc" in config[model]:
                    tasks += config[model]["nproc"]
                elif "nproca" in config[model] and "nprocb" in config[model]:
                    tasks += config[model]["nproca"] * config[model]["nprocb"]

                    # KH 30.04.20: nprocrad is replaced by more flexible
                    # partitioning using nprocar and nprocbr
                    if "nprocar" in config[model] and "nprocbr" in config[model]:
                        if (
                            config[model]["nprocar"] != "remove_from_namelist"
                            and config[model]["nprocbr"] != "remove_from_namelist"
                        ):
                            tasks += config[model]["nprocar"] * config[model]["nprocbr"]

        elif config["general"]["jobtype"] == "post":
            tasks = 1
        return tasks

    @staticmethod
    def get_environment(config):
        environment = []
        env = esm_environment.environment_infos("runtime", config)
        return env.commands

    @staticmethod
    def get_extra(config):
        extras = []
        if config["general"].get("unlimited_stack_size", True):
            extras.append("# Set stack size to unlimited")
            extras.append("ulimit -s")
        if config['general'].get('use_venv', True):
            extras.append("# Start everything in a venv")
            extras.append("source "+config["general"]["experiment_dir"]+"/.venv/bin/activate")
        if config["general"].get("funny_comment", True):
            extras.append("# 3...2...1...Liftoff!")
        return extras

    @staticmethod
    def get_run_commands(config):  # here or in compute.py?
        commands = []
        batch_system = config["computer"]
        if "execution_command" in batch_system:
            line = helpers.assemble_log_message(
                config,
                [
                    config["general"]["jobtype"],
                    config["general"]["run_number"],
                    config["general"]["current_date"],
                    config["general"]["jobid"],
                    "- start",
                ],
                timestampStr_from_Unix=True,
            )
            commands.append(
                "echo " + line + " >> " + config["general"]["experiment_log_file"]
            )
            commands.append("time " + batch_system["execution_command"] + " &")
        return commands

    @staticmethod
    def get_submit_command(config, sadfilename):
        commands = []
        batch_system = config["computer"]
        if "submit" in batch_system:
            commands.append(
                "cd "
                + config["general"]["thisrun_scripts_dir"]
                + "; "
                + batch_system["submit"]
                + " "
                + sadfilename
            )
        return commands

    @staticmethod
    def write_simple_runscript(config):
        self = config["general"]["batch"]
        sadfilename = batch_system.get_sad_filename(config)
        header = batch_system.get_batch_header(config)
        environment = batch_system.get_environment(config)
        extra = batch_system.get_extra(config)

        if config["general"]["verbose"]:
            print("still alive")
            print("jobtype: ", config["general"]["jobtype"])

        if config["general"]["jobtype"] == "compute":
            commands = batch_system.get_run_commands(config)
            tidy_call = (
                "esm_runscripts "
                + config["general"]["scriptname"]
                + " -e "
                + config["general"]["expid"]
                + " -t tidy_and_resubmit -p ${process} -j "
                + config["general"]["jobtype"]
                + " -v "
            )
        elif config["general"]["jobtype"] == "post":
            tidy_call = ""
            commands = config["general"]["post_task_list"]

        with open(sadfilename, "w") as sadfile:
            for line in header:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            for line in environment:
                sadfile.write(line + "\n")
            for line in extra:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            sadfile.write("cd " + config["general"]["thisrun_work_dir"] + "\n")
            for line in commands:
                sadfile.write(line + "\n")
            sadfile.write("process=$! \n")
            sadfile.write("cd " + config["general"]["experiment_scripts_dir"] + "\n")
            sadfile.write(tidy_call + "\n")

        config["general"]["submit_command"] = batch_system.get_submit_command(
            config, sadfilename
        )

        if config["general"]["verbose"]:
            six.print_("\n", 40 * "+ ")
            six.print_("Contents of ", sadfilename, ":")
            with open(sadfilename, "r") as fin:
                print(fin.read())
            if os.path.isfile(self.bs.filename):
                six.print_("\n", 40 * "+ ")
                six.print_("Contents of ", self.bs.filename, ":")
                with open(self.bs.filename, "r") as fin:
                    print(fin.read())
        return config

    @staticmethod
    def submit(config):
        if not config["general"]["check"]:
            if config["general"]["verbose"]:
                six.print_("\n", 40 * "+ ")
            print("Submitting jobscript to batch system...")
            print()
            print(f"Output written by {config['computer']['batch_system']}:")
            if config["general"]["verbose"]:
                for command in config["general"]["submit_command"]:
                    print(command)
                six.print_("\n", 40 * "+ ")
            for command in config["general"]["submit_command"]:
                os.system(command)
        else:
            print(
                "Actually not submitting anything, this job preparation was launched in 'check' mode (-c)."
            )
            print()
        return config
