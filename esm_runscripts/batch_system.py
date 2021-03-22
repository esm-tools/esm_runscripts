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
 
    # all wrappers to slurm, pbs and co as esm_runscript
    # should be written independent of actual batch system
    def __init__(self, config, name):
        self.name = name
        self.reserved_jobtypes = ["compute", "prepare", "tidy", "inspect"]

        if name == "slurm":
            self.bs = Slurm(config)
        else:
            raise UnknownBatchSystemError(name)

    def check_if_submitted(self):
        return self.bs.check_if_submitted()

    def get_jobid(self):
        return self.bs.get_jobid()

    def write_hostfile(self, config):
        return self.bs.write_hostfile(config)

    def get_job_state(self, jobid):
        return self.bs.get_job_state(jobid)

    def job_is_still_running(self, jobid):
        return self.bs.job_is_still_running(jobid)

    # methods that actually do something


    @staticmethod
    def get_sad_filename(config, cluster):
        folder = config["general"]["thisrun_scripts_dir"]
        expid = config["general"]["expid"]
        startdate = config["general"]["current_date"]
        enddate = config["general"]["end_date"]
        return (
            folder
            + "/"
            + expid
            + "_"
            + cluster
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
        config = batch_system.calculate_requirements(config)
        tasks = config["general"]["resubmit_tasks"]
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
        #??? Do we need the exclusive flag?
        if config["general"]["jobtype"] in ["compute", "tidy"]:
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
    def calculate_requirements(config, cluster = None):
        # get number of tasks for the whole job to be submitted,
        # as well as number of start process and end process for each
        # component (in case a hostfile needs to be written)

        tasks = 0
        start_proc = 0
        end_proc = 0

        # if not explicitly stated for which cluster we need the
        # requirements, calculate them for the job we are already in

        if not cluster:
            cluster = config["general"]["jobtype"]

        if cluster in self.reserved_jobtypes:
            for model in config["general"]["valid_model_names"]:
                if "nproc" in config[model]:
                    config[model]["tasks"] = config[model]["nproc"]
                    end_proc = start_proc + int(config[model]["nproc"]) - 1
                elif "nproca" in config[model] and "nprocb" in config[model]:
                    config[model]["tasks"] = config[model]["nproca"] * config[model]["nprocb"]
                    end_proc = start_proc + int(config[model]["nproca"])*int(config[model]["nprocb"]) - 1

                    # KH 30.04.20: nprocrad is replaced by more flexible
                    # partitioning using nprocar and nprocbr
                    if "nprocar" in config[model] and "nprocbr" in config[model]:
                        if (
                            config[model]["nprocar"] != "remove_from_namelist"
                            and config[model]["nprocbr"] != "remove_from_namelist"
                        ):
                            config[model]["tasks"] = config[model]["nprocar"] * config[model]["nprocbr"]
                            end_proc += config[model]["nprocar"] * config[model]["nprocbr"]
                else:
                    continue
                tasks += config[model]["tasks"]
                config[model]["start_proc"] = config[model]["end_proc"]
                start_proc = end_proc + 1


        else:
            # dataprocessing job with user definded name
            # number of tasks are actually already prepared in
            # workflow

            if not cluster or not cluster in config["general"]["workflow"]["subjob_clusters"]:
                print(f"Unknown or unset cluster: {cluster}.")
                sys.exit(-1)
            # user defined jobtype doing dataprocessing
            tasks = config["general"]["workflow"]["subjob_clusters"][cluster][nproc]

        config["general"]["resubmit_tasks"] = tasks
        return config



    @staticmethod
    def get_environment(config, subjob):
        environment = []
        if subjob in self.reserved_jobtypes:
            env = esm_environment.environment_infos("runtime", config)
            commands = env.commands
        else:
            commands = dataprocess.subjob_environment(config, subjob) 

        return commands



    @staticmethod
    def get_extra(config):
        extras = []
        if config["general"].get("unlimited_stack_size", True):
            extras.append("# Set stack size to unlimited")
            extras.append("ulimit -s unlimited")
        if config['general'].get('use_venv', False):
            extras.append("# Start everything in a venv")
            extras.append("source "+config["general"]["experiment_dir"]+"/.venv_esmtools/bin/activate")
        if config["general"].get("funny_comment", True):
            extras.append("# 3...2...1...Liftoff!")
        return extras




    @staticmethod
    def get_run_commands(config, subjob):  # here or in compute.py?

        commands = []
        if subjob in self.reserved_jobtypes:

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
        else:
            commands.append(dataprocess.subjob_tasks(config, subjob))

        return commands



    @staticmethod
    def get_submit_command(config, batch_or_shell, sadfilename):
        # in case of slurm e.g. returns:
        # cd SCRIPTDIR; sbatch sadfile
        # in case if shell:
        # cd SCRIPTDIR; ./sadfile
        
        commands = []
        if "batch_or_shell" == "batch":
            call = batch_system["submit"] + " "
        else:
            call = "./"

        batch_system = config["computer"]
        if "submit" in batch_system:
            commands.append(
                "cd "
                + config["general"]["thisrun_scripts_dir"]
                + "; "
                + call
                + sadfilename
            )
        return commands



    @staticmethod
    def write_simple_runscript(config, batch_or_shell = "batch", cluster = "None"):

        # if no cluster is specified, work on the one we are in
        if not cluster:
            cluster = config["general"]["jobtype"]

        clusterconf = None
        if "workflow" in config["general"]:
            if "clusters" in config["general"]["workflow"]:
                if cluster in config["general"]["workflow"]["clusters"]:
                   clusterconf = config["general"]["workflow"]["clusters"][cluster] 

        self = config["general"]["batch"]
        sadfilename = batch_system.get_sad_filename(config, cluster)

        if config["general"]["verbose"]:
            print("still alive")
            print("jobtype: ", config["general"]["jobtype"])
            print("writing sad file for:", cluster)


        with open(sadfilename, "w") as sadfile:

            # batch header (if any)
            if batch_or_shell == "batch":
                header = batch_system.get_batch_header(config)
                for line in header:
                    sadfile.write(line + "\n")
                sadfile.write("\n")

            if clusterconf:
                for subjob in clusterconf["subjobs"]:

                    # environment for each subjob of a cluster
                    environment = batch_system.get_environment(config, subjob)
                    for line in environment:
                        sadfile.write(line + "\n")

                    # extra entries for each subjob
                    extra = batch_system.get_extra(config)
                    for line in extra:
                        sadfile.write(line + "\n")

                    # Add actual commands
                    commands = batch_system.get_run_commands(config)
                    #commands = clusterconf.get("data_task_list", [])
                    sadfile.write("\n")
                    sadfile.write("cd " + config["general"]["thisrun_work_dir"] + "\n")
                    for line in commands:
                        sadfile.write(line + "\n")

            elif multisrun_stuff: # pauls stuff maybe here? or matching to clusterconf possible?
                dummy = 0
            else: # "normal" case
                dummy = 0



            if submits_another_job(cluster) and batch_or_shell == "batch":
                # -j ? is that used somewhere? I don't think so, replaced by workflow
                #   " -j "+ config["general"]["jobtype"]
                observe_call = (
                    "esm_runscripts "
                    + config["general"]["scriptname"]
                    + " -e "
                    + config["general"]["expid"]
                    + " -t observe -p ${process}"
                    + " -v "
                )

                if "--open-run" in config["general"]["original_command"] or not config["general"].get("use_venv"):
                    observe_call += " --open-run"
                elif "--contained-run" in config['general']['original_command'] or config["general"].get("use_venv"):
                    observe_call += " --contained-run"
                else:
                    print("ERROR -- Not sure if you were in a contained or open run!")
                    print("ERROR -- See write_simple_runscript for the code causing this.")
                    sys.exit(1)


                sadfile.write("process=$! \n")
                sadfile.write("cd " + config["general"]["experiment_scripts_dir"] + "\n")
                sadfile.write(observe_call + "\n")

        config["general"]["submit_command"] = batch_system.get_submit_command(
            config, batch_or_shell, sadfilename
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
