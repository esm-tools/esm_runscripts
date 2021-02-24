import os
import textwrap
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
    def get_batch_header_multisrun(config, headers_so_far):
        all_headers = [headers_so_far[0]]
        for idx, run_type in enumerate(config["general"]["multi_srun"]):
            idx += 1
            import pprint
            pprint.pprint(config['general']['multi_srun'][run_type])
            for model_idx, model in enumerate(config["general"]["multi_srun"][run_type]['models']):
                if config["general"]["verbose"]:
                    print(f"Assigning SBATCH headers for ({idx}): {run_type}")
                    print("Going through headers:")
                    for header in headers_so_far[1:]:
                        print(header)
                for header in headers_so_far[1:]:
                    if "--ntasks=" in header:
                        if config["general"]["verbose"]:
                            print("In --ntasks block")
                            print('all_headers.append("#SBATCH --ntasks="+str(config["general"]["multi_srun"][run_type]["model_tasks"]))')
                        all_headers.append("#SBATCH --ntasks="+str(config["general"]["multi_srun"][run_type][model+"_tasks"]))
                    elif "--output=" in header:
                        if config["general"]["verbose"]:
                            print("In --output block")
                            print('all_headers.append(header.replace("%j", "%j_"+run_type))')
                        all_headers.append(header.replace("%j", "%j_"+run_type+"_"+model))
                    else:
                        if config["general"]["verbose"]:
                            print("In else block")
                            print('all_headers.append(header)')
                        all_headers.append(header)
                all_headers.append("#SBATCH --propagate=STACK,CORE")
                all_headers.append("#SBATCH --comment="+run_type+"_"+model)
                print(f"model_index={model_idx}")
                print(config["general"]["multi_srun"][run_type]["models"])
                if model_idx != (len(config["general"]["multi_srun"][run_type]["models"]) - 1):
                    all_headers.append("#SBATCH packjob")
            if idx != len(config["general"]["multi_srun"]):
                all_headers.append("#SBATCH packjob")
        for header in all_headers:
            if config["general"]["verbose"]:
                print(header)
        return all_headers

    @staticmethod
    def get_batch_header(config):
        header = []
        this_batch_system = config["computer"]
        if "sh_interpreter" in this_batch_system:
            header.append("#!" + this_batch_system["sh_interpreter"])
        tasks = batch_system.calculate_requirements(config)
        qos = this_batch_system.get("qos", "")
        replacement_tags = [("@tasks@", tasks), ("@qos@", qos)]
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
            "qos_flag",
            "additional_flags",
        ]
        if config["general"]["jobtype"] in ["compute", "tidy_and_resume"]:
            conditional_flags.append("exclusive_flag")
        for flag in conditional_flags:
            if flag in this_batch_system and not this_batch_system[flag].strip() == "":
                if "qos" in flag:
                    print(flag)
                    print(qos)
                    if qos:
                        all_flags.append(flag)
                else:
                    all_flags.append(flag)
        for flag in all_flags:
            for (tag, repl) in replacement_tags:
                this_batch_system[flag] = this_batch_system[flag].replace(
                    tag, str(repl)
                )
            header.append(
                this_batch_system["header_start"] + " " + this_batch_system[flag]
            )
        if "multi_srun" in config["general"]:
            return batch_system.get_batch_header_multisrun(config, header)
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

            if "multi_srun" in config["general"]:
                for run_type in list(config["general"]["multi_srun"]):
                    header_tasks = 0
                    for model in config["general"]["multi_srun"][run_type]["models"]:
                        if "nproc" in config[model]:
                            header_tasks += config[model]["nproc"]
                        elif "nproca" in config[model] and "nprocb" in config[model]:
                            header_tasks += config[model]["nproca"] * config[model]["nprocb"]

                            # KH 30.04.20: nprocrad is replaced by more flexible
                            # partitioning using nprocar and nprocbr
                            if "nprocar" in config[model] and "nprocbr" in config[model]:
                                if (
                                    config[model]["nprocar"] != "remove_from_namelist"
                                    and config[model]["nprocbr"] != "remove_from_namelist"
                                ):
                                    header_tasks += config[model]["nprocar"] * config[model]["nprocbr"]
                    config["general"]["multi_srun"][run_type]["header_tasks"] = header_tasks
        elif config["general"]["jobtype"] == "post":
            tasks = 1
        return tasks

    @staticmethod
    def get_environment(config):
        environment = []
        env = esm_environment.environment_infos("runtime", config)
        return env.commands

    @staticmethod
    def determine_nodelist(config):
        setup_name = config['general']['setup_name']
        if config['general'].get('multi_srun'):
            for run_type in config['general']['multi_srun']:
                total_tasks = 0
                for model in config['general']['multi_srun'][run_type]['models']:
                    model_tasks = 0
                    # determine how many nodes that component needs
                    if "nproc" in config[model]:
                        model_tasks += int(config[model]["nproc"])
                    elif "nproca" in config[model] and "nprocb" in config[model]:
                        model_tasks += int(config[model]["nproca"])*int(config[model]["nprocb"])

                        # KH 30.04.20: nprocrad is replaced by more flexible
                        # partitioning using nprocar and nprocbr
                        if "nprocar" in config[model] and "nprocbr" in config[model]:
                            if config[model]["nprocar"] != "remove_from_namelist" and config[model]["nprocbr"] != "remove_from_namelist":
                                model_tasks += config[model]["nprocar"] * config[model]["nprocbr"]
                    else:
                        continue
                    config['general']['multi_srun'][run_type][model+"_tasks"] = model_tasks
                    total_tasks += model_tasks
                config['general']['multi_srun'][run_type]['total_tasks'] = total_tasks
        print(config['general']['multi_srun'])


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
            if config['general']['multi_srun']:
                return get_run_commands_multisrun(config, commands)
            commands.append("time " + batch_system["execution_command"] + " &")
        return commands



    @staticmethod
    def get_submit_command(config, sadfilename):
        # FIXME(PG): Here we need to include a multi-srun thing
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
        # NOTE(PG): This next line allows for multi-srun simulations:
        batch_system.determine_nodelist(config)
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
            if "--open-run" in config["general"]["original_command"] or not config["general"].get("use_venv"):
                tidy_call += " --open-run"
            elif "--contained-run" in config['general']['original_command'] or config["general"].get("use_venv"):
                tidy_call += " --contained-run"
            else:
                print("ERROR -- Not sure if you were in a contained or open run!")
                print("ERROR -- See write_simple_runscript for the code causing this.")
                sys.exit(1)

            if "modify_config_file_abspath" in config["general"]:
                if config["general"]["modify_config_file_abspath"]:
                    tidy_call += " -m " + config["general"]["modify_config_file_abspath"]

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
            # NOTE(PG): It'd be nice to have some sort of super-method that
            # defines a verbose_class_name.method_name to debug only certain
            # routines. But I can keep dreaming...
            if config["general"]["verbose"] or config["general"].get("verbose_submit_command", False):
                for command in config["general"]["submit_command"]:
                    print(command)
                six.print_("\n", 40 * "+ ")
            if config["general"].get("paul_just_prints", False):
                sys.exit()
            for command in config["general"]["submit_command"]:
                os.system(command)
        else:
            print(
                "Actually not submitting anything, this job preparation was launched in 'check' mode (-c)."
            )
            print()
        return config


def get_run_commands_multisrun(config, commands):
    default_exec_command = config['computer']["execution_command"]
    # Since I am already confused, I need to write comments.
    #
    # The next part is actually a shell script fragment, which will be injected
    # into the "sad" file. sad = Sys Admin Dump. It's sad :-(
    #
    # In this part, we figure out what compute nodes we are using so we can
    # specify nodes for each srun command. That means, ECHAM+FESOM will use one
    # pre-defined set of nodes, PISM another, and so on. That should be general
    # enough to also work for other model combos...
    #
    # Not sure if this is specific to Mistral as a HPC, Slurm as a batch
    # system, or whatever else might pop up...
    # @Dirk, please move this where you see it best (I guess slurm.py)
    job_node_extraction = r"""
    # Job nodes extraction
    nodeslurm=$SLURM_JOB_NODELIST
    echo "nodeslurm = ${nodeslurm}"
    # Get rid of the hostname and surrounding brackets:
    tmp=${nodeslurm#*[}
    echo "tmp=$tmp"
    nodes=${tmp%]*}
    echo "nodes=$nodes"
    # Turn it into an array seperated by newlines:
    myarray=(`echo ${nodes} | sed 's/,/\n/g'`)
    #
    idx=0
    for element in "${myarray[@]}"; do
        if [[ "$element" == *"-"* ]]; then
            array=(`echo $element | sed 's/-/\n/g'`)
            for node in $(seq ${array[0]} ${array[1]}); do
               nodelist[$idx]=${node}
               idx=${idx}+1
            done
        else
            nodelist[$idx]=${element}
            idx=${idx}+1
        fi
    done

    for element in "${nodelist[@]}"; do
        echo "${element}"
    done
    """

    def assign_nodes(run_type, start_node, end_node):
        template = f"""
        # Assign nodes for {run_type}
        {run_type}=""
        for idx in $srbseq %%START_NODE%% %%END_NODE%%erb; do
            if ssbssb $idx == %%END_NODE%% esbesb; then
                {run_type}="$scb{run_type}ecb$scbnodelist[$idx]ecb"
            else
                {run_type}="$scb{run_type}ecb$scbnodelistssb$idxesbecb,"
            fi
        done
        echo "{run_type} nodes: $scb{run_type}ecb"
        """
        # Since Python f-strings and other braces don't play nicely together,
        # we replace some stuff:
        #
        # For the confused:
        # scb = start curly brace {
        # ecb = end curly brace }
        # ssb = start square brace [
        # esb = end square brace ]
        # srb = start round brace (
        # erb = end round brace )
        template = template.replace("scb", "{")
        template = template.replace("ecb", "}")
        template = template.replace("ssb", "[")
        template = template.replace("esb", "]")
        template = template.replace("srb", "(")
        template = template.replace("erb", ")")
        # Get rid of the starting spaces (they come from Python as the string
        # is defined inside of this function which is indented (facepalm))
        template = textwrap.dedent(template)
        # TODO: Some replacements
        template = template.replace("%%START_NODE%%", str(start_node))
        template = template.replace("%%END_NODE%%", str(end_node))
        return template


    commands.append(textwrap.dedent(job_node_extraction))
    total_nodes_so_far = 0
    for idx, run_type in enumerate(config['general']['multi_srun']):
        for model in config['general']['multi_srun'][run_type]['models']:
            num_nodes_for_this_srun = int(config['general']['multi_srun'][run_type][model+'_tasks'] / config['computer']['cores_per_node'])
            nodes = assign_nodes(model+"_nodes", total_nodes_so_far, total_nodes_so_far + num_nodes_for_this_srun - 1)
            total_nodes_so_far += num_nodes_for_this_srun
            commands.append(nodes)
    pack_group_counter = 0
    for idx, run_type in enumerate(config['general']['multi_srun']):
        new_exec_command = default_exec_command.replace("hostfile_srun", "") # config['general']['multi_srun'][run_type]['hostfile'])
        new_exec_command = new_exec_command.replace("--multi-prog", "")
        new_exec_command = new_exec_command.replace(config["computer"]["launcher_flags"], "")
        for idx_mod, model in enumerate(config['general']['multi_srun'][run_type]['models']):
                model_tasks = config["general"]["multi_srun"][run_type][model+"_tasks"]
                end_character = ": " if idx_mod < (len(config['general']['multi_srun'][run_type]['models']) - 1) else ">>LOG_PLACEHOLDER<< &"
                add_pack_group = f"--pack-group={pack_group_counter}" # if idx_mod < (len(config['general']['multi_srun'][run_type]['models']) - 1) else ""
                pack_group_counter += 1
                new_exec_command += config["computer"]["launcher_flags"] + " --mpi=pmi2 --cpu_bind=cores --distribution=block:block"
                log_for_this_srun = config["general"]["experiment_scripts_dir"]+"/"+config["general"]["expid"]+"_"+run_type+"_compute_"+config["general"]["run_datestamp"]+".log"
                if "execution_command" in config[model]:
                    model_command = "./" + config[model]["execution_command"]
                elif "executable" in config[model]:
                    model_command = "./" + config[model]["executable"]
                if config['general']['multi_srun'].get('include_nodelist', False):
                    new_exec_command += f" {add_pack_group} --nodelist ${model}_nodes -n {model_tasks} {model_command} {end_character}"
                else:
                    new_exec_command += f" {add_pack_group} -n {model_tasks} {model_command} {end_character}"
        commands.append("time " + new_exec_command.replace(">>LOG_PLACEHOLDER<<", f">{log_for_this_srun} 2>&1"))
    commands.append("wait")
    return commands
