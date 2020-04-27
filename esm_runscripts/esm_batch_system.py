import sys
known_batch_systems = ["slurm"]

class esm_batch_system:
    def __init__(self, config, name):
        self.name = name
        if name == "slurm":
            from . import slurm
            self.bs = slurm.slurm(config)
        else:
            print ("Unknown batch system: ", name)
            sys.exit(1)

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
        return folder + "/" + expid+"_"+config["general"]["jobtype"]+"_"+config["general"]["run_datestamp"]+".sad"



    @staticmethod
    def get_batch_header(config):
        header = []
        batch_system = config["computer"]
        if "sh_interpreter" in batch_system:
            header.append("#!"+batch_system["sh_interpreter"])
        tasks = esm_batch_system.calculate_requirements(config)
        replacement_tags = [("@tasks@", tasks)]
        all_flags = ["partition_flag",
                     "time_flag",
                     "tasks_flag",
                     "output_flags",
                     "name_flag",
                    ]
        conditional_flags = ["accounting_flag",
                             "notification_flag",
                             "hyperthreading_flag",
                             "additional_flags"
                            ]
        if config["general"]["jobtype"] in ["compute", "tidy_and_resume"]:
            conditional_flags.append("exclusive_flag")
        for flag in conditional_flags:
            if flag in batch_system and not batch_system[flag].strip() == "":
                all_flags.append(flag)
        for flag in all_flags:
            for (tag, repl) in replacement_tags:
                batch_system[flag] = batch_system[flag].replace(tag, str(repl))
            header.append(batch_system["header_start"] + " " + batch_system[flag])
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
        elif config["general"]["jobtype"] == "post":
            tasks = 1
        return tasks




    @staticmethod
    def get_environment(config):
        environment = []
        import esm_environment
        env = esm_environment.environment_infos("runtime", config)
        return env.commands



    @staticmethod 
    def get_run_commands(config):  # here or in compute.py?
        commands = []
        batch_system = config["computer"]
        if "execution_command" in batch_system:
            commands.append("time " + batch_system["execution_command"] + " &")
        return commands


    @staticmethod
    def get_submit_command(config, sadfilename):
        commands = []
        batch_system = config["computer"]
        if "submit" in batch_system:
            commands.append("cd " + config["general"]["thisrun_scripts_dir"] + "; " + batch_system["submit"] + " " +sadfilename)
        return commands



    @staticmethod
    def write_simple_runscript(config):
        import six
        import os
        self = config["general"]["batch"]
        sadfilename = esm_batch_system.get_sad_filename(config)
        header = esm_batch_system.get_batch_header(config)
        environment = esm_batch_system.get_environment(config)

        if config["general"]["jobtype"] == "compute":
            commands = esm_batch_system.get_run_commands(config)
            tidy_call =  "esm_runscripts " + config["general"]["scriptname"] + " -e " + config["general"]["expid"] + " -t tidy_and_resubmit -p ${process} -j "+config["general"]["jobtype"]
        elif config["general"]["jobtype"] == "postprocess":
            tidy_call = "" 
            commands = config["general"]["post_task_list"]

        with open(sadfilename, "w") as sadfile:
            for line in header:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            for line in environment:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            sadfile.write("cd "+ config["general"]["thisrun_work_dir"] + "\n")
            for line in commands:
                sadfile.write(line + "\n")
            sadfile.write("process=$! \n")
            sadfile.write("cd "+ config["general"]["experiment_scripts_dir"] + "\n")
            sadfile.write(tidy_call + "\n")



        config["general"]["submit_command"] = esm_batch_system.get_submit_command(config, sadfilename)

        six.print_("\n", 40 * "+ ")
        six.print_("Contents of ",sadfilename, ":")
        with open(sadfilename, "r") as fin:
            print (fin.read())
        if os.path.isfile(self.bs.filename):
            six.print_("\n", 40 * "+ ")
            six.print_("Contents of ",self.bs.filename, ":")
            with open(self.bs.filename, "r") as fin:
                print (fin.read())
        return config


    @staticmethod    
    def submit(config):
        import six
        import os
        if not config["general"]["check"]:
            six.print_("\n", 40 * "+ ")
            print ("Submitting sad jobscript to batch system...")
            for command in config["general"]["submit_command"]:
                print (command)
            six.print_("\n", 40 * "+ ")
            for command in config["general"]["submit_command"]:
                os.system(command)
        return config
