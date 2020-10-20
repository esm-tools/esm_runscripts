"""
Documentation goes here
"""
from datetime import datetime
from io import StringIO
import collections
from loguru import logger
import pathlib
import os
import pdb
import shutil
import sys

import f90nml
import six
import tqdm
import yaml
import time


from esm_calendar import Date, Calendar
import esm_parser
from . import esm_coupler
from . import esm_methods
#import .esm_coupler
from esm_profile import *

import pprint

pp = pprint.PrettyPrinter(indent=4)


def date_representer(dumper, date):
    return dumper.represent_str("%s" % date.output())


yaml.add_representer(Date, date_representer)


# Utility functions:
def rm_r(path):
    """
    Python equivalent of rm -r

    Parameters
    ----------
    path : str
        Path or directory to remove
    """
    if not os.path.exists(path):
        return
    if os.path.isfile(path) or os.path.islink(path):
        os.unlink(path)
    else:
        shutil.rmtree(path)

def size_bytes_to_human(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

# PG: BROKEN!!!
def size_human_to_bytes(s, suffix="B"):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        num = float(s.replace(unit, ''))
        if abs(num) < 1024.0:
            return num
        num *= 1024.0
    return num

class RunFolders(list):
    """
    Logs the ``run_`` directories in ``<experiment_id>/log/run_folders.log``,
    updating it with new folders. The resulting object is a list of ``run_`` paths
    that exist or existed during the run time (even if they got deleted). This
    is useful for indexing operations such as ``<object_name>[::<interval>]``
    used when removing ``run_`` folders.

    Notes
    -----
    It keeps the folder names sorted so there is no need of sorting out of the
    object, and it also prevents the existence of duplicates.
    """
    def __init__(self, config):
        """
        The initialization of the object:

        * Loads the existing paths of the ``run_`` folders

        * Loads previous ``run_`` folder names from the logging file

        * Adds the current folder names to the logging file

        * Returns a list of ``pathlib.Path`` folder paths
        """

        # Load paths from ``config``
        self.exp_dir = config["general"]["experiment_dir"]
        self.log_path = self.exp_dir + "/log/run_folders.log"

        # Load existing folders
        self.current_folders = [folder for folder in os.listdir(self.exp_dir) if folder.startswith("run_")]
        self.current_folders = [self.exp_dir + "/" + folder for folder in self.current_folders]

        # Check if the ``run_folders.log`` file exists, and if not, create it
        if not os.path.exists(self.log_path):
            with open(self.log_path, "w") as log_file:
                pass

        # Load previous run names from ``run_folders.log``
        self.folders = []
        self.load()

        # Add current folders
        self.update()

        # Add folders to the list
        for folder in self.folders:
            super().append(pathlib.Path(folder))

    def load(self):
        """
        Loads the existing paths of the ``run_`` folders.
        """
        with open (self.log_path, "r") as log_file:
            for folder in log_file.readlines():
                self.folders.append(folder.strip())

    def save(self):
        """
        Saves all folder names.
        """
        with open (self.log_path, "w") as log_file:
            log_file.writelines([folder  + '\n' for folder in self.folders])

    def update(self):
        """
        Updates the folders read from the log file with the currently existing
        folders, removes duplicates, sorts them and save them into the log file.
        """
        # Update with ``self.curren_folders``
        for folder in self.current_folders:
            self.folders.append(folder)
        # Remove duplicates
        self.folders = list(dict.fromkeys(self.folders))
        # Sort folders
        self.folders.sort()
        # Save to the log file
        self.save()


class SimulationSetup(object):
    def __init__(self, command_line_config = None, user_config = None):

        if not command_line_config and not user_config:
            raise ValueError("SimulationSetup needs to be initialized with either command_line_config or user_config.")
        if command_line_config:
            self.command_line_config = command_line_config
        if not user_config:
            user_config = self.get_user_config_from_command_line(command_line_config)
        if user_config["general"].get("debug_obj_init", False):
            import pdb; pdb.set_trace()
        self.get_total_config_from_user_config(user_config)

    def __call__(self, *args, **kwargs):
        if self.config["general"]["jobtype"] == "compute":
            self.compute(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "tidy_and_resubmit":
            self.tidy(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "post":
            self.postprocess(*args, **kwargs)
        else:
            print("Unknown jobtype specified! Goodbye...")
            self.end_it_all()





###################################     COMPUTE      #############################################################
    def compute(self, kill_after_submit=True):  # supposed to be reduced to a stump
        """
        All steps needed for a model computation.

        Parameters
        ----------
        kill_after_submit : bool
            Default ``True``. If set, the entire Python instance is killed with
            a ``sys.exit()`` as the very last after job submission.
        """
        from . import compute
        Compute = compute(self.config)
        self.config = Compute.evaluate(self.config)

        if kill_after_submit:
            self.end_it_all()

    # NOTE(PG): No longer needed...? Defined also in jobclass...?
    def end_it_all(self):
        import sys
        if self.config["general"]["profile"]:
            for line in timing_info:
                print(line)
        print("Exiting entire Python process!")
        sys.exit()

###############################################       POSTPROCESS ######################################






    def postprocess(self):
        from . import esm_batch_system
        """
        Calls post processing routines for this run.
        """
        with open( # TODO: this will be deleted by the cleanup function
                   # maybe change its location to the general experiment folder?
            self.config["general"]["thisrun_scripts_dir"] +
            "/" +
            self.config["general"]["expid"] +
            "_post_" +
            self.run_datestamp +
            "_" +
            str(self.config['general']['jobid']) +
            ".log",
            "w",
            buffering=1,
        ) as post_file:
            post_task_list = self._assemble_postprocess_tasks(post_file)
            self.config["general"]["post_task_list"] = post_task_list
            esm_batch_system.write_simple_runscript(self.config)
            self.config = esm_batch_system.submit(self.config)

    def _assemble_postprocess_tasks(self, post_file):
        """
        Generates all tasks for post processing which will be written to the sad file.

        Parameters
        ----------
        post_file
            File handle to which information should be written.

        Returns
        -------
        post_task_list : list
            The list of post commands which will be executed. These are written
            to the sad file.
        """
        post_task_list = []
        for component in self.config["general"]["valid_model_names"]:
            post_file.write(40*"+ "+"\n")
            post_file.write("Generating post-processing tasks for: %s \n" % component)

            post_task_list.append("\n#Postprocessing %s\n" % component)
            post_task_list.append("cd "+ self.config[component]["experiment_outdata_dir"]+"\n")

            pconfig_tasks = self.config[component].get('postprocess_tasks', {})
            post_file.write("Configuration for post processing: %s \n" % pconfig_tasks)
            for outfile in pconfig_tasks:
                post_file.write("Generating task to create: %s \n" % outfile)
                ofile_config = pconfig_tasks[outfile]
                # TODO(PG): This can be cleaned up. I probably actually want a
                # ChainMap here for more than just the bottom...
                #
                # Run CDO tasks (default)
                task_definition = self.config[component].get("postprocess_task_definitions", {}).get(ofile_config['post_process'])
                method_definition = self.config[component].get("postprocess_method_definitions", {}).get(task_definition['method'])

                program = method_definition.get("program", task_definition["method"])

                possible_args = method_definition.get("possible_args", [])
                required_args = method_definition.get("required_args", [])

                possible_flags = method_definition.get("possible_flags", [])
                required_flags = method_definition.get("required_flags", [])

                outfile_flags = ofile_config.get("flags")
                outfile_args = ofile_config.get("args")

                task_def_flags = task_definition.get("flags")
                task_def_args = task_definition.get("args")

                args = collections.ChainMap(outfile_args, task_def_args)
                flags = outfile_flags + task_def_flags
                flags = ["-"+flag for flag in flags]

                # See here: https://stackoverflow.com/questions/21773866/how-to-sort-a-dictionary-based-on-a-list-in-python
                all_call_things = {"program": program, "outfile": outfile, **args, "flags": flags}
                print(all_call_things)
                index_map = {v: i for i, v in enumerate(method_definition["call_order"])}
                call_list = sorted(all_call_things.items(), key=lambda pair: index_map[pair[0]])
                call = []
                for call_id, call_part in call_list:
                    if isinstance(call_part, str):
                        call.append(call_part)
                    elif isinstance(call_part, list):
                        call.append(" ".join(call_part))
                    else:
                        raise TypeError("Something straaaange happened. Consider starting the debugger.")
                post_file.write(" ".join(call)+"\n")
                post_task_list.append(" ".join(call))
            post_task_list.append("cd -\n")
        return post_task_list













    ##########################    ASSEMBLE ALL THE INFORMATION  ##############################


    def get_user_config_from_command_line(self, command_line_config):
        try:
            user_config = esm_parser.initialize_from_yaml(command_line_config["scriptname"])
            if not "additional_files" in user_config["general"]:
                user_config["general"]["additional_files"] = []
        except esm_parser.EsmConfigFileError as error:
            raise error
        except:
            user_config = esm_parser.initialize_from_shell_script(command_line_config["scriptname"])

        user_config["general"].update(command_line_config)
        return user_config



    def get_total_config_from_user_config(self, user_config):

        if "version" in user_config["general"]:
            version = str(user_config["general"]["version"])
        else:
            setup_name = user_config["general"]["setup_name"]
            if "version" in user_config[setup_name.replace("_standalone","")]:
                version = str(user_config[setup_name.replace("_standalone","")]["version"])
            else:
                version = "DEFAULT"

        self.config = esm_parser.ConfigSetup(user_config["general"]["setup_name"].replace("_standalone",""),
                                             version,
                                             user_config)

        self.config["computer"]["jobtype"] = self.config["general"]["jobtype"]
        self.config["general"]["experiment_dir"] = self.config["general"]["base_dir"] + "/" + self.config["general"]["expid"]

        self._read_date_file(self.config)
        esm_parser.choose_blocks(self.config, blackdict=self.config._blackdict)

        self._initialize_calendar(self.config)
        esm_parser.choose_blocks(self.config, blackdict=self.config._blackdict)

        self._add_all_folders()
        self.set_prev_date()

        self.config.finalize()
        self.add_submission_info()
        self.initialize_batch_system()

        #esm_parser.pprint_config(self.config)
        #sys.exit(0)

        if self.config["general"]["standalone"] == False:
            self.init_coupler()

        # Write where the experiment log file should be in the config
        self.config["general"]["experiment_log_file"] = self.config["general"].get("experiment_log_file",
                self.config["general"]["experiment_log_dir"] + "/"
                + self.config["general"]["expid"] + "_"
                + self.config["general"]["setup_name"]
                + ".log"
                )


    def _add_all_folders(self):
        self.all_filetypes = ["analysis", "config", "log", "mon", "scripts", "ignore",  "unknown"]
        self.config["general"]["out_filetypes"] = ["analysis", "log", "mon", "scripts", "ignore",  "unknown", "outdata", "restart_out"]
        self.config["general"]["in_filetypes"] = ["scripts", "input", "forcing", "bin", "config", "restart_in"]
        self.all_filetypes.append("work")
        self.config["general"]["thisrun_dir"] = self.config["general"]["experiment_dir"] + "/run_" + self.run_datestamp

        for filetype in self.all_filetypes:
            self.config["general"][
                "experiment_" + filetype + "_dir"
            ] = self.config["general"]["experiment_dir"] + "/" + filetype + "/"

        for filetype in self.all_filetypes:
            self.config["general"][
                "thisrun_" + filetype + "_dir"
            ] = self.config["general"]["thisrun_dir"] + "/" + filetype + "/"

        self.config["general"]["work_dir"] =  self.config["general"]["thisrun_work_dir"]

        self.all_model_filetypes = [
            "analysis",
            "bin",
            "config",
            "couple",
            "forcing",
            "input",
            "log",
            "mon",
            "outdata",
            "restart_in",
            "restart_out",
            "viz",
            "ignore"
        ]

        self.config["general"]["all_model_filetypes"] = self.all_model_filetypes
        self.config["general"]["all_filetypes"] = self.all_filetypes

        for model in self.config["general"]["valid_model_names"]:
             for filetype in self.all_model_filetypes:
                if "restart" in filetype:
                    filedir = "restart"
                else:
                    filedir = filetype
                self.config[model][
                    "experiment_" + filetype + "_dir"
                ] = self.config["general"]["experiment_dir"] + "/" + filedir + "/" + model + "/"
                self.config[model][ "thisrun_" + filetype + "_dir"
                ] = self.config["general"]["thisrun_dir"] + "/" + filedir  + "/" + model + "/"
                self.config[model]["all_filetypes"] = self.all_model_filetypes

    @timing
    def _read_date_file(self, config, date_file=None):
        if not date_file:
            date_file = (
                config["general"]["experiment_dir"]
                + "/scripts/"
                + config["general"]["expid"]
                + "_"
                + config["general"]["setup_name"]
                + ".date"
            )
        if os.path.isfile(date_file):
            logger.info("Date file read from %s", date_file)
            with open(date_file) as date_file:
                date, self.run_number = date_file.readline().strip().split()
                self.run_number = int(self.run_number)
            write_file = False
        else:
            logger.info("No date file found %s", date_file)
            logger.info("Initializing run_number=1 and date=18500101")
            date = config["general"].get("initial_date", "18500101")
            self.run_number = 1
            write_file = True
        config["general"]["run_number"] = self.run_number

        self.current_date = date

        if config["general"]["run_number"] != 1:
            for model in config["general"]["valid_model_names"]:
                config[model]["lresume"] = True
        else:
            # Did the user give a value? If yes, keep it, if not, first run:
            for model in config["general"]["valid_model_names"]:
                if "lresume" in config[model]:
                    user_lresume = config[model]["lresume"]
                else:
                    user_lresume = False

                if isinstance(user_lresume, str) and "${" in user_lresume:
                    user_lresume = esm_parser.find_variable(model, user_lresume, self.config, [], [])
                if type(user_lresume) == str:

                    if user_lresume == "0" or user_lresume.upper() == "FALSE":
                        user_lresume = False
                    elif user_lresume == "1" or user_lresume.upper() == "TRUE":
                        user_lresume = True
                elif isinstance(user_lresume, int):
                    if user_lresume == 0:
                        user_lresume = False
                    elif user_lresume == 1:
                        user_lresume = True
                config[model]["lresume"] = user_lresume

        # needs to happen AFTER a run!
        # if write_file:
        #    self._write_date_file()

        logger.info("current_date = %s", self.current_date)
        logger.info("run_number = %s", self.run_number)




    #########################       PREPARE EXPERIMENT / WORK    #############################



    def _create_toplevel_marker_file(self):
        if not os.path.isfile(self.config['thisrun_']):
            with open(".top_of_exp_tree") as f:
                f.write("Top of experiment: "+self.config['general']['expid'])


    def _dump_final_yaml(self):
        with open(
            self.experiment_config_dir
            + "/"
            + self.config["general"]["expid"]
            + "_preconfig.yaml",
            "w",
        ) as config_file:
            yaml.dump(self.config, config_file)

    def _initialize_calendar(self, config):
        nyear, nmonth, nday, nhour, nminute, nsecond = 0, 0, 0, 0, 0, 0
        nyear = int(config["general"].get("nyear", nyear))
        if not nyear:
            nmonth = int(config["general"].get("nmonth", nmonth))
        if not nyear and not nmonth:
            nday = int(config["general"].get("nday", nday))
        if not nyear and not nmonth and not nday:
            nhour = int(config["general"].get("nhour", nhour))
        if not nyear and not nmonth and not nday and not nhour:
            nminute = int(config["general"].get("nminute", nminute))
        if not nyear and not nmonth and not nday and not nhour and not nminute:
            nsecond = int(config["general"].get("nsecond", nsecond))
        if (
            not nyear
            and not nmonth
            and not nday
            and not nhour
            and not nminute
            and not nsecond
        ):
            nyear = 1

        # make sure all models agree on leapyear
        if "leapyear" in self.config["general"]:
            for model in self.config["general"]["valid_model_names"]:
                self.config[model]["leapyear"] = self.config["general"]["leapyear"]
        else:
            for model in self.config["general"]["valid_model_names"]:
                if "leapyear" in self.config[model]:
                    for other_model in self.config["general"]["valid_model_names"]:
                        if "leapyear" in self.config[other_model]:
                            if not self.config[other_model]["leapyear"] == self.config[model]["leapyear"]:
                                print ("Models " + model + " and " + other_model + " do not agree on leapyear. Stopping.")
                                sys.exit(43)
                        else:
                            self.config[other_model]["leapyear"] = self.config[model]["leapyear"]
                    self.config["general"]["leapyear"] = self.config[model]["leapyear"]
                    break

        if not "leapyear" in self.config["general"]:
            for model in self.config["general"]["valid_model_names"]:
                self.config[model]["leapyear"] = True
            self.config["general"]["leapyear"] = True


        # set the overall calendar
        if self.config["general"]["leapyear"]:
            self.calendar = Calendar(1)
            self.config["general"]["calendar"] = Calendar(1)
        else:
            self.calendar = Calendar(0)
            self.config["general"]["calendar"] = Calendar(0)

        self.current_date = Date(self.current_date, self.calendar)
        self.delta_date = (nyear, nmonth, nday, nhour, nminute, nsecond)
        config["general"]["current_date"] = self.current_date
        config["general"]["start_date"] = self.current_date
        config["general"]["initial_date"] = Date(config["general"]["initial_date"], self.calendar)
        config["general"]["final_date"] = Date(config["general"]["final_date"], self.calendar)
        #config["general"]["prev_date"] = self.current_date.sub((0, 0, 1, 0, 0, 0))
        config["general"]["prev_date"] = self.current_date - (0, 0, 1, 0, 0, 0)

        config["general"]["next_date"] = self.current_date.add(self.delta_date)
        config["general"]["last_start_date"] = self.current_date - self.delta_date
        #config["general"]["end_date"] = config["general"]["next_date"].sub(
        config["general"]["end_date"] = config["general"]["next_date"] - (0, 0, 1, 0, 0, 0)

        config["general"]["runtime"] = (
            config["general"]["next_date"] - config["general"]["current_date"]
        )

        config["general"]["total_runtime"] = (
            config["general"]["next_date"] - config["general"]["initial_date"]
        )

        self.run_datestamp = (
            config["general"]["current_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
            + "-"
            + config["general"]["end_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
        )

        config["general"]["run_datestamp"] = self.run_datestamp

        self.last_run_datestamp = (
            config["general"]["last_start_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
            + "-"
            + config["general"]["prev_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
        )
        config["general"]["last_run_datestamp"] = self.last_run_datestamp


    def set_prev_date(self):
        """Sets several variables relevant for the previous date. Loops over all models in ``valid_model_names``, and sets model variables for:
        * ``prev_date``
        * ``parent_expid``
        * ``parent_date``
        * ``parent_restart_dir``
        """
        for model in self.config["general"]["valid_model_names"]:
            if "time_step" in self.config[model] and not (isinstance(self.config[model]["time_step"], str) and "${" in self.config[model]["time_step"]):
                self.config[model]["prev_date"] = self.current_date - (0, 0, 0, 0, 0, int(self.config[model]["time_step"]))
            # NOTE(PG, MAM): Here we check if the time step still has a variable which might be set in a different model, and resolve this case
            elif "time_step" in self.config[model] and (isinstance(self.config[model]["time_step"], str) and "${" in self.config[model]["time_step"]):
                dt = esm_parser.find_variable(model, self.config[model]["time_step"], self.config, [], [])
                self.config[model]["prev_date"] = self.current_date - (0, 0, 0, 0, 0, int(dt))
            else:
                self.config[model]["prev_date"] = self.current_date
            # Check if lresume contains a variable which might be set in a different model, and resolve this case
            if "lresume" in self.config[model] and isinstance(self.config[model]["lresume"], str) and "${" in self.config[model]["lresume"]:
                lr = esm_parser.find_variable(model, self.config[model]["lresume"], self.config, [], [])
                self.config[model]["lresume"] = eval(lr)
            if self.config[model]["lresume"] == True and self.config["general"]["run_number"] == 1:
                self.config[model]["parent_expid"] = self.config[model][
                    "ini_parent_exp_id"
                ]
                if "parent_date" not in self.config[model]:
                    self.config[model]["parent_date"] = self.config[model][
                        "ini_parent_date"
                    ]
                self.config[model]["parent_restart_dir"] = self.config[model][
                    "ini_restart_dir"
                ]
            else:
                self.config[model]["parent_expid"] = self.config["general"][
                    "expid"
                ]
                if "parent_date" not in self.config[model]:
                    self.config[model]["parent_date"] = self.config[model][
                        "prev_date"
                    ]
                self.config[model]["parent_restart_dir"] = self.config[model][
                    "experiment_restart_in_dir"
                ]
            #print (model + "   " + str(self.config[model]["parent_date"]))


    def init_coupler(self):
        for model in list(self.config):
            if model in esm_coupler.known_couplers:
                self.coupler_config_dir = (
                    self.config["general"]["base_dir"]
                    + "/"
                    + self.config["general"]["expid"]
                    + "/run_"
                    + self.run_datestamp
                    + "/config/"
                    + model
                    + "/"
                )
                self.config["general"]["coupler_config_dir"] = self.coupler_config_dir

                self.coupler = esm_coupler.esm_coupler(self.config, model)
                self.config["general"]["coupler"] = self.coupler
                break
        self.coupler.add_files(self.config)


    def initialize_batch_system(self):
        from . import esm_batch_system
        self.batch = esm_batch_system(self.config, self.config["computer"]["batch_system"])
        self.config["general"]["batch"] = self.batch










    ################################# TIDY STUFF ###########################################

    def tidy(self):
        from . import jobclass
        """
        Performs steps for tidying up a simulation after a job has finished and
        submission of following jobs.

        This method uses two lists, ``all_files_to_copy`` and
        ``all_listed_filetypes`` to sort finished data from the **current run
        folder** back to the **main experiment folder** and submit new
        **compute** and **post-process** jobs. Files for ``log``, ``mon``,
        ``outdata``, and ``restart_out`` are gathered. The program waits until
        the job completes or an error is found (See ~self.wait_and_observe).
        Then, if necessary, the coupler cleans up it's files (unless it's a
        standalone run), and the files in the lists are copied from the **work
        folder** to the **current run folder**. A check for unknown files is
        performed (see ~self.check_for_unknown_files), files are
        moved from the  the **current run folder** to the **main experiment
        folder**, and new compute and post process jobs are started.

        Warning
        -------
            The date is changed during this routine! Be careful where you put
            any calls that may depend on date information!

        Note
        ----
            This method is also responsible for calling the next compute job as
            well as the post processing job!
        """

        called_from = self.config["general"]["last_jobtype"]

        with open(
            self.config["general"]["thisrun_scripts_dir"] + "/monitoring_file.out",
            "w",
            buffering=1,
        ) as monitor_file:
            monitor_file.write("tidy job initialized \n")
            monitor_file.write("attaching to process " + str(self.config["general"]["launcher_pid"]) + " \n")
            monitor_file.write("Called from a " + called_from + "job \n")
            last_jobid = "UNKNOWN"
            if called_from == "compute":
                with open(self.config["general"]["experiment_log_file"], "r") as logfile:
                    lastline = [l for l in logfile.readlines() if "compute" in l and "start" in l][-1]
                    last_jobid = lastline.split(" - ")[0].split()[-1]
            #monitoring_events=self.assemble_monitoring_events()

            if self.config["general"]["submitted"]:
                self.wait_and_observe(monitor_file)
            if self.config["general"]["standalone"] == False:
                self.coupler.tidy(self.config)
            monitor_file.write("job ended, starting to tidy up now \n")
            # Log job completion
            if called_from != "command_line":
                jobclass.write_to_log(self.config, [
                    called_from,
                    str(self.config["general"]["run_number"]),
                    str(self.config["general"]["current_date"]),
                    last_jobid,
                    "- done"])
            # Tell the world you're cleaning up:
            jobclass.write_to_log(self.config, [
                str(self.config["general"]["jobtype"]),
                str(self.config["general"]["run_number"]),
                str(self.config["general"]["current_date"]),
                str(self.config["general"]["jobid"]),
                "- start"])

            all_listed_filetypes=["log", "mon", "outdata", "restart_out","bin", "config", "forcing", "input", "restart_in", "ignore"]
            self.assemble_file_lists()
            self.finalize_file_lists(all_listed_filetypes)
            self.config = jobclass.copy_files_from_work_to_thisrun(self.config)

            import esm_parser
            import sys
            esm_parser.pprint_config(self.config)

            monitor_file.write("Copying stuff to main experiment folder \n")
            self.copy_all_results_to_exp()


            do_post = False
            for model in self.config:
                if "post_processing" in self.config[model]:
                    if self.config[model]["post_processing"]:
                        do_post = True

            if do_post:
                monitor_file.write("Post processing for this run:\n")
                self.command_line_config["jobtype"] = "post"
                self.command_line_config["original_command"] = self.command_line_config[
                    "original_command"
                ].replace("compute", "post")
                monitor_file.write("Initializing post object with:\n")
                monitor_file.write(str(self.command_line_config))
                this_post = SimulationSetup(self.command_line_config)
                monitor_file.write("Post object built; calling post job:\n")
                this_post()

            monitor_file.write("writing date file \n")
            self._increment_date_and_run_number()
            self._write_date_file()
#            monitor_file.write("resubmitting \n")
            self.command_line_config["jobtype"] = "compute"
            self.command_line_config["original_command"] = self.command_line_config["original_command"].replace("tidy_and_resubmit", "compute")

            jobclass.write_to_log(self.config, [
                str(self.config["general"]["jobtype"]),
                str(self.config["general"]["run_number"]),
                str(self.config["general"]["current_date"]),
                str(self.config["general"]["jobid"]),
                "- done"])

            # Clean up the run directiory if requested
            # BUG(PG): Where does the post processing run? In the run_???
            # folder? If yes, this routine needs to be moved into the post job.
            self.config = self.clean_run_dir(self.config)

            from . import database_actions
            database_actions.database_entry_success(self.config)

            # seb-wahl: end_date is by definition (search for 'end_date') smaller than final_date
            # hence we have to use next_date = current_date + increment
            if self.config["general"]["next_date"] >= self.config["general"]["final_date"]:
                monitor_file.write("Reached the end of the simulation, quitting...\n")
                jobclass.write_to_log(self.config, ["# Experiment over"], message_sep="")
            else:
                monitor_file.write("Init for next run:\n")
                next_compute = SimulationSetup(self.command_line_config)
                next_compute(kill_after_submit=False)
            self.end_it_all()

    # NOTE(PG): Turn this into a static method once we seperate out the tidy job into plugins
    # @staticmethod
    def clean_run_dir(self, config):
        """
        This plugin allows you to clean up the ``run_${DATE}`` folders.
        To do that you can use the following variables under the
        ``general`` section of your runscript (documentation follows order
        of code as it is executed):

        * ``clean_runs``: **This is the most important variable for most
          users**. It can take the following values:
            * ``True``: removes the ``run_`` directory after each run
              (**overrides every other** ``clean_`` **option**).

            * ``False``: does not remove any ``run_`` directory (default)
              if no ``clean_`` variable is defined.

            * ``<int>``: giving an integer as a value results in deleting
              the ``run_`` folders except for the last <int> runs
              (recommended option as it allows for debugging of crashed
              simulations).

          .. Note::
             ``clean_runs: (bool)`` is incompatible with
             ``clean_this_rundir`` and ``clean_runs: (int)`` is incompatible
             with ``clean_old_rundirs_except`` (an error will be raised
             after the end of the first simulation). The functionality of
             ``clean_runs`` variable **alone will suffice most of the
             standard user requirements**. If finer tunning for the removal
             of ``run_`` directories is required you can used the following
             variables instead of ``clean_runs``.

        * ``clean_this_rundir``: (bool) Removes the entire run directory
          (equivalent to ``clean_runs: (bool)``). ``clean_this_rundir: True``
          **overrides every other** ``clean_`` **option**.

        * ``clean_old_rundirs_except``: (int) Removes the entire run
          directory except for the last <x> runs (equivalent to
          ``clean_runs: (int)``).

        * ``clean_old_rundirs_keep_every``: (int) Removes the entire
          run directory except every <x>th run. Compatible with
          ``clean_old_rundirs_except`` or ``clean_runs: (int)``.

        * ``clean_<filetype>_dir``: (bool) Erases the run directory
          for a specific filetype. Compatible with all the other options.

        * ``clean_size``: (int or float) Erases all files with size
          greater than ``clean_size``, must be specified in bytes! Compatible
          with all the other options.

        Example
        -------

        To delete all the ``run_`` directories in your experiment include this
        into your runscript:

        .. code-block:: yaml

           general:
                   clean_runs: True

        To keep the last 2 ``run_`` directories:

        .. code-block:: yaml

           general:
                   clean_runs: 2

        To keep the last 2 runs and every 5 runs:

        .. code-block:: yaml

           general:
                   clean_old_rundirs_except: 2
                   clean_old_rundirs_keep_every: 5
        """
        self._clean_run_determine_user_choice(config)
        self._clean_this_rundir(config)
        self._clean_old_rundirs_except(config)
        self._clean_old_runs_filetypes(config)
        self._clean_old_runs_size(config)
        return config


    def _clean_run_determine_user_choice(self, config):
        """
        Determine user choice from a simple switch.

        The user sets::

        general:
            clean_runs: <x>

        where ``x`` can be one of:

        * ``True`` Removes the current run dir
        * ``False`` Keeps run dir
        * ``int`` (must be >= 0) keep last ``x`` run dirs
        """
        user_clean = config["general"].get("clean_runs")
        # TODO(PG): It might be nice if these sorts of checks happened earlier
        # in the job, before it even gets to this function
        if user_clean is None:
            return  # Skip the rest of the function
        if isinstance(user_clean, bool):
            if "clean_this_rundir" not in config["general"]:
                config["general"]["clean_this_rundir"] = user_clean
            else:
                print("------------------------------------------")
                print("You have set both in your config:")
                print()
                print("general:")
                print("    clean_this_rundir: ", config["general"]["clean_this_rundir"])
                print("    clean_runs: ", user_clean)
                print()
                print("Please only use one of these!")
                print("------------------------------------------")
                sys.exit(1)
        elif isinstance(user_clean, int):
            if "clean_old_rundirs_except" not in config["general"]:
                config["general"]["clean_old_rundirs_except"] = user_clean
            else:
                print("------------------------------------------")
                print("You have set both in your config:")
                print()
                print("general:")
                print("    clean_old_rundirs_except: ", config["general"]["clean_old_rundirs_except"])
                print("    clean_runs: ", user_clean)
                print()
                print("Please only use one of these!")
                print("------------------------------------------")
                sys.exit(1)
        else:
            print("------------------------------------------")
            print("Type Error!")
            print("You have set this in your config:")
            print("general:")
            print("    clean_runs: ", user_clean)
            print()
            print("This is of type: ", type(user_clean))
            print("However, only the following types are valid:")
            print("   * boolean")
            print("   * integer (greater or equal to 0!)")
            print("Please correct that")
            print("------------------------------------------")
            sys.exit(1)


    def _clean_this_rundir(self, config):
        if config['general'].get("clean_this_rundir", False):
            rm_r(config['general']['thisrun_dir'])

    def _clean_old_rundirs_except(self, config):
        all_run_folders_in_experiment = RunFolders(config)

        number_rundirs_keep_every = config["general"].get("clean_old_rundirs_keep_every")
        runs_to_keep_via_keepevery = []
        if number_rundirs_keep_every:
            try:
                assert isinstance(number_rundirs_keep_every, int)
                assert number_rundirs_keep_every >= 1
            except AssertionError:
                print("Please ensure that you use an integer in your configuration:")
                print("-------------------------------------------------------------")
                print()
                print("general:")
                print("   clean_old_rundirs_keep_every: <x>")
                print()
                print("-------------------------------------------------------------")
                print("<x> **MUST** be an integer greater or equal than 1!")
                sys.exit(1)
            runs_to_keep_via_keepevery = all_run_folders_in_experiment[::number_rundirs_keep_every]

        number_rundirs_to_keep = config["general"].get("clean_old_rundirs_except")
        runs_to_keep_via_end_select = []
        if number_rundirs_to_keep:
            try:
                assert isinstance(number_rundirs_to_keep, int)
                assert number_rundirs_to_keep > 1
            except AssertionError:
                print("Please ensure that you use an integer in your configuration:")
                print("-------------------------------------------------------------")
                print()
                print("general:")
                print("   clean_old_rundirs_except: <x>")
                print()
                print("-------------------------------------------------------------")
                print("<x> **MUST** be an integer greater than 1!")
                sys.exit(1)
            runs_to_keep_via_end_select = all_run_folders_in_experiment[-number_rundirs_to_keep:]
        if  number_rundirs_keep_every or number_rundirs_to_keep:
            runs_to_keep = set(runs_to_keep_via_keepevery + runs_to_keep_via_end_select)
        else:
            runs_to_keep = set(all_run_folders_in_experiment)
        runs_to_clean = set(all_run_folders_in_experiment) - runs_to_keep
        for run in list(runs_to_clean):
            rm_r(run)

    def _clean_old_runs_filetypes(self, config):
        all_filetypes = config['general']['all_filetypes']
        for filetype in all_filetypes:
            if config['general'].get("clean_" + filetype + "_dir", False):
                rm_r(config['general']['thisrun_' + filetype + '_dir'])

    def _clean_old_runs_size(self, config):
        rmsize = config['general'].get("clean_size", False)
        if rmsize:
            flist = []
            for root, _, files in os.walk(config['general']['thisrun_dir']):
                for file_ in files:
                    size = os.path.getsize(root + "/" + file_)
                    if size >= rmsize:
                        flist.append(root + "/" + file_)
            for file_ in flist:
                os.remove(file_)

    def wait_and_observe(self, monitor_file):
        import time
        thistime = 0
        error_check_list = self.assemble_error_list()
        while self.job_is_still_running():
            monitor_file.write("still running \n")
            error_check_list = self.check_for_errors(error_check_list, thistime, monitor_file)
            thistime = thistime + 10
            time.sleep(10)
        thistime = thistime + 100000000
        error_check_list = self.check_for_errors(error_check_list, thistime, monitor_file)


    def assemble_error_list(self):
        gconfig = self.config["general"]
        known_methods = ["warn", "kill"]
        stdout = gconfig["experiment_dir"] + "/scripts/" +  gconfig["expid"] + "_compute_" + gconfig["run_datestamp"] + "_" + gconfig["jobid"] + ".log"

        error_list = [("error", stdout, "warn", 60, 60, "keyword error detected, watch out")]

        for model in self.config:
            if "check_error" in self.config[model]:
                for trigger in self.config[model]["check_error"]:
                    search_file = stdout
                    method = "warn"
                    frequency = 60
                    message = "keyword " + trigger + " detected, watch out"
                    if isinstance(self.config[model]["check_error"][trigger], dict):
                        if "file" in  self.config[model]["check_error"][trigger]:
                            search_file = self.config[model]["check_error"][trigger]["file"]
                            if search_file == "stdout" or search_file == "stderr":
                                search_file = stdout
                        if "method" in  self.config[model]["check_error"][trigger]:
                            method = self.config[model]["check_error"][trigger]["method"]
                            if method not in known_methods:
                                method = "warn"
                        if "message" in  self.config[model]["check_error"][trigger]:
                            message = self.config[model]["check_error"][trigger]["message"]
                        if "frequency" in  self.config[model]["check_error"][trigger]:
                            frequency = self.config[model]["check_error"][trigger]["frequency"]
                            try:
                                frequency = int(frequency)
                            except:
                                frequency = 60
                    elif isinstance(self.config[model]["check_error"][trigger], str) :
                        pass
                    else:
                        continue
                    error_list.append((trigger, search_file, method, frequency, frequency, message))

        return error_list


    def check_for_errors(self, error_check_list, time, monitor_file):
        import re
        new_list = []
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
                                    harakiri = "scancel " + self.config["general"]["jobid"]
                                    monitor_file.write("ERROR: " + message + "\n")
                                    monitor_file.write("Will kill the run now..." + "\n")
                                    monitor_file.flush()
                                    print("ERROR: " + message)
                                    print("Will kill the run now...", flush=True)
                                    from . import database_actions
                                    database_actions.database_entry_crashed(self.config)
                                    os.system(harakiri)
                                    sys.exit(42)
                next_check += frequency
            if warned == 0:
                new_list.append((trigger, search_file, method, next_check, frequency, message))
        return new_list



    def job_is_still_running(self):
        import psutil
        if psutil.pid_exists(self.config["general"]["launcher_pid"]):
            return True
        return False

    def add_submission_info(self):
        from . import esm_batch_system
        bs = esm_batch_system(self.config, self.config["computer"]["batch_system"])

        submitted = bs.check_if_submitted()
        if submitted:
            jobid = bs.get_jobid()
        else:
            jobid = os.getpid()

        self.config["general"]["submitted"] = submitted
        self.config["general"]["jobid"] = jobid



    def _increment_date_and_run_number(self):
        self.run_number += 1
        self.current_date += self.delta_date

    def _write_date_file(self, date_file=None):
        if not date_file:
            date_file = (
                self.config["general"]["experiment_scripts_dir"]
                + "/"
                + self.config["general"]["expid"]
                + "_"
                + self.config["general"]["setup_name"]
                + ".date"
            )
        with open(date_file, "w") as date_file:
            date_file.write(self.current_date.output() + " " + str(self.run_number))


















    def assemble_file_lists(self):

        from . import filelists

        self.config = filelists.rename_sources_to_targets(self.config)
        self.config = filelists.choose_needed_files(self.config)
        self.config = filelists.complete_targets(self.config)
        self.config = filelists.complete_sources(self.config)
        self.config = filelists.replace_year_placeholder(self.config)


    def finalize_file_lists(self, filetypes):
        # needs to be called right before copying
        from . import filelists

        self.config = filelists.globbing(self.config)
        self.config = filelists.target_subfolders(self.config)
        self.config = filelists.assemble_intermediate_files_and_finalize_targets(self.config)
        self.config = filelists.complete_restart_in(self.config)
        self.config = filelists.check_for_unknown_files(self.config)
        self.config = filelists.log_used_files(self.config, filetypes)








    @staticmethod
    def merge_thisrun_into_experiment(config):

        import os
        # to should be thisrun, work or experiment

        for filetype in config["general"]["all_model_filetypes"]:
            for model in config["general"]["valid_model_names"]:
                from_dir = config[model]["thisrun_" + filetype + "dir"]
                to_dir = config[model]["experiment_" + filetype + "dir"] + "/" + config["general"]["run_datestamp"]
                os.rename(from_dir, to_dir)

        for filetype in config["general"]["all_filetypes"]:
            from_dir = config["general"]["thisrun_" + filetype + "dir"]
            to_dir = config["general"]["experiment_" + filetype + "dir"] + "/" + config["general"]["run_datestamp"]
            os.rename(from_dir, to_dir)

        return config




    def copy_all_results_to_exp(self):
        import filecmp

        for root, dirs, files in os.walk(self.config["general"]["thisrun_dir"], topdown=False):
            print ("Working on folder: " + root)
            if root.startswith(self.config["general"]["thisrun_work_dir"]) or root.endswith("/work"):
                print ("Skipping files in work.")
                continue
            for name in files:
                source = os.path.join(root, name)
                print ("File: " + source)
                destination = source.replace(self.config["general"]["thisrun_dir"], self.config["general"]["experiment_dir"])
                destination_path = destination.rsplit("/", 1)[0]
                if not os.path.exists(destination_path):
                    os.makedirs(destination_path)
                if not os.path.islink(source):
                    if os.path.isfile(destination):
                        if filecmp.cmp(source, destination):
                            print ("File " + source + " has not changed, skipping.")
                            continue
                        else:
                            if os.path.isfile(destination + "_" + self.run_datestamp):
                                print ("Don't know where to move " + destination +", file exists")
                                continue
                            else:
                                if os.path.islink(destination):
                                    os.remove(destination)
                                else:
                                    os.rename(destination, destination + "_" + self.last_run_datestamp)
                                newdestination = destination + "_" + self.run_datestamp
                                print ("Moving file " + source + " to " + newdestination)
                                os.rename(source, newdestination)
                                os.symlink(newdestination, destination)
                                continue
                    try:
                        print ("Moving file " + source + " to " + destination)
                        os.rename(source, destination)
                    except:
                        print(">>>>>>>>>  Something went wrong moving " + source + " to " + destination)
                else:
                    linkdest = os.path.realpath(source)
                    newlinkdest = destination.rsplit("/", 1)[0] + "/" + linkdest.rsplit("/", 1)[-1]
                    if os.path.islink(destination):
                        os.remove(destination)
                    if os.path.isfile(destination):
                        os.rename(destination, destination + "_" + self.last_run_datestamp)
                    os.symlink(newlinkdest, destination)
