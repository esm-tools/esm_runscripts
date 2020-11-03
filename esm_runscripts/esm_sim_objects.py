"""
Documentation goes here
"""
from datetime import datetime
from io import StringIO
import collections
import logging
import os
import pdb
import shutil
import sys

import six
import tqdm
import yaml
import time


from esm_calendar import Date, Calendar
import esm_parser
from . import esm_coupler
from . import helpers
#import .esm_coupler
from esm_profile import *

import pprint

pp = pprint.PrettyPrinter(indent=4)


def date_representer(dumper, date):
    return dumper.represent_str("%s" % date.output())


yaml.add_representer(Date, date_representer)


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

        from . import prepare
        self.config = prepare.run_job(self.config)



    def __call__(self, *args, **kwargs):
        if self.config["general"]["jobtype"] == "compute":
            self.compute(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "tidy_and_resubmit":
            self.tidy(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "post":
            self.postprocess(*args, **kwargs)
        else:
            print("Unknown jobtype specified! Goodbye...")
            helpers.end_it_all(self.config)





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
        self.config = compute.run_job(self.config)

        if kill_after_submit:
            helpers.end_it_all(self.config)
###############################################       POSTPROCESS ######################################






    def postprocess(self):
        from . import batch_system
        """
        Calls post processing routines for this run.
        """
        with open(
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
            batch_system.write_simple_runscript(self.config)
            self.config = batch_system.submit(self.config)

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















    ################################# TIDY STUFF ###########################################

    def tidy(self):
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


        from . import tidy
        with open(
            self.config["general"]["thisrun_scripts_dir"] + "/monitoring_file.out",
            "w",
            buffering=1,
        ) as monitor_file:

            self.config["general"]["monitor_file"] = monitor_file
            self.config = tidy.run_job(self.config)

        helpers.end_it_all(self.config)


