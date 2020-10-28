import copy
import logging
import os
import shutil
import sys
from datetime import datetime

import esm_rcfile
import six
import tqdm

import esm_plugin_manager


def evaluate(config, job_type, recipe_name):

    # Check for a user defined compute recipe in the setup section of the
    # general section. If nothing is found, recipe_steps should evaluate to
    # None and the default is used
    try:
        setup_name = config["general"]["setup_name"]
        recipe_steps = config.get(setup_name, {}).get(recipe_name) or config["general"].get(recipe_name)
    except KeyError:
        print("Your configuration is incorrect, and should include headings for %s as well as general!" % setup_name)
        sys.exit(1)

    recipefile = esm_rcfile.FUNCTION_PATH + "/esm_software/esm_runscripts/esm_runscripts.yaml"
    pluginsfile = esm_rcfile.FUNCTION_PATH + "/esm_software/esm_runscripts/esm_plugins.yaml"

    framework_recipe = esm_plugin_manager.read_recipe(recipefile, {"job_type": job_type})
    if recipe_steps:
        framework_recipe["recipe"] = recipe_steps
    framework_plugins = esm_plugin_manager.read_plugin_information(pluginsfile, framework_recipe)
    esm_plugin_manager.check_plugin_availability(framework_plugins)

    config = esm_plugin_manager.work_through_recipe(framework_recipe, framework_plugins, config)
    return config


    #########################################################################################
    #                                   general stuff                                       #
    #########################################################################################
    @staticmethod
    def end_it_all(config, silent=False):
        if config["general"]["profile"]:
            for line in timing_info:
                print(line)
        if not silent:
            print("Exiting entire Python process!")
        sys.exit()



    @staticmethod
    def write_to_log(config, message, message_sep=None):
        """
        Puts a message into the experiment log file

        Parameters
        ----------
        message : list
            A list of the message elements; which is joined by either (highest
            to lowest): 1) the message_sep argument passed to the method, 2)
            The user's chosen seperator, as written in
            ``self.config["general"]["experiment_log_file_message_sep"]``, 3)
            An empty space ``" "``.
        message_sep : None
            The hard-coded message seperator to use; which ignores user choices.

        Note
        ----
        The user can control two things regarding the logfile format:

        1) The datestamp formatting, whjich is taken from the config
           section ``general.experiment_log_file_dateformat``.
        2) The message seperators; taken from
           ``general.experiment_log_file_message_sep``. Note that if the
           programmer passes a ``message_sep`` argument; this one wins over
           the user choice.
        """
        try:
            with open(config["general"]["experiment_log_file"], "a+") as logfile:
                line = assemble_log_message(config, message, message_sep)
                logfile.write(line + "\n")
        except KeyError:
            print("Sorry; couldn't find 'experiment_log_file' in config['general']...")
            esm_parser.pprint_config(self.config["general"])
            raise


    @staticmethod
    def assemble_log_message(config, message, message_sep=None, timestampStr_from_Unix=False):
        """Assembles message for log file. See doc for write_to_log"""
        message = [str(i) for i in message]
        dateTimeObj = datetime.now()
        strftime_str = config["general"].get("experiment_log_file_dateformat", "%c")
        if message_sep is None:
            message_sep = config["general"].get("experiment_log_file_message_sep", " ")
        if timestampStr_from_Unix:
            timestampStr = "$(date +"+strftime_str+")"
        else:
            timestampStr = dateTimeObj.strftime(strftime_str)
        # TODO: Do we want to be able to specify a timestamp seperator as well?
        line = timestampStr + " : " + message_sep.join(message)
        return line

#
#    @staticmethod
#    def copy_files_from_work_to_thisrun(config, target = "thisrun", source = "work"):
#        # idea is to unify all the copy routines by giving a parameter that tells from where to where stuff is to be copied
#
#        # source = "init", "thisrun", "work"
#        # target = "thisrun", "work", "experiment"
#
#        six.print_("=" * 80, "\n")
#        six.print_("COPYING STUFF FROM " + source.upper() + " TO " + target.upper() + " FOLDERS")
#
#        successful_files = []
#        missing_files = {}
#        # TODO: Check if we are on login node or elsewhere for the progress
#        # bar, it doesn't make sense on the compute nodes:
#
#        relevant_filetypes = config["general"]["all_model_filetypes"]
#        if target == "work" or source == "init":
#            relevant_filetypes = config["general"]["in_filetypes"]
##        else:
#            relevant_filetypes = config["general"]["out_filetypes"]
#
#        for filetype in relevant_filetypes:
#            for model in config["general"]["valid_model_names"] + ["general"]:
#                if filetype + "_sources" in config[model] and not filetype == "ignore":
#                    for categ in config[model][filetype + "_sources"]:
#                        file_source = config[model][filetype + "_sources"][categ]
#                        if target == "thisrun":
#                            file_target = config[model][filetype + "_intermediate"][categ]
#                        else:
#                            file_target = config[model][filetype + "_targets"][categ]
#                        dest_dir = file_target.rsplit("/", 1)[0]
#                        try:
#                            if not os.path.isdir(dest_dir):
#                                os.makedirs(dest_dir)
 #                           shutil.copy2(file_source, file_target)
 #                           print ("Copying " + file_source)
#                            print ("        ---> " + file_target)
#                            successful_files.append(file_source)
#                        except IOError:
#                            missing_files.update({file_target: file_source})
#        if missing_files:
#            if not "files_missing_when_preparing_run" in config["general"]:
#                config["general"]["files_missing_when_preparing_run"] = {}
#            six.print_("--- WARNING: These files were missing:")
#            for missing_file in missing_files:
#                print( "  - " + missing_file + ": " + missing_files[missing_file])
#            config["general"]["files_missing_when_preparing_run"].update(missing_files)
#        return config


