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

def vprint(config, message):
    if config["general"]["verbose"]:
        print (message)


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
def end_it_all(config):
    if config["general"]["profile"]:
        for line in timing_info:
            print(line)
    if config["general"]["verbose"]:
        print("Exiting entire Python process!")
    sys.exit()


def write_to_log(config, message, message_sep=None):
    """
    Puts a message into the experiment log file

    Parameters
    ----------
    message : list
        A list of the message elements; which is joined by either (highest
        to lowest): 1) the message_sep argument passed to the method, 2)
        The user's chosen seperator, as written in
        ``config["general"]["experiment_log_file_message_sep"]``, 3)
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
        import esm_parser
        print("Sorry; couldn't find 'experiment_log_file' in config['general']...")
        esm_parser.pprint_config(config["general"])
        raise

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

