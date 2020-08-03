#!/usr/bin/env python
"""
A small wrapper that combines the shell interface and the Python interface
"""

# Import from Python Standard Library
import argparse
import logging
import os
import sys

# Import from 3rd Party packages
import coloredlogs

from .esm_sim_objects import *

# Logger and related constants
logger = logging.getLogger("root")
DEBUG_MODE = logger.level == logging.DEBUG
FORMAT = (
    "[%(asctime)s,%(msecs)03d:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
)
f_handler = logging.FileHandler("file.log")
f_handler.setFormatter(FORMAT)
logger.addHandler(f_handler)


def parse_shargs():
    """ The arg parser for interactive use """
    parser = argparse.ArgumentParser()
    parser.add_argument("runscript", default=None)

    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.ERROR,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )

    parser.add_argument(
        "-e", "--expid", help="The experiment ID to use", default="test"
    )

# kh 15.07.20
    parser.add_argument(
        "--modify-config",
        "-m",
        dest="modify",
        help="[m]odify configuration", 
        default="", # kh 15.07.20 "usermods.yaml"
    )

# kh 21.07.20
#    parser.add_argument(
#        "--ignore-errors",
#        "-i",
#        help="Ignore errors",
#        default=False,
#        action="store_true",
#    )

    parser.add_argument(
        "-c",
        "--check",
        help="Run in check mode (don't submit job to supercomputer)",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-P",
        "--profile",
        help="Write profiling information (esm-tools)",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-j",
        "--last_jobtype",
        help="Write the jobtype this run was called from (esm-tools internal)",
        default="command_line",
    )

    parser.add_argument(
        "-t",
        "--task",
        help="The task to run. Choose from: compute, post, couple, tidy_and_resubmit",
        default="compute",
    )

    parser.add_argument(
        "-p",
        "--pid",
        help="The PID of the task to observe.",
        default=-666,
    )


    parser.add_argument("-x", "--exclude", help="e[x]clude this step", default=None)
    parser.add_argument("-o", "--only", help="[o]nly do this step", default=None)
    parser.add_argument(
        "-r", "--resume-from", help="[r]esume from this step", default=None
    )

    # PG: Might not work anymore:
    parser.add_argument(
        "-U",
        "--update",
        help="[U]date the tools from the current version",
        default=False,
        action="store_true",
    )

    return parser.parse_args()


def main():
    ARGS = parse_shargs()
    coloredlogs.install(fmt=FORMAT, level=ARGS.loglevel)

    logger.info("Working here: %s", os.getcwd())
    logger.info("This file is here: %s", os.path.dirname(__file__))
    logger.info(
        "The main function directory should be: %s",
        os.getcwd() + "/" + os.path.dirname(__file__) + "/../",
    )

    check = False
    profile = False
    update = False
    expid = "test"
    pid = -666
    jobtype = "compute"

    parsed_args = vars(ARGS)

# kh 15.07.20
    modify_config_file = ""
    modify_config_file_abspath = ""
    if "modify" in parsed_args: 
        modify_config_file = parsed_args["modify"]

    if "check" in parsed_args:
        check = parsed_args["check"]
    if "profile" in parsed_args:
        profile = parsed_args["profile"]
    if "pid" in parsed_args:
        pid = parsed_args["pid"]
    if "update" in parsed_args:
        update = parsed_args["update"]
    if "expid" in parsed_args:
        expid = parsed_args["expid"]
    if "task" in parsed_args:
        jobtype = parsed_args["task"]

    command_line_config={}

# kh 15.07.20
    if modify_config_file:
        modify_config_file_abspath = os.path.abspath(modify_config_file)
        modify_config = esm_parser.yaml_file_to_dict(modify_config_file_abspath)
    else:
        modify_config = {}

    command_line_config["modify_config_file"] = modify_config_file
    command_line_config["modify_config_file_abspath"] = modify_config_file_abspath
    command_line_config["modify_config"] = modify_config

    command_line_config["check"] = check
    command_line_config["profile"] = profile
    command_line_config["update"] = update
    command_line_config["expid"] = expid
    command_line_config["launcher_pid"] = pid
    command_line_config["jobtype"] = jobtype
    command_line_config["scriptname"] = ARGS.runscript
    command_line_config["last_jobtype"] = ARGS.last_jobtype

    modify_config_argument_found = False
    original_command = ""
    for argument in sys.argv[1:]:
        if modify_config_argument_found:

# kh 30.07.20 prepare passing of the modify_config yaml file via "tidy_and_resubmit" etc. by
# replacing a potentially relative path with its absolute path
            original_command = original_command + modify_config_file_abspath + " "
            modify_config_argument_found = False
        else:
            original_command = original_command + argument + " "
            if argument.startswith(("-m", "--m")):
                modify_config_argument_found = True

    command_line_config["original_command"] = original_command.strip()
    command_line_config["started_from"] = os.getcwd()

    print ("Started from: ", command_line_config["started_from"])
    print ("starting : ", jobtype)

    Setup = SimulationSetup(command_line_config)
    Setup()
