#!/usr/bin/env python
"""
A small wrapper that combines the shell interface and the Python interface
"""

# Import from Python Standard Library
import argparse
import logging
import os
import sys

from .sim_objects import *
from loguru import logger
from esm_motd import check_all_esm_packages


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
        "-v", "--verbose", help="Be verbose", action="store_true", default=False,
    )

    parser.add_argument(
        "-e", "--expid", help="The experiment ID to use", default="test"
    )

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
        "-p", "--pid", help="The PID of the task to observe.", default=-666,
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

    check_all_esm_packages()

    ARGS = parse_shargs()
    logger.debug("Working here: %s", os.getcwd())
    logger.debug("This file is here: %s", os.path.dirname(__file__))
    logger.debug(
        "The main function directory should be: %s",
        os.getcwd() + "/" + os.path.dirname(__file__) + "/../",
    )

    check = False
    profile = False
    update = False
    expid = "test"
    pid = -666
    jobtype = "compute"
    verbose = False

    parsed_args = vars(ARGS)

    original_command = ""
    for argument in sys.argv[1:]:
        original_command = original_command + argument + " "

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
    if "verbose" in parsed_args:
        verbose = parsed_args["verbose"]

    command_line_config = {}
    command_line_config["check"] = check
    command_line_config["profile"] = profile
    command_line_config["update"] = update
    command_line_config["expid"] = expid
    command_line_config["launcher_pid"] = pid
    command_line_config["jobtype"] = jobtype
    command_line_config["scriptname"] = ARGS.runscript
    command_line_config["last_jobtype"] = ARGS.last_jobtype
    command_line_config["verbose"] = verbose

    command_line_config["original_command"] = original_command.strip()
    command_line_config["started_from"] = os.getcwd()

    if verbose:
        print("Started from: ", command_line_config["started_from"])
        print("starting : ", jobtype)

    Setup = SimulationSetup(command_line_config)
    Setup()
