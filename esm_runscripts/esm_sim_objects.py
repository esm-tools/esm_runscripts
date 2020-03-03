"""
Documentation goes here
"""
import collections
import logging
import os
import pdb
import shutil
import sys
from io import StringIO

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


class SimulationSetup(object):
    def __init__(self, command_line_config = None, user_config = None):

        if not command_line_config and not user_config:
            print ("SimulationSetup needs to be initialized with either command_line_config or user_config.")
            sys.exit(-1)
        if command_line_config:
            self.command_line_config = command_line_config
        if not user_config:
            user_config = self.get_user_config_from_command_line(command_line_config)
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


    def compute(self, kill_after_submit=True):
        """
        All steps needed for a model computation.

        Parameters
        ----------
        kill_after_submit : bool
            Default ``True``. If set, the entire Python instance is killed with
            a ``sys.exit()`` as the very last after job submission.
        """
        # make folders
        self._create_folders(self.config["general"], self.all_filetypes)
        self._create_component_folders()
        # write config
        self._write_finalized_config()
        self.copy_tools_to_thisrun()
        # copy date_file etc. into experiment
        self._copy_preliminary_files_from_experiment_to_thisrun()
        # little bit of output
        self._show_simulation_info()
        # assemble file lists and copy everything to thisrun
        self.prepare()
        # write sad file
        self.write_simple_runscript()
        if self.config["general"]["check"]:
            self.end_it_all()
        self.submit()
        if kill_after_submit:
            self.end_it_all()

    def postprocess(self):
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
            self.write_simple_runscript(post_task_list, write_tidy_call=False)
            self.submit()

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
        for component in self.components:
            post_file.write(40*"+ "+"\n")
            post_file.write("Generating post-processing tasks for: %s \n" % component)

            post_task_list.append("\n#Postprocessing %s\n" % component)
            post_task_list.append("cd "+component.config["experiment_outdata_dir"]+"\n")

            pconfig_tasks = component.config.get('postprocess_tasks', {})
            post_file.write("Configuration for post processing: %s \n" % pconfig_tasks)
            for outfile in pconfig_tasks:
                post_file.write("Generating task to create: %s \n" % outfile)
                ofile_config = pconfig_tasks[outfile]
                # TODO(PG): This can be cleaned up. I probably actually want a
                # ChainMap here for more than just the bottom...
                #
                # Run CDO tasks (default)
                task_definition = component.config.get("postprocess_task_definitions", {}).get(ofile_config['post_process'])
                method_definition = component.config.get("postprocess_method_definitions", {}).get(task_definition['method'])

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


    def end_it_all(self):
        if self.config["general"]["profile"]:
            for line in timing_info:
                print(line)
        print("Exiting entire Python process!")
        sys.exit()

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
        with open(
            self.config["general"]["thisrun_scripts_dir"] + "/monitoring_file.out",
            "w",
            buffering=1,
        ) as monitor_file:
            monitor_file.write("tidy job initialized \n")
            monitor_file.write("attaching to process " + str(self.config["general"]["launcher_pid"]) + " \n")
            #monitoring_events=self.assemble_monitoring_events()

            filetypes=["log", "mon", "outdata", "restart_out"]
            all_files_to_copy=self.assemble_file_lists(filetypes)
            if self.config["general"]["submitted"]:
                self.wait_and_observe(monitor_file)
            if self.config["general"]["standalone"] == False:
                self.coupler.tidy(self.config)
            monitor_file.write("job ended, starting to tidy up now \n")
            self.copy_files_from_work_to_thisrun(all_files_to_copy)
            all_listed_filetypes=["log", "mon", "outdata", "restart_out","bin", "config", "forcing", "input", "restart_in", "ignore"]
            all_files_to_check = self.assemble_file_lists(all_listed_filetypes)
            self.check_for_unknown_files(all_files_to_check)

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

            if self.config["general"]["end_date"] >= self.config["general"]["final_date"]:
                monitor_file.write("Reached the end of the simulation, quitting...\n")
            else:
                monitor_file.write("Init for next run:\n")
                next_compute = SimulationSetup(self.command_line_config)
                next_compute(kill_after_submit=False)
            self.end_it_all()

            

    def prepare(self):
        filetypes=["bin", "config", "forcing", "input", "restart_in"]
        all_files_to_copy = self.assemble_file_lists(filetypes)
        self.copy_files_to_thisrun(all_files_to_copy)
        self.modify_namelists()
        self.modify_files()  # ??? Doing nothing yet
        self.create_new_files(all_files_to_copy)

        if self.config["general"]["standalone"] == False:
            self.prepare_coupler_files(all_files_to_copy)
        self.add_batch_hostfile(all_files_to_copy)
        self.copy_files_to_work(all_files_to_copy)





    #####################################    JOB PHASES   ####################################


    ##########################    ASSEMBLE ALL THE INFORMATION  ##############################


    def get_user_config_from_command_line(self, command_line_config):
        try:
            user_config = esm_parser.initialize_from_yaml(command_line_config["scriptname"])
            if not "additional_files" in user_config["general"]:
                user_config["general"]["additional_files"] = []
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
        self._initialize_components()
        self.add_submission_info()
        self.initialize_batch_system()

        if self.config["general"]["standalone"] == False:
            self.init_coupler()


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
                    os.mkdir(destination_path)
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

    def check_for_unknown_files(self, listed_files):
        import glob
        #files = os.listdir(self.config["general"]["thisrun_work_dir"])
        files = glob.iglob(self.config["general"]["thisrun_work_dir"] + '**/*', recursive = True)
        known_files = ["hostfile_srun", "namcouple"]
        unknown_files = []
        for thisfile in files:
            if thisfile.rsplit("/", 1)[0] in known_files:
                break
            found = False
            file_in_list = False
            file_in_work = False
            if os.path.isfile(thisfile):
                for (file_source, filedir_interm, filename_interm, filename_target, subfolder) in listed_files:
                    file_intermediate = filedir_interm + subfolder + filename_interm
                    #print (file_target.split("/", -1)[-1] + "    " + thisfile)
                    if os.path.join(self.config["general"]["thisrun_work_dir"], subfolder + filename_target)  == thisfile:
                        file_in_list = True
                        if "ignore" in file_intermediate:
                            file_in_work = True
                        if os.path.isfile(file_intermediate):
                            found = True
                            file_in_work = True
                        break
                if not found:
                    unknown_files.append(thisfile)
                if not file_in_list:
                    print ("File is not in list: " + thisfile )
                elif not file_in_work:
                    print ("File is not where it should be: ", thisfile)

#        for thisfile in unknown_files:


    @timing
    def copy_tools_to_thisrun(self):
        import esm_rcfile
        gconfig = self.config["general"]

        fromdir = os.path.normpath(gconfig["started_from"])
        scriptsdir = os.path.normpath(gconfig["experiment_scripts_dir"])

        tools_dir = scriptsdir + "/esm_tools/functions"
        namelists_dir = scriptsdir + "/esm_tools/namelists"

        print ("Started from :", fromdir)
        print ("Scripts Dir : ", scriptsdir)

        if os.path.isdir(tools_dir) and gconfig["update"]:
            shutil.rmtree(tools_dir, ignore_errors=True)
        if os.path.isdir(namelists_dir) and gconfig["update"]:
            shutil.rmtree(namelists_dir, ignore_errors=True)

        if not os.path.isdir(tools_dir):
            print("Copying from: ", esm_rcfile.FUNCTION_PATH)
            shutil.copytree(esm_rcfile.FUNCTION_PATH, tools_dir) 
        if not os.path.isdir(namelists_dir):
            shutil.copytree(esm_rcfile.get_rc_entry("NAMELIST_PATH"), namelists_dir) 

        if (fromdir == scriptsdir) and not gconfig["update"]:
            print ("Started from the experiment folder, continuing...")
        else:
            if not fromdir == scriptsdir:
                print ("Not started from experiment folder, restarting...")
            else:
                print ("Tools were updated, restarting...")

            if not os.path.isfile(scriptsdir + "/" + gconfig["scriptname"]):
                oldscript = fromdir + "/" + gconfig["scriptname"]
                print (oldscript)
                shutil.copy2 (oldscript, scriptsdir)

            for tfile in gconfig["additional_files"]:
                if not os.path.isfile(scriptsdir + "/" + tfile):
                     shutil.copy2 (fromdir + "/" + tfile, scriptsdir)

            restart_command = ("cd " + scriptsdir + "; " + \
                               "esm_runscripts " + \
                               gconfig["original_command"].replace("-U", ""))           
            print (restart_command)
            os.system( restart_command )

            gconfig["profile"] = False
            self.end_it_all()




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
        stdout = gconfig["thisrun_scripts_dir"] + "/" +  gconfig["expid"] + "_compute_" + gconfig["jobid"] + ".log"

        error_list = [("error", stdout, "warn", 60, 60, "keyword error detected, watch out")]

        for model in self.config:
            if "check_error" in self.config[model]:
                for trigger in self.config[model]["check_error"]:
                    search_file = stdout
                    method = "warn"
                    frequency = 60
                    message = "keyword " + trigger + " detected, watch out"
                    if type (self.config[model]["check_error"][trigger] ) == dict:
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
                    elif type( self.config[model]["check_error"][trigger] ) == str:
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
        bs = esm_batch_system.esm_batch_system(self.config, self.config["computer"]["batch_system"])

        submitted = bs.check_if_submitted()
        if submitted:
            jobid = bs.get_jobid()
        else:
            jobid = os.getpid()

        self.config["general"]["submitted"] = submitted
        self.config["general"]["jobid"] = jobid


    def _add_all_folders(self):
        self.all_filetypes = ["analysis", "config", "log", "mon", "scripts", "ignore",  "unknown"]
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
            logging.info("Date file read from %s", date_file)
            with open(date_file) as date_file:
                date, self.run_number = date_file.readline().strip().split()
                self.run_number = int(self.run_number)
            write_file = False
        else:
            logging.info("No date file found %s", date_file)
            logging.info("Initializing run_number=1 and date=18500101")
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
                if type(user_lresume) == str:
                    if user_lresume == "0" or user_lresume.upper() == "FALSE":
                        user_lresume = False
                    elif user_lresume == "1" or user_lresume.upper() == "TRUE":
                        user_lresume = True
                elif type(user_lresume) == int:
                    if user_lresume == 0:
                        user_lresume = False
                    elif user_lresume == 1:
                        user_lresume = True
                config[model]["lresume"] = user_lresume

        # needs to happen AFTER a run!
        # if write_file:
        #    self._write_date_file()

        logging.info("current_date = %s", self.current_date)
        logging.info("run_number = %s", self.run_number)

    #########################       PREPARE EXPERIMENT / WORK    #############################

    def write_simple_runscript(self, commands=None, write_tidy_call=True):
        sadfilename = self.get_sad_filename()
        header = self.get_batch_header()
        environment = self.get_environment()
        commands = commands or self.get_run_commands()
        tidy_call =  "esm_runscripts " + self.config["general"]["scriptname"] + " -e " + self.config["general"]["expid"] + " -t tidy_and_resubmit -p ${process}"

        with open(sadfilename, "w") as sadfile:
            for line in header:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            for line in environment:
                sadfile.write(line + "\n")
            sadfile.write("\n")
            sadfile.write("cd "+ self.config["general"]["thisrun_work_dir"] + "\n")
            for line in commands: 
                sadfile.write(line + "\n")
            sadfile.write("process=$! \n")
            sadfile.write("cd "+ self.config["general"]["experiment_scripts_dir"] + "\n")
            if write_tidy_call:
                sadfile.write(tidy_call + "\n")

        self.submit_command = self.get_submit_command(sadfilename)

        six.print_("\n", 40 * "+ ")
        six.print_("Contents of ",sadfilename, ":")
        with open(sadfilename, "r") as fin:
            print (fin.read())
        if os.path.isfile(self.batch.bs.filename):
            six.print_("\n", 40 * "+ ")
            six.print_("Contents of ",self.batch.bs.filename, ":")
            with open(self.batch.bs.filename, "r") as fin:
                print (fin.read())
    
    def get_sad_filename(self):
        folder = self.config["general"]["thisrun_scripts_dir"]
        expid = self.config["general"]["expid"]
        startdate = self.config["general"]["current_date"]
        enddate = self.config["general"]["end_date"]
        return folder + "/" + expid+"_"+self.config["general"]["jobtype"]+"_"+self.run_datestamp+".sad"

    def get_batch_header(self):
        header = []
        batch_system = self.config["computer"]
        if "sh_interpreter" in batch_system:
            header.append("#!"+batch_system["sh_interpreter"])
        tasks = self.calculate_requirements()
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
        if self.config["general"]["jobtype"] in ["compute", "tidy_and_resume"]:
            conditional_flags.append("exclusive_flag")
        for flag in conditional_flags:
            if flag in batch_system and not batch_system[flag].strip() == "":
                all_flags.append(flag)
        for flag in all_flags:
            for (tag, repl) in replacement_tags:
                batch_system[flag] = batch_system[flag].replace(tag, str(repl))
            header.append(batch_system["header_start"] + " " + batch_system[flag])
        return header
    
    def calculate_requirements(self):
        tasks = 0
        if self.config["general"]["jobtype"] == "compute":
            for model in self.config["general"]["models"]:
                if "nproc" in self.config[model]:
                    tasks += self.config[model]["nproc"]
                elif "nproca" in self.config[model] and "nprocb" in self.config[model]:
                    tasks += self.config[model]["nproca"] * self.config[model]["nprocb"]
        elif self.config["general"]["jobtype"] == "post":
            tasks = 1
        return tasks

    def get_environment(self):
        environment = []
        import esm_environment
        env = esm_environment.environment_infos("runtime", self.config)
        return env.commands

    def get_run_commands(self):
        commands = []
        batch_system = self.config["computer"]
        if "execution_command" in batch_system:
            commands.append("time " + batch_system["execution_command"] + " &")
        return commands

    def get_submit_command(self, sadfilename):
        commands = []
        batch_system = self.config["computer"]
        if "submit" in batch_system:
            commands.append("cd " + self.config["general"]["thisrun_scripts_dir"] + "; " + batch_system["submit"] + " " +sadfilename)
        return commands








    def submit(self):
        six.print_("\n", 40 * "+ ")
        print ("Submitting sad jobscript to batch system...")
        for command in self.submit_command:
            print (command)
        six.print_("\n", 40 * "+ ")
        for command in self.submit_command:
            os.system(command)
        # PG: Why is there an exit here when there is also a end_it_all after the submit?
        #sys.exit()




    #########################################################################################

    def _initialize_components(self):
        components = []
        for component in self.config["general"]["valid_model_names"]:
            components.append(
                SimulationComponent(self.config["general"], self.config[component])
            )
        self.components = components

    def _create_toplevel_marker_file(self):
        if not os.path.isfile(self.config['thisrun_']):
            with open(".top_of_exp_tree") as f:
                f.write("Top of experiment: "+self.config['general']['expid'])

    def _create_folders(self, config, filetypes):
        for filetype in filetypes:
            if not filetype == "ignore":
                if not os.path.exists(config["experiment_" + filetype + "_dir"]):
                    os.makedirs(config["experiment_" + filetype + "_dir"])
                if not os.path.exists(config["thisrun_" + filetype + "_dir"]):
                    os.makedirs(config["thisrun_" + filetype + "_dir"])

    def _dump_final_yaml(self):
        with open(
            self.experiment_config_dir
            + "/"
            + self.config["general"]["expid"]
            + "_preconfig.yaml",
            "w",
        ) as config_file:
            yaml.dump(self.config, config_file)

    def _show_simulation_info(self):
        six.print_(80 * "=")
        six.print_("STARTING SIMULATION JOB!")
        six.print_("Experiment ID = %s" % self.config["general"]["expid"])
        six.print_("Setup = %s" % self.config["general"]["setup_name"])
        six.print_("This setup consists of:")
        for model in self.config["general"]["valid_model_names"]:
            six.print_("- %s" % model)
        six.print_("You are using the Python version.")


    def _write_finalized_config(self):
        #pp.pprint(self.config)
        with open(
            self.config["general"]["thisrun_config_dir"]
            + "/"
            + self.config["general"]["expid"]
            + "_finished_config.yaml",
            "w",
        ) as config_file:
            yaml.dump(self.config, config_file)

    def _create_component_folders(self):
        for component in self.components:
            #component.general_config = self.config["general"]
            self._create_folders(component.config, self.all_model_filetypes)

    def _copy_preliminary_files_from_experiment_to_thisrun(self):
        filelist = [
            (
                "scripts",
                self.config["general"]["expid"]
                + "_"
                + self.config["general"]["setup_name"]
                + ".date",
                "copy",
            )
        ]
        for filetype, filename, copy_or_link in filelist:
            source = self.config["general"]["experiment_" + filetype + "_dir"]
            dest = self.config["general"]["thisrun_" + filetype + "_dir"]
            if copy_or_link == "copy":
                method = shutil.copy2
            elif copy_or_link == "link":
                method = os.symlink
            if os.path.isfile(source + "/" + filename):
                method(source + "/" + filename, dest + "/" + filename)
        this_script = self.config["general"]["scriptname"]
        shutil.copy2("./" + this_script, self.config["general"]["thisrun_scripts_dir"])

        for additional_file in self.config["general"]["additional_files"]:
            shutil.copy2(additional_file, self.config["general"]["thisrun_scripts_dir"])


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
            for model in self.config["general"]["models"]:
                self.config[model]["leapyear"] = self.config["general"]["leapyear"]
        else:
            for model in self.config["general"]["models"]:
                if "leapyear" in self.config[model]:
                    for other_model in self.config["general"]["models"]:
                        if "leapyear" in self.config[other_model]:
                            if not self.config[other_model]["leapyear"] == self.config[model]["leapyear"]:
                                print ("Models " + model + " and " + other_model + " do not agree on leapyear. Stopping.")
                                sys.exit(43)
                        else:    
                            self.config[other_model]["leapyear"] = self.config[model]["leapyear"]
                    self.config["general"]["leapyear"] = self.config[model]["leapyear"]
                    break

        if not "leapyear" in self.config["general"]:
            for model in self.config["general"]["models"]:
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
        self.last_run_datestamp = (
            config["general"]["last_start_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
            + "-"
            + config["general"]["prev_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
        )

    def set_prev_date(self):
        for model in self.config["general"]["models"]:
            if self.config[model]["time_step"] and not (type(self.config[model]["time_step"]) == str and "${" in self.config[model]["time_step"]):
                self.config[model]["prev_date"] = self.current_date - (0, 0, 0, 0, 0, int(self.config[model]["time_step"]))
            if self.config[model]["lresume"] == True and self.config["general"]["run_number"] == "1":
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


    def assemble_file_lists(self, filetypes):
        all_files_to_copy = []
        six.print_("\n" "- Generating file lists for this run...")
        for component in self.components:
            six.print_("-" * 80)
            six.print_("* %s" % component.config["model"], "\n")
            all_component_files, filetype_specific_dict = (
                component.filesystem_to_experiment(filetypes)
            )
            with open(
                component.config["thisrun_config_dir"]
                + "/"
                + self.config["general"]["expid"]
                + "_filelist_"
                + self.run_datestamp,
                "w",
            ) as flist:
                flist.write(
                    "These files are used for \nexperiment %s\ncomponent %s\ndate %s"
                    % (
                        self.config["general"]["expid"],
                        component.config["model"],
                        self.run_datestamp,
                    )
                )
                flist.write("\n")
                flist.write(80 * "-")
                for filetype in filetype_specific_dict:
                    flist.write("\n" + filetype.upper() + ":\n")
                    for source, exp_tree, exp_name, work_dir_name, subfolder  in filetype_specific_dict[
                        filetype
                    ]:
                        flist.write("\nSource: " + source)
                        flist.write("\nExp Tree: " + exp_tree + subfolder + exp_name)
                        flist.write("\nWork Dir: " + subfolder + work_dir_name)
                        flist.write("\n")
                    flist.write("\n")
                    flist.write(80 * "-")
            esm_parser.pprint_config(filetype_specific_dict)
            all_files_to_copy += all_component_files
        return all_files_to_copy


    def copy_files_to_thisrun(self, all_files_to_copy):
        six.print_("=" * 80, "\n")
        six.print_("PREPARING EXPERIMENT")
        # Copy files:
        six.print_("\n" "- File lists populated, proceeding with copy...")
        six.print_("- Note that you can see your file lists in the config folder")
        six.print_("- You will be informed about missing files")
        successful_files = []
        missing_files = []
        # TODO: Check if we are on login node or elsewhere for the progress
        # bar, it doesn't make sense on the compute nodes:
        flist = all_files_to_copy
        for ftuple in tqdm.tqdm(
            flist,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ):
            logging.debug(ftuple)
            (file_source, filedir_intermediate, filename_intermediate, filename_target, subfolder) = ftuple
            file_intermediate = filedir_intermediate + "/" + subfolder + filename_intermediate
            file_target = filedir_intermediate + "/" + subfolder + filename_target
            try:
                if not os.path.isdir(filedir_intermediate + "/" + subfolder): 
                    os.mkdir(filedir_intermediate + "/" + subfolder) 
                shutil.copy2(file_source, file_intermediate)
                if os.path.islink(file_target):
                    os.remove(file_target)
                if not os.path.isfile(file_target):
                    os.symlink(file_intermediate, file_target)
                successful_files.append(file_target)
            except IOError:
                missing_files.append(file_target)
        if missing_files:
            six.print_("--- WARNING: These files were missing:")
            for missing_file in missing_files:
                six.print_("- %s" % missing_file)


    def copy_files_from_work_to_thisrun(self, all_files_to_copy):
        six.print_("=" * 80, "\n")
        six.print_("COPYING STUFF FROM WORK TO THISRUN FOLDERS")
        # Copy files:
        successful_files = []
        missing_files = []
        # TODO: Check if we are on login node or elsewhere for the progress
        # bar, it doesn't make sense on the compute nodes:
        flist = all_files_to_copy
        for ftuple in tqdm.tqdm(
            flist,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ):
            logging.debug(ftuple)
            #(file_target, file_intermediate, file_source) = ftuple ### ???????
            (source, filedir_interm, filename_interm, filename_target, subfolder) = ftuple

            file_source = self.config["general"]["thisrun_work_dir"] + "/" + subfolder + filename_target
            file_intermediate = filedir_interm + "/" + subfolder + filename_target
            file_target = filedir_interm + "/" + subfolder + filename_interm

            #file_source = self.config["general"]["thisrun_work_dir"] + "/" + file_source.split("/", -1)[-1]
            #file_intermediate = file_intermediate.rsplit("/", 1)[0] + "/" + file_source.split("/", -1)[-1]
            #file_target = file_intermediate.rsplit("/", 1)[0] + "/" + file_target.split("/", -1)[-1]

            try:
                print(file_source + " " + file_intermediate + " " + file_target)
                if os.path.isfile(file_intermediate):
                    os.rename(file_intermediate, file_intermediate + "_" + self.last_run_datestamp)
                shutil.copy2(file_source, file_intermediate)
                if not file_target == file_intermediate:
                    if os.path.islink(file_target):
                        os.remove(file_target)
                    if os.path.isfile(file_target):
                        os.rename(file_target, file_target + "_" + self.last_run_datestamp)

                    os.symlink(file_intermediate, file_target)
                successful_files.append(file_target)

            except IOError:
                missing_files.append(file_target)
        if missing_files:
            six.print_("--- WARNING: These files were missing:")
            for missing_file in missing_files:
                six.print_("- %s" % missing_file)

    def modify_namelists(self):
        # Load and modify namelists:
        six.print_("\n" "- Setting up namelists for this run...")
        all_namelists = {}
        for component in self.components:
            six.print_("-" * 80)
            six.print_("* %s" % component.config["model"], "\n")
            component.nmls_load()
            component.nmls_remove()
            component.nmls_modify()
            component.nmls_finalize(all_namelists)
        six.print_(
            "\n" "- Namelists modified according to experiment specifications..."
        )
        for nml_name, nml in all_namelists.items():
            six.print_("Contents of ", nml_name, ":")
            nml.write(sys.stdout)
            six.print_("\n", 40 * "+ ")


    def modify_files(self):
        for model in self.config['general']['valid_model_names']:
            for filetype in self.all_model_filetypes: 
                #print(self.config[model].get(filetype+"_modifications"))
                if filetype == "restart":
                    nothing = "nothing"
                    #print(self.config[model].get(filetype+"_in_modifications"))


    def create_new_files(self, all_files_to_copy):
        for model in list(self.config):
            for filetype in self.all_filetypes:
                if "create_"+filetype in self.config[model]:
                    filenames = self.config[model]["create_"+filetype].keys()
                    for filename in filenames:
                        with open(self.config[model]["thisrun_" + filetype + "_dir"] + "/" +filename, "w") as createfile:
                            actionlist = self.config[model]["create_"+filetype][filename]
                            for action in actionlist:
                                if "<--append--" in action:
                                    appendtext = action.replace("<--append--", "")
                                    createfile.write(appendtext.strip() + "\n")
                        all_files_to_copy.append(
                            (
                                "",
                                self.config[model]["thisrun_" + filetype + "_dir"],
                                filename,
                                filename,
                                ""
                            )
                        )


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
                self.coupler = esm_coupler.esm_coupler(self.config, model)
                break
        self.coupler.add_files(self.config)   

    def prepare_coupler_files(self, all_files_to_copy):
        self.coupler.prepare(self.config, self.coupler_config_dir)
        coupler_filename="namcouple"  # needs to be set by function above
        all_files_to_copy.append(
            (
                "", 
                self.coupler_config_dir,  
                coupler_filename,
                coupler_filename,
                ""
            )
        )
        #print (coupler_config_dir + "/" + coupler_filename)



    def initialize_batch_system(self):
        from . import esm_batch_system
        self.batch = esm_batch_system.esm_batch_system(self.config, self.config["computer"]["batch_system"])

    def add_batch_hostfile(self, all_files_to_copy):
        from . import esm_batch_system
        #self.batch = esm_batch_system.esm_batch_system(self.config, self.config["computer"]["batch_system"])
        self.batch.calc_requirements(self.config)

        all_files_to_copy.append(
            (
                "", self.config["general"]["thisrun_scripts_dir"], 
                self.batch.bs.path.rsplit("/", 1)[-1],
                self.batch.bs.path.rsplit("/", 1)[-1],
                ""
            )
        )



    def copy_files_to_work(self, flist):
        successful_files = []
        missing_files = []
        # TODO: Check if we are on login node or elsewhere for the progress
        # bar, it doesn't make sense on the compute nodes:
        for ftuple in tqdm.tqdm(
            flist,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ):
            logging.debug(ftuple)
            (file_source, filedir_intermediate, filename_intermediate, filename_target, subfolder) = ftuple
            file_target = filedir_intermediate + "/" + subfolder + filename_target
            file_in_work = self.config["general"]["thisrun_work_dir"] + "/" + subfolder + filename_target
            try:
                if not os.path.isdir(self.config["general"]["thisrun_work_dir"] + "/" + subfolder):
                    os.mkdir(self.config["general"]["thisrun_work_dir"] + "/" + subfolder)
                shutil.copy2(file_target, file_in_work)
                successful_files.append(file_target)
            except IOError:
                missing_files.append(file_target)
        if missing_files:
            six.print_("--- WARNING: These files were missing:")
            for missing_file in missing_files:
                six.print_("- %s" % missing_file)
        #sys.exit()








    ################################# COMPONENT ###########################################



class SimulationComponent(object):
    def __init__(self, general, component_config):
        self.config = component_config
        self.general_config = general

    def __repr__(self):
        return "SimulationComponent: %s, v%s" % (self.config.get('model'), self.config.get('version'))

    def find_correct_source(self, file_source, year):
        if isinstance(file_source, dict):
            logging.debug(
                "Checking which file to use for this year: %s",
                year,
            )
            for fname, valid_years in six.iteritems(file_source):
                logging.debug("Checking %s", fname)
                min_year = float(valid_years.get("from", "-inf"))
                max_year = float(valid_years.get("to", "inf"))
                logging.debug("Valid from: %s", min_year)
                logging.debug("Valid to: %s", max_year)
                logging.debug(
                    "%s <= %s --> %s",
                    min_year,
                    year,
                    min_year <= year,
                )
                logging.debug(
                    "%s <= %s --> %s",
                    year,
                    max_year,
                    year <= max_year,
                )
                if (
                    min_year <= year
                    and year <= max_year
                ):
                    return fname
                else:
                    continue
        return file_source



    def filesystem_to_experiment(self, filetypes):
        import glob
        import copy
        all_files_to_process = []
        filetype_files_for_list = {}
        for filetype in filetypes:
        #for filetype in self.config["all_filetypes"]:
            filetype_files = []
            six.print_("- %s" % filetype)
            
            if filetype == "restart_in" and not self.config["lresume"]:
                six.print_("- restart files do not make sense for a cold start, skipping...")
                continue
            if filetype + "_sources" not in self.config:
                continue

            ####### start globbing here

            inverted_dict = {}
            if filetype + "_files" in self.config:
                for k, v in six.iteritems(self.config[filetype + "_files"]):
                    inverted_dict[v] = k

            sources_dict = copy.deepcopy(self.config[filetype + "_sources"])

            for file_descriptor, file_source in six.iteritems(
                sources_dict
            ):
                if "*" in file_source:
                    esm_parser.pprint_config(self.config)
                    file_category = None
                    subfolder = None
                    if filetype + "_files" in self.config:
                        if file_descriptor in self.config[filetype + "_files"]:
                            file_category = inverted_dict[file_descriptor]
                    if filetype + "_in_work" in self.config:
                        if file_descriptor in self.config[filetype + "_in_work"]:
                            subfolder = self.config[filetype + "_in_work"][file_descriptor].replace("*", "")
                            if not subfolder.endswith("/"):
                                subfolder = subfolder + "/"
                    all_file_sources = glob.glob(file_source)
                    
                    running_index = 0
                    for new_source in all_file_sources:
                        running_index += 1
                        new_descriptor = file_descriptor + "_" + str(running_index)
                        self.config[filetype + "_sources"][new_descriptor] = new_source
                        if file_category:
                            new_category = file_category + "_" + str(running_index)
                            self.config[filetype + "_files"][new_category] = new_descriptor
                        if subfolder:
                            new_in_work = subfolder + new_source.rsplit("/", 1)[-1]
                            self.config[filetype + "_in_work"][new_descriptor] = new_in_work

                    del self.config[filetype + "_sources"][file_descriptor]
                    if file_category:
                        del self.config[filetype + "_files"][file_category]
                    if subfolder:
                        del self.config[filetype + "_in_work"][file_descriptor]


           ######## end globbing stuff

            filedir_intermediate = self.config["thisrun_" + filetype + "_dir"]
            for file_descriptor, file_source in six.iteritems(
                self.config[filetype + "_sources"]
            ):
                if filetype == "restart_in":
                    file_source = self.config["experiment_restart_in_dir"] + "/" + os.path.basename(file_source)
                logging.debug(
                    "file_descriptor=%s, file_source=%s", file_descriptor, file_source
                )
                if filetype + "_files" in self.config:
                    if file_descriptor not in self.config[filetype + "_files"].values():
                        continue
                    else:
                        inverted_dict = {}
                        for k, v in six.iteritems(self.config[filetype + "_files"]):
                            inverted_dict[v] = k
                        file_category = inverted_dict[file_descriptor]
                else:
                    file_category = file_descriptor

                logging.debug(type(file_source))

                # should be generalized to all sorts of dates on day

                all_years = [self.general_config["current_date"].year]
                if (
                   filetype + "_additional_information" in self.config
                   and file_category in self.config[filetype + "_additional_information"]
                ):
                    if (
                       "need_timestep_before" in self.config[filetype + "_additional_information"][file_category]
                    ):
                        all_years.append(self.general_config["prev_date"].year)
                    if (
                       "need_timestep_after" in self.config[filetype + "_additional_information"][file_category]
                    ):
                        all_years.append(self.general_config["next_date"].year)
                    if (
                       "need_year_before" in self.config[filetype + "_additional_information"][file_category]
                    ):
                        all_years.append(self.general_config["current_date"].year - 1)
                    if (
                       "need_year_after" in self.config[filetype + "_additional_information"][file_category]
                    ):
                        all_years.append(self.general_config["next_date"].year + 1 )

                all_years = list(dict.fromkeys(all_years)) # removes duplicates

                if (
                    filetype + "_in_work" in self.config
                    and file_category in self.config[filetype + "_in_work"].keys()
                ):
                    target_name = self.config[filetype + "_in_work"][file_category]
                else:
                    target_name = os.path.basename(file_source)

                for year in all_years:

                    this_target_name=target_name.replace("@YEAR@", str(year))
                    source_name=self.find_correct_source(file_source, year)
                    file_target = (
                        filedir_intermediate + "/" + this_target_name
                    )

                    if "/" in this_target_name:
                        subfolder = this_target_name.rsplit("/", 1)[0] + "/" 
                    else:
                        subfolder = ""

                    filetype_files.append(
                        (
                            source_name,
                            filedir_intermediate,  os.path.basename(source_name),
                            this_target_name.rsplit("/", 1)[-1],
                            subfolder
                        )
                    )

            filetype_files_for_list[filetype] = filetype_files
            all_files_to_process += filetype_files
        return all_files_to_process, filetype_files_for_list

    def nmls_load(self):
        nmls = self.config.get("namelists", [])
        self.config["namelists"] = dict.fromkeys(nmls)
        for nml in nmls:
            if os.path.isfile( os.path.join(self.config["thisrun_config_dir"], nml)):
                logging.debug("Loading %s", nml)
                self.config["namelists"][nml] = f90nml.read(
                    os.path.join(self.config["thisrun_config_dir"], nml)
                )
            else:
                self.config["namelists"][nml] = f90nml.namelist.Namelist()

    def nmls_remove(self):
        namelist_changes = self.config.get("namelist_changes", {})
        namelist_removes = []
        esm_parser.pprint_config(namelist_changes)
        print(list(namelist_changes))
        for namelist in list(namelist_changes):
            changes = namelist_changes[namelist]
            esm_parser.pprint_config(changes)
            logging.debug("Determining remove entires for %s", namelist)
            logging.debug("All changes: %s", changes)
            for change_chapter in list(changes):
                change_entries = changes[change_chapter]
                for key in list(change_entries):
                    value = change_entries[key]
                    if value == "remove_from_namelist":
                        namelist_removes.append((namelist, change_chapter, key))
                        del namelist_changes[namelist][change_chapter][key]
        for remove in namelist_removes:
            namelist, change_chapter, key = remove
            logging.debug("Removing from %s: %s, %s", namelist, change_chapter, key)
            if key in self.config["namelists"][namelist][change_chapter]:
                del self.config["namelists"][namelist][change_chapter][key]

    def nmls_modify(self):
        namelist_changes = self.config.get("namelist_changes", {})
        for namelist, changes in six.iteritems(namelist_changes):
            self.config["namelists"][namelist].patch(changes)

    def nmls_finalize(self, all_nmls):
        for nml_name, nml_obj in six.iteritems(self.config.get("namelists", {})):
            with open(os.path.join(self.config["thisrun_config_dir"], nml_name), "w") as nml_file:
                nml_obj.write(nml_file)
            #if nml_name == "namelist.echam":
            #    pp.pprint(nml_obj)
            #    print(80*"*")
            #    pp.pprint(str(nml_obj))
            #    sys.exit(1)
            all_nmls[nml_name] = nml_obj  # PG or a string representation?

