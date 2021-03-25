"""
Documentation goes here
"""
import sys
import os

from . import config_initialization
from . import prepare
from . import workflow
from . import batch_system

import esm_parser

class SimulationSetup(object):
    def __init__(self, command_line_config=None, user_config=None):

        if not command_line_config and not user_config:
            raise ValueError("SimulationSetup needs to be initialized with either command_line_config or user_config.")
        
        user_config = config_initialization.init_first_user_config(command_line_config, user_config)

        self.config = config_initialization.complete_config_from_user_config(user_config)

        self.config = config_initialization.save_command_line_config(self.config, command_line_config)

        self.config = prepare.run_job(self.config)

        self.config = workflow.assemble(self.config)

        #esm_parser.pprint_config(self.config)
        #sys.exit(0)



    def __call__(self, kill_after_submit=True):

        #self.pseudocall(kill_after_submit)
        # call to observe here..

        log_stuff = False
        if os.path.isdir(os.path.dirname(self.config["general"]["experiment_log_file"])):
            log_stuff = True

        org_jobtype = str(self.config["general"]["jobtype"])

        if log_stuff:
            helpers.write_to_log(
                config,
                [
                    org_jobtype,
                    str(self.config["general"]["run_number"]),
                    str(self.config["general"]["current_date"]),
                    str(self.config["general"]["jobid"]),
                    "- start",
                ],
            )
        
        if self.config["general"]["jobtype"] == "prepcompute":
            self.prepcompute()
        elif self.config["general"]["jobtype"] == "inspect":
            #esm_parser.pprint_config(self.config)
            self.inspect()
            helpers.end_it_all(self.config)
        elif self.config["general"]["jobtype"] == "tidy":
            self.tidy()
        elif self.config["general"]["jobtype"] == "viz":
            self.viz()
        elif self.config["general"]["jobtype"].startswith("observe"): 
            pid = self.config["general"]["command_line_config"].get("pid", -666)
            if not pid == -666:
                self.observe()

            self.config["general"]["jobtype"] = self.config["general"]["jobtype"].replace("observe_", "")
            # that last line is necessary so that maybe_resubmit knows which 
            # cluster to look up in the workflow


        batch_system.maybe_resubmit(self.config)
        
        if os.path.isdir(os.path.dirname(self.config["general"]["experiment_log_file"])):
            log_stuff = True

        if log_stuff:
            helpers.write_to_log(
                self.config,
                [
                    org_jobtype,
                    str(self.config["general"]["run_number"]),
                    str(self.config["general"]["current_date"]),
                    str(self.config["general"]["jobid"]),
                    "- done",
                ],
            )
        
            if kill_after_submit:
                helpers.end_it_all(self.config)



###################################     OBSERVE      #############################################################

    def observe(self):
        
        from . import observe
       
        # not sure what this is doing really

        self.config = set_logfile_name(self.config, "monitoring_file")

        with open(
            self.config["general"]["logfile_path"],
            "w",
            buffering=1,
        ) as logfile:
            self.config["general"]["logfile"] = logfile
            self.config = observe.run_job(self.config)


###################################     TIDY      #############################################################
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
        folder.
        """
        from . import tidy
        config = set_logfile_name(config)

        with open(
            config["general"]["logfile_path"],
            "w",
            buffering=1,
        ) as logfile:
            self.config["general"]["logfile"] = logfile
            self.config = tidy.run_job(self.config)


###################################     INSPECT      #############################################################
    def inspect(self):
        from . import inspect
        print(f"Inspecting {self.config['general']['experiment_dir']}")
        self.config = set_logfile_name(self.config)

        #with open(
        #    self.config["general"]["logfile_path"],
        #    "w",
        #    buffering=1,
        #) as logfile:
        #    self.config["general"]["logfile"] = logfile
        self.config = inspect.run_job(self.config)



###################################     PREPCOMPUTE      #############################################################
    def prepcompute(self):
        """
        All steps needed for a model computation.

        Parameters
        ----------
        kill_after_submit : bool
            Default ``True``. If set, the entire Python instance is killed with
            a ``sys.exit()`` as the very last after job submission.
        """
        from . import prepcompute
        self.config = set_logfile_name(self.config)

        #with open(
        #    self.config["general"]["logfile_path"],
        #    "w",
        #    buffering=1,
        #) as logfile:
        #    self.config["general"]["logfile"] = logfile
        self.config = prepcompute.run_job(self.config)


###################################     VIZ     #############################################################

    def viz(self):
        """
        Starts the Viz job.

        Parameters
        ----------
        kill_after_submit: bool
            Default ``True``. If set, the entire Python instance is killed with ``sys.exit()``.
        """
        # NOTE(PG): Local import, not everyone will have viz yet...
        import esm_viz as viz
        config = set_logfile_name(config)

        with open(
            config["general"]["logfile_path"],
            "w",
            buffering=1,
        ) as logfile:
            self.config["general"]["logfile"] = logfile
            self.config = viz.run_job(self.config)




def set_logfile_name(config, jobtype = None):

    if not jobtype:
        jobtype = config["general"]["jobtype"]

    filename = (
            config["general"]["expid"] +
            "_" +
            jobtype +
            "_" +
            config["general"]["run_datestamp"] +
            ".log"
    )

    config["general"]["logfile_path"] = (
        config["general"]["experiment_scripts_dir"] +
        "/" +
        filename
    )

    config["general"]["logfile_path_in_run"] = (
        config["general"]["thisrun_scripts_dir"] +
        "/" +
        filename
    )

    if os.path.isfile(config["general"]["logfile_path"]):
        os.symlink(
            config["general"]["logfile_path"],
            config["general"]["logfile_path_in_run"]
            )

    
    return config
