"""
Documentation goes here
"""
#import pdb
#import os
#
#from loguru import logger
#
#import esm_tools
#import esm_parser
#
#
#import esm_rcfile
#
from . import config_initialization
from . import prepare
from . import workflow
#from . import batch_system, helpers, prepare, workflow
#from . import chunky_parts

class SimulationSetup(object):
    def __init__(self, command_line_config=None, user_config=None):

        if not command_line_config and not user_config:
            raise ValueError("SimulationSetup needs to be initialized with either command_line_config or user_config.")
        
        user_config = config_initialization.init_first_user_config(command_line_config, user_config)

        self.config = config_initialization.complete_config_from_user_config(user_config)

        self.config = config_initialization.save_command_line_config(self.config, command_line_config)

        self.config = prepare.run_job(self.config)

        self.config = workflow.assemble(self.config)



    def __call__(self, kill_after_submit=True):

        # call to observe here..
        pid = self.config["general"]["command_line_config"].get("pid", -666)
        if not pid == "-666":
            self.observe(*args, **kwargs)


        if self.config["general"]["jobtype"] == "compute":
            self.compute(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "inspect":
            self.inspect(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "tidy":
            self.tidy(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "viz":
            self.viz(*args, **kwargs)

        """
        Warning
        -------
            The date is changed during maybe_resubmit! Be careful where you put
            any calls that may depend on date information!

        Note
        ----
            This method is also responsible for calling the next compute job as
            well as the post processing job!
        """

        batch_system.maybe_resubmit(*args, **kwargs)
        if kill_after_submit:
            helpers.end_it_all(self.config)



###################################     OBSERVE      #############################################################

    def observe(self):
        
        from . import observe
       
        # not sure what this is doing really

        config = set_logfile_name(config, "monitoring_file")

        with open(
            config["general"]["logfile_path"],
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
        config = set_logfile_name(config)

        with open(
            config["general"]["logfile_path"],
            "w",
            buffering=1,
        ) as logfile:
            self.config["general"]["logfile"] = logfile
            self.config = inspect.run_job(self.config)



###################################     COMPUTE      #############################################################
    def compute(self):
        """
        All steps needed for a model computation.

        Parameters
        ----------
        kill_after_submit : bool
            Default ``True``. If set, the entire Python instance is killed with
            a ``sys.exit()`` as the very last after job submission.
        """
        from . import compute
        config = set_logfile_name(config)

        with open(
            config["general"]["logfile_path"],
            "w",
            buffering=1,
        ) as logfile:
            self.config["general"]["logfile"] = logfile
            self.config = compute.run_job(self.config)


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

    filename = (
            self.config["general"]["expid"] +
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

    if os.path.isfile(monitor_file_path):
        os.symlink(monitor_file_path, monitor_file_in_run)
    
    return config
