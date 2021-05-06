"""
Documentation goes here
"""
import pdb
import os
import yaml

from loguru import logger

import esm_tools
import esm_parser
import esm_rcfile


from . import batch_system, compute, helpers, prepare, tidy


class SimulationSetup(object):
    def __init__(self, command_line_config=None, user_config=None):
        if not command_line_config and not user_config:
            raise ValueError("SimulationSetup needs to be initialized with either command_line_config or user_config.")
        if command_line_config:
            self.command_line_config = command_line_config
        else:
            self.command_line_config = {}

        if not user_config:
            user_config = self.get_user_config_from_command_line(command_line_config)
        if user_config["general"].get("debug_obj_init", False):
            pdb.set_trace()
        self.get_total_config_from_user_config(user_config)

        self.config["general"]["command_line_config"] = self.command_line_config
        if "verbose" not in self.config["general"]:
            self.config["general"]["verbose"] = False
        # read the prepare recipe
        self.config["general"]["reset_calendar_to_last"] = False
        if self.config["general"].get("inspect"):
            self.config["general"]["jobtype"] = "inspect"
            self.config["general"]["reset_calendar_to_last"] = True

        self.config["prev_run"] = PrevRunInfo(self.config)
        self.config = prepare.run_job(self.config)





    def __call__(self, *args, **kwargs):
        if self.config["general"]["jobtype"] == "compute":
            self.compute(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "inspect":
            self.inspect(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "tidy_and_resubmit":
            self.tidy(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "post":
            self.postprocess(*args, **kwargs)
        elif self.config["general"]["jobtype"] == "viz":
            self.viz(*args, **kwargs)
        else:
            print("Unknown jobtype specified! Goodbye...")
            helpers.end_it_all(self.config)


###################################     INSPECT      #############################################################
    def inspect(self):
        from . import inspect
        print(f"Inspecting {self.config['general']['experiment_dir']}")
        self.config = inspect.run_job(self.config)
        helpers.end_it_all(self.config)



###################################     COMPUTE      #############################################################
    def compute(self, kill_after_submit=True):
        """
        All steps needed for a model computation.

        Parameters
        ----------
        kill_after_submit : bool
            Default ``True``. If set, the entire Python instance is killed with
            a ``sys.exit()`` as the very last after job submission.
        """
        self.config = compute.run_job(self.config)

        if kill_after_submit:
            helpers.end_it_all(self.config)

###################################     VIZ     #############################################################

    def viz(self, kill_after_submit=True):
        """
        Starts the Viz job.

        Parameters
        ----------
        kill_after_submit: bool
            Default ``True``. If set, the entire Python instance is killed with ``sys.exit()``.
        """
        # NOTE(PG): Local import, not everyone will have viz yet...
        import esm_viz as viz
        self.config = viz.run_job(self.config)
        if kill_after_submit:
            helpers.end_it_all(self.config)


    ##########################    ASSEMBLE ALL THE INFORMATION  ##############################

    def get_user_config_from_command_line(self, command_line_config):
        try:
            user_config = esm_parser.initialize_from_yaml(command_line_config["scriptname"])
            if "additional_files" not in user_config["general"]:
                user_config["general"]["additional_files"] = []
        except esm_parser.EsmConfigFileError as error:
            raise error
        except:
            user_config = esm_parser.initialize_from_shell_script(command_line_config["scriptname"])

        # NOTE(PG): I really really don't like this. But I also don't want to
        # re-introduce black/white lists
        #
        # User config wins over command line:
        # -----------------------------------
        # Update all **except** for use_venv if it was supplied in the
        # runscript:
        deupdate_use_venv = False
        if "use_venv" in user_config["general"]:
            user_use_venv = user_config['general']["use_venv"]
            deupdate_use_venv = True
        user_config["general"].update(command_line_config)
        if deupdate_use_venv:
            user_config["general"]["use_venv"] = user_use_venv
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

        self.config = self.add_esm_runscripts_defaults_to_config(self.config)

        self.config["computer"]["jobtype"] = self.config["general"]["jobtype"]
        self.config["general"]["experiment_dir"] = self.config["general"]["base_dir"] + "/" + self.config["general"]["expid"]



    def distribute_per_model_defaults(self, config):
        default_config = config["general"]["defaults.yaml"]
        if "per_model_defaults" in default_config:
            for model in config["general"]["valid_model_names"]:
                config[model] = esm_parser.new_deep_update(config[model], default_config["per_model_defaults"])
        return config


    def add_esm_runscripts_defaults_to_config(self, config):
        FUNCTION_PATH = esm_rcfile.EsmToolsDir("FUNCTION_PATH")
        path_to_file = FUNCTION_PATH + "/esm_software/esm_runscripts/defaults.yaml"
        default_config = esm_parser.yaml_file_to_dict(path_to_file)
        config["general"]["defaults.yaml"] = default_config
        config = self.distribute_per_model_defaults(config)
        return config






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


        monitor_file_path = (
            self.config["general"]["experiment_scripts_dir"] +
            "/monitoring_file_" +
            self.config["general"]["run_datestamp"] +
            ".out"
        )
        monitor_file_in_run = self.config["general"]["thisrun_scripts_dir"] + "/monitoring_file.out"
        exp_log_path = (
            self.config["general"]["experiment_scripts_dir"] +
            self.config["general"]["expid"] +
            "_compute_" +
            self.config["general"]["run_datestamp"] +
            "_" +
            str(self.config["general"]["jobid"]) +
            ".log"
        )
        log_in_run = (
            self.config["general"]["thisrun_scripts_dir"] +
            self.config["general"]["expid"] +
            "_compute_" +
            str(self.config["general"]["jobid"]) +
            ".log"
        )


        with open(
            monitor_file_path,
            "w",
            buffering=1,
        ) as monitor_file:

            self.config["general"]["monitor_file"] = monitor_file
            if os.path.isfile(monitor_file_path):
                # os.symlink(monitor_file_path, monitor_file_in_run)
                helpers.symlink(monitor_file_path, monitor_file_in_run, overwrite=True)
            if os.path.isfile(exp_log_path):
                # os.symlink(exp_log_path, log_in_run)
                helpers.symlink(exp_log_path, log_in_run, overwrite=True)
            self.config = tidy.run_job(self.config)

        helpers.end_it_all(self.config)



###############################################       POSTPROCESS ######################################

    def postprocess(self):
        """
        Calls post processing routines for this run.
        """
        with open(
            self.config["general"]["thisrun_scripts_dir"] +
            "/" +
            self.config["general"]["expid"] +
            "_post_" +
            self.config["general"]["run_datestamp"] +
            "_" +
            str(self.config['general']['jobid']) +
            ".log",
            "w",
            buffering=1,
        ) as post_file:

            self.config["general"]["post_file"] = post_file
            self.config = postprocess.run_job(self.config)


            #post_task_list = self._assemble_postprocess_tasks(post_file)
            #self.config["general"]["post_task_list"] = post_task_list
            batch_system.write_simple_runscript(self.config)
            self.config = batch_system.submit(self.config)


class PrevRunInfo(dict):
    """
    A dictionary subclass to access information from the previous run. The object is
    created in the ``SimulationSetup`` class in ``self.config["prev_run"]``. The idea
    behind this class is that variables from the previous run can be called from the
    yaml files with the same syntax as one would do for the current run.

    The syntax is as follows:

    .. code-block:: yaml

       <your_var>: ${prev_run.<path>.<to>.<var>}

    For example, let's assume we want to access the `time_step` from the previous run
    of a `FESOM` simulation and store it in a variable called `prev_time_step`:

    .. code-block:: yaml

       prev_time_step: ${prev_run.fesom.time_step}

    .. Note:: Only the single previous simulation loaded

    .. Warning:: Use this feature only when there is no other way of accessing the
       information needed. Note that, for example, dates of the previous run are
       already available in the current run, under variables such as
       ``last_start_date``, ``parent_start_date``, etc.
    """


    def __init__(self, config={}, prev_config=None):
        """
        Links the current ``config`` and ``prev_config`` to the object.
        """
        self._last_run_datestamp = config.get("general", {}).get("last_run_datestamp")
        self._experiment_config_dir = config.get("general", {}).get("experiment_config_dir")
        self._expid = config.get("general", {}).get("expid")
        self._prev_config = prev_config
        self.__setitem__("NONE_YET", {})


    def __getitem__(self, key):
        """
        Defines the special behaviour for accessing a ``key`` of the object (i.e. when
        the object is called such as ``<object>[key]``). If ``_prev_config`` is already
        loaded returns the value of the ``key``. Otherwise, it tries to load
        ``_prev_config`` and if not possible yet, returns a ``PrevRunInfo`` instance.
        """
        # If the previous config is not loaded yet, try to load it
        if not self._prev_config:
            self.prev_run_config()
        # If the previous config was loaded return the key
        if self._prev_config:
            value = self._prev_config[key]
        # If the previous config is not loaded yet, return an instance of
        # ``PrevRunInfo``
        else:
            value = PrevRunInfo(prev_config=self._prev_config)

        #self.__setitem__(key, value)
        return value #super().__getitem__(key)


    def get(self, *args, **kwargs):
        """
        Defines the special behaviour for the ``get`` method of the object (i.e. when
        the object is called such as ``<object>.get(key, <value>)``). If
        ``_prev_config`` is already loaded returns the value of the ``key``. Otherwise,
        it tries to load it from ``_prev_config`` and if not possible yet, returns
        ``None`` if no second argument is defined for the ``get``, or it returns the
        second argument, just as a standard ``<dict>.get`` would do.
        """
        key = args[0]
        # If the previous config is not loaded yet, try to load it
        if not self._prev_config:
            self.prev_run_config()
        # If the previous config was loaded, use get
        if self._prev_config:
            value = self._prev_config.get(*args, **kwargs)
        # If the previous config is not loaded yet, return get of an empty dict
        else:
           value = {}.get(*args, **kwargs)

        return value


    def prev_run_config(self):
        """
        If the ``last_run_datestamp`` exists at this poing in the current ``config``,
        tries to load the previous config file into ``_prev_run_config``.
        """
        # If the config already includes the date stamp then load the previous config
        # file and return the corresponding value to the key
        if all([
            self._last_run_datestamp,
            self._experiment_config_dir,
            self._expid
        ]):
            # Build name of the file
            prev_run_config_file = (
                self._experiment_config_dir +
                self._expid +
                "_finished_config.yaml_" +
                self._last_run_datestamp
            )
            # If the file exists, load the file content
            if os.path.isfile(prev_run_config_file):
                with open(prev_run_config_file, "r") as prev_file:
                    prev_config = yaml.load(prev_file, Loader=yaml.FullLoader)
                self._prev_config = prev_config["dictitems"]
                # In case a ``prev_run`` info exists inside the file, remove it to
                # avoid config files from getting huge (prev_run nested inside
                # prev_run...)
                if "prev_run" in self._prev_config:
                    del self._prev_config["prev_run"]
                # Check that the data really comes from the previous run
                this_run_number = self._config["general"]["run_number"]
                prev_run_number = self._prev_config["general"]["run_number"]
                if this_run_number - 1 != prev_run_number:
                    esm_parser.user_error(
                        "Incorrect file loaded as previous configuration:",
                        (
                            f"    File loaded: {prev_run_config_file}\n"
                            + f"    This run number: {this_run_number}\n"
                            + f"    Previous run number: {prev_run_number}\n"
                        )
                    )
