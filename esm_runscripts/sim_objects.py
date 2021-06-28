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

        if self.command_line_config.get("no_motd", False):
            self.config["general"]["no_motd"] = True
            
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

import sys
import questionary
from esm_calendar import Date, Calendar
class PrevRunInfo(dict):
    """
    A dictionary subclass to access information from the previous run. The object is
    created in the ``SimulationSetup`` class in ``self.config["prev_run"]``. The idea
    behind this class is that variables from the previous run can be called from the
    yaml files with the same syntax as one would do for the current run.

    The syntax is as follows:

    .. code-block:: yaml

       <your_var>: ${prev_run.<path>.<to>.<var>}

    For example, let's assume we want to access the ``time_step`` from the previous run
    of a `FESOM` simulation and store it in a variable called ``prev_time_step``:

    .. code-block:: yaml

       prev_time_step: ${prev_run.fesom.time_step}

    .. Note:: Only the single previous simulation loaded

    .. Warning:: Use this feature only when there is no other way of accessing the
       information needed. Note that, for example, dates of the previous run are
       already available in the current run, under variables such as
       ``last_start_date``, ``parent_start_date``, etc.
    """


    def __init__(self, config={}, prev_config=None, dir_to_solve=None):
        """
        Links the current ``config`` and ``prev_config`` to the object.

        Parameters
        ----------
        config : dict, esm_parser.ConfigSetup
            ConfigSetup object containing the information of the current simulation.
            **Note:** this variable needs to remain untouched inside this class because
            it is not a deepCopy, and it cannot be a deepCopy because this class needs
            this config to be updated on the go to properly work.
        prev_config : dict
            Dictionary that contains information loaded from the previous config
            file/run. If not provided, it means that the object is not nested into
            another PrevRunInfo object. When provided, it means it is nested.
        """
        self._config = config
        self._prev_config = prev_config
        # Set default value of the object while the config file has not been read
        self.__setitem__("NONE_YET", {})
        # prev_run_config_file and calendar date
        self._prcf = {}
        # List of components containning variables using the ``prev_run`` feature
        self.components_with_prev_run()
        # Counter for debuggin
        self._prev_config_count = 0
        self._dir_to_solve = dir_to_solve


    def components_with_prev_run(self):
        """
        Lists components containning variables using the ``prev_run`` feature.
        """
        # Search for components only if ``self._config`` is not empty
        if len(self._config) > 0:
            components = self._config.keys()
            # Make sure prev_run is not included and also that general is the last of
            # the components
            components = [
                component for component in components
                if component not in ["prev_run", "general"]
            ]
            components.append("general")
            # Loop through the components, and find which ones contain at least one
            # ``prev_run.`` value
            c_with_prev_run = []
            for component in components:
                if self.str_value_in_nested_dictionary(
                    "prev_run.", self._config[component]
                ):
                    c_with_prev_run.append(component)

            self._components = c_with_prev_run
        else:
            self._components = []


    def str_value_in_nested_dictionary(self, string_to_search, nested_dict):
        """
        Search recursively inside of a component for a ``string_to_search`` in the
        values of the nested dictionary ``nested_dict``.

        Parameters
        ----------
        string_to_search : str
            A string to be match in any values of the nested dictionary.
        nested_dict : dict
            Maybe, a nested dictionary which keys need to be recursively evaluated, to
            try to find in their values the ``string_to_search``. It really does not need
            to be a dictionary, as it stops the recursive search as soon as it is not a
            dictionary.

        Returns
        -------
        found : bool
            A boolean indicating if the ``string_to_search`` was found in any value.
        """
        found = False
        # If it's a dictionary, call this method recursively for each key
        if isinstance(nested_dict, dict):
            for key in nested_dict.keys():
                found = self.str_value_in_nested_dictionary(
                    string_to_search, nested_dict[key]
                )
                if found:
                    break
        # If it's a string check if ``string_to_searh`` is contained
        elif isinstance(nested_dict, str):
            if string_to_search in nested_dict:
                found = True

        return found


    def __getitem__(self, key):
        """
        Defines the special behaviour for accessing a ``key`` of the object (i.e. when
        the object is called such as ``<object>[key]``). If ``_prev_config`` is already
        loaded returns the value of the ``key``. Otherwise, it tries to load
        ``_prev_config`` and if not possible yet, returns a ``PrevRunInfo`` instance.
        """

        # If the previous config is not loaded yet (no file found), try to load it
        if not self._prev_config and len(self._config) > 0:
            self.prev_run_config()
        # If the previous config was loaded return the key
        if self._prev_config:
            if key == "NONE_YET":
                value = {}
            else:
                value = self._prev_config[key]
        # If the previous config is not loaded yet, return an instance of
        # ``PrevRunInfo``
        else:
            value = PrevRunInfo(prev_config=self._prev_config)

        return value


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
        # If the previous config is not loaded yet (no file foudn), try to load it
        if not self._prev_config and len(self._config) > 0:
            self.prev_run_config()
        # If the previous config was loaded, use get
        if self._prev_config:
            if key == "NONE_YET":
                value = {}
            else:
                value = self._prev_config.get(*args, **kwargs)
        # If the previous config is not loaded yet, return get of an empty dict
        else:
           value = {}.get(*args, **kwargs)

        return value


    def prev_run_config(self):
        """
        If all the necessary information is available, loads the previous config file
        for each component. The component loop is only run once per model, once loaded,
        the ``get`` and ``__get__`` methods will not call this method anymore.
        """
        # Logic for interactive questions about the config file
        fromdir = os.path.realpath(self._config["general"]["started_from"])
        scriptsdir = os.path.realpath(
            f"{self._config['general']['experiment_dir']}/scripts/"
        )
        # This is necessary to display the message only once, instead of twice
        self.warn = (
            fromdir == scriptsdir and
            self._config["general"].get("jobtype", "") == "compute"
        )
        # Check for interactive, or submitted from a computing node, to avoid
        # using ``input()`` or ``questionaries`` in the second case
        self.isinteractive = (
            self._config["general"].get("last_jobtype", "") == "command_line"
        )

        # Loop through components
        components = self._components
        for component in components:
            # Check if the ``prev_run_config_file`` was previously found already. If
            # not, try to find it
            if self._prcf.get(component):
                prev_run_config_file, calc_prev_date = self._prcf[component]
            else:
                prev_run_config_file, calc_prev_date = self.find_config(component)
                self._prcf[component] = (prev_run_config_file, calc_prev_date)

            # If the file exists, load the file content
            if os.path.isfile(prev_run_config_file):
                # TODO: delete the following lines
                self._prev_config_count += 1
                print(f"PREV CONFIG COUNT: {self._prev_config_count}")

                with open(prev_run_config_file, "r") as prev_file:
                    prev_config = yaml.load(prev_file, Loader=yaml.FullLoader)
                prev_config = prev_config["dictitems"]
                # In case a ``prev_run`` info exists inside the file, remove it to
                # avoid config files from getting huge (prev_run nested inside
                # prev_run...)
                if "prev_run" in prev_config:
                    del prev_config["prev_run"]
                # Check that the data really comes from the previous run
                prev_date = prev_config["general"]["end_date"]
                prev_date_stamp = Date(prev_date).format(
                    form=9, givenph=False, givenpm=False, givenps=False
                ) + "2" # TODO: remove the +2, for testing purposes only
                calc_prev_date_stamp = calc_prev_date.format(
                    form=9, givenph=False, givenpm=False, givenps=False
                )
                # Dates don't match
                if (
                    calc_prev_date_stamp != prev_date_stamp and
                    self.warn
                ):
                    esm_parser.user_note(
                        f"End date of the previous configuration file for '{component}'"
                        + " not coinciding:",
                        (
                            f"    File loaded: {prev_run_config_file}\n"
                            + f"    This previous date: {calc_prev_date}\n"
                            + f"    Previous date in prev config: {prev_date}\n"
                        )
                    )
                    # Only ask the user about a mismatch when manually restarted
                    if self.isinteractive:
                        no_input = True
                        while no_input:
                            answer = input(f"Do you want to proceed anyway?[y/n]: ")
                            if answer=="y":
                                no_input = False
                            elif answer=="n":
                                sys.exit(0)
                            else:
                                print("Incorrect answer.")

                # Load the component info into the self._prev_config dictionary
                if not self._prev_config:
                    self._prev_config = {}
                self._prev_config[component] = prev_config[component]
                # Load the general value of this component's prev_run
                # MA: this is potentially dangerous if all the following conditions are
                # met: 1) more than one component uses the prev_run feature, 2) both
                # components are branched off and come from different spinups, meaning
                # that they don't share the same general configuration, and 3) some of
                # the models need the general configuration from the previous run.
                if component != "general":
                    self._prev_config["general"] = prev_config["general"]


    def find_config(self, component):
        prev_run_config_file = ""
        # This experiment ``config_dir``
        config_dir = (
            self._config.get("general", {}).get("experiment_dir", "")
            + "/config/"
        )
        # Find ``lresume`` and ``run_number`` for this component
        lresume = self._config.get(component, {}).get("lresume", False)
        run_number = self._config.get("general", {}).get("run_number", 1)
        # It's run 1
        if run_number==1:
            # It's a branchoff experiment
            if lresume:
                # The user needs to provide the path to the config file of the previous
                # tun for branchoff experiments that use the prev_run feature
                user_prev_run_config_full_path = self._config[component].get(
                    "prev_run_config_file"
                )
                if not user_prev_run_config_full_path:
                    esm_parser.user_error(
                        "'prev_run_config_file' not defined",
                        "You are trying to run a branchoff experiment that uses the " +
                        f"'prev_run' functionality for '{component}' without " +
                        "specifying the path to the previous config file. " +
                        "Please, add to your runscript the following:\n\n" +
                        f"{component}:\n" +
                        "\tprev_run_config_file: <path_to_config_file>\n\n" +
                        "Note: the path to the config file from the parent " +
                        "is '<path_to_parent_exp>/configs/*_finished_*.yaml_<DATE>'."
                    )
                if "${" in user_prev_run_config_full_path:
                    user_prev_run_config_full_path = esm_parser.find_variable(
                        [component, "prev_run_config_file"],
                        user_prev_run_config_full_path,
                        self._config,
                        [],
                        True
                    )
                # Separate the base name from the path to the file
                user_prev_run_config_file = os.path.basename(
                    user_prev_run_config_full_path
                )
                config_dir = os.path.dirname(user_prev_run_config_full_path)
            # It's a cold start
            else:
                # There is no need of prev_run for cold starts. Do nothing
                return prev_run_config_file, ""

        # Check for errors
        if not os.path.isdir(config_dir):
            esm_parser.user_error("Config folder not existing", (
                f"The config folder {config_dir} does not exist. " +
                "The existance of this folder is a requirement for the use of the " +
                "prev_run feature."
            ))

        # Calculate previous date. This point is reached some times before it is
        # calculated in prepare.py, that's why we need the calculation here. It's only
        # use is to search for the config file time stamps.
        current_date = Date(self._config["general"]["current_date"])
        time_step = self._config[component].get("time_step", 1)
        try:
            time_step = int(time_step)
        except ValueError:
            time_step = 1
        prev_date = current_date - (
            0,
            0,
            0,
            0,
            0,
            time_step
        )

        # Calculate end date for the previous run
        prev_datestamp = prev_date.format(
            form=9, givenph=False, givenpm=False, givenps=False
        )

        # List all the config files in the config folder
        config_files = [
            cf for cf in os.listdir(config_dir) if "_finished_config.yaml" in cf
        ]
        # Select the ones ending with the correct timestamp
        potential_prev_configs = []
        for cf in config_files:
            if cf.endswith(prev_datestamp):
                potential_prev_configs.append(cf)

        # CASES FOR FINDING THE CONFIG FILE
        # ---------------------------------
        # Continuing run, not branch off, but no timestamped config files. Select the
        # one without timestamp
        if len(potential_prev_configs)==0 and run_number>1:
            prev_run_config_file = (
                self._config["general"]["expid"] +
                "_finished_config.yaml"
            )
        # Continuing run, not branch off, and one potential file. That's our file!
        elif len(potential_prev_configs)==1 and run_number>1:
            prev_run_config_file = potential_prev_configs[0]
        # Continuing run, too many possibilities, if interactive, ask the user,
        # otherwise, crash the simulation
        elif len(potential_prev_configs)>0 and run_number > 1:
            if self.warn:
                if self.isinteractive:
                    text = (
                        "Using the 'prev_run' feature several valid config files were"
                        + f" found for the component '{component}'."
                    )
                    prev_run_config_file = self.ask_about_files(
                        potential_prev_configs, component, config_dir, text
                    )
                else:
                    esm_parser.user_error(
                        "Too many possible config files",
                        "There is more than one config file with the same final date " +
                        "as the one required for the continuation of this experiment." +
                        " Please, resubmit the simulation, then you'll be ask about " +
                        "which file you'd like to use. This error comes from the " +
                        f"PrevRunInfo class, for the '{component}' component."
                    )
            else:
                # If started by the user in a directory different than the experiment
                # directory, scripts, then load the first option. The resubmission will
                # ask about which file really use
                prev_run_config_file = potential_prev_configs[0]
        # Branch off, load what the user specifies in the runscript
        elif run_number==1:
            prev_run_config_file = user_prev_run_config_file

        return f"{config_dir}/{prev_run_config_file}", prev_date


    def ask_about_files(self, potential_prev_configs, component, config_dir, text):
        questionary.print(100*"=")
        questionary.print(text)
        questionary.print(100*"=")

        user_confirmed = False
        while not user_confirmed:
            response = questionary.select(
                f"Which one do you want to use?",
                choices = (
                    potential_prev_configs
                    + ["[Quit] None of the files, stop simulation now"]
                )).ask()  # returns value of selection
            if "[Quit]" in response:
                if questionary.confirm("Are you sure?").ask():
                    sys.exit(0)
            user_confirmed = questionary.confirm("Are you sure?").ask()
        return response
