"""
Class to hold compute jobs and recipe steps
"""
import os
import shutil
import sys

import esm_rcfile
import six
import yaml

from .jobclass import jobclass
from .namelists import Namelist

#####################################################################
#                                   compute jobs                    #
#####################################################################


class compute(jobclass):
    def __init__(self, config):

        self.relevant_files = ["bin", "config", "forcing", "input", "restart_in"]
        self.all_files_to_copy = self.assemble_file_lists(config, self.relevant_files)
        # Check for a user defined compute recipe in the setup section of the
        # general section. If nothing is found, recipe_steps should evaluate to
        # None and the default is used
        try:
            setup_name = config["general"]["setup_name"]
            recipe_steps = config.get(setup_name, {}).get("compute_recipe") or config["general"].get("compute_recipe")
        except KeyError:
            print("Your configuration is incorrect, and should include headings for %s as well as general!" % setup_name)
            sys.exit(1)
        super(compute, self).__init__("compute", recipe_steps=recipe_steps)
        config["general"]["jobclass"] = self

    @staticmethod
    def add_batch_hostfile(config):

        self = config["general"]["jobclass"]
        config["general"]["batch"].calc_requirements(config)

        self.all_files_to_copy.append(
            (
                "",
                config["general"]["thisrun_scripts_dir"],
                config["general"]["batch"].bs.path.rsplit("/", 1)[-1],
                config["general"]["batch"].bs.path.rsplit("/", 1)[-1],
                "",
            )
        )
        return config

    @staticmethod
    def prepare_coupler_files(config):
        self = config["general"]["jobclass"]
        if config["general"]["standalone"] is False:
            coupler_filename = config["general"]["coupler"].prepare(
                config, config["general"]["coupler_config_dir"]
            )
            self.all_files_to_copy.append(
                (
                    "",
                    config["general"]["coupler_config_dir"],
                    coupler_filename,
                    coupler_filename,
                    "",
                )
            )
        return config

    @staticmethod
    def create_new_files(config):
        self = config["general"]["jobclass"]
        for model in list(config):
            for filetype in config["general"]["all_filetypes"]:
                if "create_" + filetype in config[model]:
                    filenames = config[model]["create_" + filetype].keys()
                    for filename in filenames:
                        with open(
                            config[model]["thisrun_" + filetype + "_dir"]
                            + "/"
                            + filename,
                            "w",
                        ) as createfile:
                            actionlist = config[model]["create_" + filetype][filename]
                            for action in actionlist:
                                if "<--append--" in action:
                                    appendtext = action.replace("<--append--", "")
                                    createfile.write(appendtext.strip() + "\n")
                        self.all_files_to_copy.append(
                            (
                                "",
                                config[model]["thisrun_" + filetype + "_dir"],
                                filename,
                                filename,
                                "",
                            )
                        )
        return config

    @staticmethod
    def modify_files(config):
        # for model in config:
        #     for filetype in config["general"]["all_model_filetypes"]:
        #         if filetype == "restart":
        #             nothing = "nothing"
        return config

    @staticmethod
    def modify_namelists(config):

        # Load and modify namelists:
        six.print_("\n" "- Setting up namelists for this run...")
        for model in config["general"]["valid_model_names"]:
            six.print_("-" * 80)
            six.print_("* %s" % config[model]["model"], "\n")
            config[model] = Namelist.nmls_load(config[model])
            config[model] = Namelist.nmls_remove(config[model])
            config[model] = Namelist.nmls_modify(config[model])
            config[model] = Namelist.nmls_finalize(config[model])
            print("end of namelist section")
        return config

    def copy_files_to_thisrun(config):

        self = config["general"]["jobclass"]
        six.print_("=" * 80, "\n")
        six.print_("PREPARING EXPERIMENT")
        # Copy files:
        six.print_("\n" "- File lists populated, proceeding with copy...")
        six.print_("- Note that you can see your file lists in the config folder")
        six.print_("- You will be informed about missing files")

        compute.print_used_files(config)

        config = compute.copy_files(
            config, self.all_files_to_copy, source="init", target="thisrun"
        )
        return config

    def copy_files_to_work(config):

        self = config["general"]["jobclass"]
        six.print_("=" * 80, "\n")
        six.print_("PREPARING WORK FOLDER")
        config = compute.copy_files(
            config, self.all_files_to_copy, source="thisrun", target="work"
        )
        return config

    @staticmethod
    def _create_folders(config, filetypes):

        for filetype in filetypes:
            if not filetype == "ignore":
                if not os.path.exists(config["experiment_" + filetype + "_dir"]):
                    os.makedirs(config["experiment_" + filetype + "_dir"])
                if not os.path.exists(config["thisrun_" + filetype + "_dir"]):
                    os.makedirs(config["thisrun_" + filetype + "_dir"])

    @staticmethod
    def _create_setup_folders(config):
        compute._create_folders(config["general"], config["general"]["all_filetypes"])
        return config

    @staticmethod
    def _create_component_folders(config):
        for component in config["general"]["valid_model_names"]:
            compute._create_folders(
                config[component], config["general"]["all_model_filetypes"]
            )
        return config

    @staticmethod
    def initialize_experiment_logfile(config):
        """
        Initializes the log file for the entire experiment.

        Creates a file ``${BASE_DIR}/${EXPID}/log/${EXPID}_${setup_name}.log``
        to keep track of start/stop times, job id numbers, and so on. Use the
        function ``write_to_log`` to put information in this file afterwards.

        The user can specify ``experiment_log_file`` under the ``general``
        section of the configuration to override the default name. Timestamps
        for each message are given by the section
        ``experiment_log_file_dateformat``, or defaults to ``Tue Mar 17
        09:36:38 2020``, i.e. ``"%c"``. Please use ``stftime`` compatable
        formats, as described here: https://strftime.org

        Parameters
        ----------
        dict :
            The experiment configuration

        Return
        ------
        dict :
            As per convention for the plug-in system; this gives back the
            entire config.

        Attention
        ---------
            Calling this has some filesystem side effects. If the run number in
            the general configuration is set to 1, and a file exists for
            ``general.exp_log_file``; this file is removed; and re-initialized.
        """

        if config["general"]["run_number"] == 1:
            if os.path.isfile(config["general"]["experiment_log_file"]):
                os.remove(config["general"]["experiment_log_file"])
            compute.write_to_log(
                config,
                ["# Beginning of Experiment " + config["general"]["expid"]],
                message_sep="",
            )

        compute.write_to_log(
            config,
            [
                str(config["general"]["jobtype"]),
                str(config["general"]["run_number"]),
                str(config["general"]["current_date"]),
                str(config["general"]["jobid"]),
                "- submitted",
            ],
        )
        return config

    @staticmethod
    def _write_finalized_config(config):

        with open(
            config["general"]["thisrun_config_dir"]
            + "/"
            + config["general"]["expid"]
            + "_finished_config.yaml",
            "w",
        ) as config_file:
            yaml.dump(config, config_file)
        return config

    @staticmethod
    def copy_tools_to_thisrun(config):

        gconfig = config["general"]

        fromdir = os.path.realpath(gconfig["started_from"])
        scriptsdir = os.path.realpath(gconfig["experiment_scripts_dir"])

        tools_dir = scriptsdir + "/esm_tools/functions"
        namelists_dir = scriptsdir + "/esm_tools/namelists"

        print("Started from :", fromdir)
        print("Scripts Dir : ", scriptsdir)

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
            print("Started from the experiment folder, continuing...")
            return config
        else:
            if not fromdir == scriptsdir:
                print("Not started from experiment folder, restarting...")
            else:
                print("Tools were updated, restarting...")

            if not os.path.isfile(scriptsdir + "/" + gconfig["scriptname"]):
                oldscript = fromdir + "/" + gconfig["scriptname"]
                print(oldscript)
                shutil.copy2(oldscript, scriptsdir)

            for tfile in gconfig["additional_files"]:
                if not os.path.isfile(scriptsdir + "/" + tfile):
                    shutil.copy2(fromdir + "/" + tfile, scriptsdir)

            restart_command = (
                "cd "
                + scriptsdir
                + "; "
                + "esm_runscripts "
                + gconfig["original_command"].replace("-U", "")
            )
            print(restart_command)
            os.system(restart_command)

            gconfig["profile"] = False
            compute.end_it_all(config, silent=True)

    @staticmethod
    def _copy_preliminary_files_from_experiment_to_thisrun(config):

        filelist = [
            (
                "scripts",
                config["general"]["expid"]
                + "_"
                + config["general"]["setup_name"]
                + ".date",
                "copy",
            )
        ]
        for filetype, filename, copy_or_link in filelist:
            source = config["general"]["experiment_" + filetype + "_dir"]
            dest = config["general"]["thisrun_" + filetype + "_dir"]
            if copy_or_link == "copy":
                method = shutil.copy2
            elif copy_or_link == "link":
                method = os.symlink
            if os.path.isfile(source + "/" + filename):
                method(source + "/" + filename, dest + "/" + filename)
        this_script = config["general"]["scriptname"]
        shutil.copy2("./" + this_script, config["general"]["thisrun_scripts_dir"])

        for additional_file in config["general"]["additional_files"]:
            shutil.copy2(additional_file, config["general"]["thisrun_scripts_dir"])
        return config

    @staticmethod
    def _show_simulation_info(config):

        six.print_(80 * "=")
        six.print_("STARTING SIMULATION JOB!")
        six.print_("Experiment ID = %s" % config["general"]["expid"])
        six.print_("Setup = %s" % config["general"]["setup_name"])
        six.print_("This setup consists of:")
        for model in config["general"]["valid_model_names"]:
            six.print_("- %s" % model)
        six.print_("You are using the Python version.")
        return config
