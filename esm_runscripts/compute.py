#####################################################################
#                                   compute jobs                    #
#####################################################################


def run_job(config):
    from .helpers import evaluate
    config["general"]["relevant_filetypes"] = ["bin", "config", "forcing", "input", "restart_in"]
    config = evaluate(config, "compute", "compute_recipe")
    return config


def all_files_to_copy_append(config, model, filetype, categ, file_source, file_interm, file_target):
    if file_source:
        if not filetype + "_sources" in config[model]:
            config[model][filetype + "_sources"] = {}
        config[model][filetype + "_sources"][categ] = file_source
    if file_interm:
        if not filetype + "_intermediate" in config[model]:
            config[model][filetype + "_intermediate"] = {}
        config[model][filetype + "_intermediate"][categ] = file_interm
    if file_target:
        if not filetype + "_targets" in config[model]:
            config[model][filetype + "_targets"] = {}
        config[model][filetype + "_targets"][categ] = file_target

    if filetype + "_files" in config[model]:
        config[model][filetype + "_files"][categ] = categ

    return config


def add_batch_hostfile(config):
    config["general"]["batch"].calc_requirements(config)
    setup_name = config["general"]["setup_name"]

    config = all_files_to_copy_append(
            config,
            setup_name,
            "config",
            "batchhostfile",
            config["general"]["batch"].bs.path,
            None,
            None,
        )
    return config


def prepare_coupler_files(config):
    if config["general"]["standalone"] is False:
        coupler_filename = config["general"]["coupler"].prepare(
            config, config["general"]["coupler_config_dir"]
        )
        coupler_name = config["general"]["coupler"].name 
        all_files_to_copy_append(
                config,
                coupler_name,
                "config",
                "namcouple",
                config["general"]["coupler_config_dir"] + "/" + coupler_filename,
                None,
                None,
            )
    return config


def create_new_files(config):
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
                    all_files_to_copy_append(
                            config,
                            model,
                            filetype,
                            filename,
                            config[model]["thisrun_" + filetype + "_dir"] + "/" + filename,
                            None,
                            None,
                        )
    return config


def modify_files(config):
    # for model in config:
    #     for filetype in config["general"]["all_model_filetypes"]:
    #         if filetype == "restart":
    #             nothing = "nothing"
    return config


def modify_namelists(config):
    from .namelists import Namelist
    # Load and modify namelists:

    if config["general"]["verbose"]:
        import six
        six.print_("\n" "- Setting up namelists for this run...")
        for model in config["general"]["valid_model_names"]:
            six.print_("-" * 80)
            six.print_("* %s" % config[model]["model"], "\n")
    
    for model in config["general"]["valid_model_names"]:
        config[model] = Namelist.nmls_load(config[model])
        config[model] = Namelist.nmls_remove(config[model])
        if model == "echam":
            config = Namelist.apply_echam_disturbance(config)
        config[model] = Namelist.nmls_modify(config[model])
        config[model] = Namelist.nmls_finalize(config[model], config["general"]["verbose"])
     
    if config["general"]["verbose"]:
        print("end of namelist section")
    return config


def copy_files_to_thisrun(config):
    from .filelists import log_used_files, copy_files
    if config["general"]["verbose"]:
        import six
        six.print_("=" * 80, "\n")
        six.print_("PREPARING EXPERIMENT")
        # Copy files:
        six.print_("\n" "- File lists populated, proceeding with copy...")
        six.print_("- Note that you can see your file lists in the config folder")
        six.print_("- You will be informed about missing files")

    log_used_files(config)

    config = copy_files(
        config, config["general"]["in_filetypes"], source="init", target="thisrun"
    )
    return config


def copy_files_to_work(config):
    from .filelists import copy_files
    if config["general"]["verbose"]:
        import six
        six.print_("=" * 80, "\n")
        six.print_("PREPARING WORK FOLDER")
    config = copy_files(
        config, config["general"]["in_filetypes"], source="thisrun", target="work"
    )
    return config


def _create_folders(config, filetypes):
    import os
    for filetype in filetypes:
        if not filetype == "ignore":
            if not filetype == "work":
                if not os.path.exists(config["experiment_" + filetype + "_dir"]):
                    os.makedirs(config["experiment_" + filetype + "_dir"])
            if not os.path.exists(config["thisrun_" + filetype + "_dir"]):
                os.makedirs(config["thisrun_" + filetype + "_dir"])


def _create_setup_folders(config):
    _create_folders(config["general"], config["general"]["all_filetypes"])
    return config


def _create_component_folders(config):
    for component in config["general"]["valid_model_names"]:
        _create_folders(
            config[component], config["general"]["all_model_filetypes"]
        )
    return config


def initialize_experiment_logfile(config):
    from .helpers import write_to_log
    import os
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
        write_to_log(
            config,
            ["# Beginning of Experiment " + config["general"]["expid"]],
            message_sep="",
        )
    write_to_log(
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


def _write_finalized_config(config):
    import yaml
    from esm_calendar import Date

    def date_representer(dumper, date):
       return dumper.represent_str("%s" % date.output())
    yaml.add_representer(Date, date_representer)
    with open(
        config["general"]["thisrun_config_dir"]
        + "/"
        + config["general"]["expid"]
        + "_finished_config.yaml",
        "w",
    ) as config_file:
        yaml.dump(config, config_file)
    return config


def copy_tools_to_thisrun(config):
    from .helpers import end_it_all
    import esm_rcfile
    import shutil
    import os
    gconfig = config["general"]

    fromdir = os.path.realpath(gconfig["started_from"])
    scriptsdir = os.path.realpath(gconfig["experiment_scripts_dir"])

    tools_dir = scriptsdir + "/esm_tools/functions"
    namelists_dir = scriptsdir + "/esm_tools/namelists"

    if config["general"]["verbose"]:
        print("Started from :", fromdir)
        print("Scripts Dir : ", scriptsdir)

    if os.path.isdir(tools_dir) and gconfig["update"]:
        shutil.rmtree(tools_dir, ignore_errors=True)
    if os.path.isdir(namelists_dir) and gconfig["update"]:
        shutil.rmtree(namelists_dir, ignore_errors=True)

    if not os.path.isdir(tools_dir):
        if config["general"]["verbose"]:
            print("Copying from: ", esm_rcfile.FUNCTION_PATH)
        shutil.copytree(esm_rcfile.FUNCTION_PATH, tools_dir)
    if not os.path.isdir(namelists_dir):
        shutil.copytree(esm_rcfile.get_rc_entry("NAMELIST_PATH"), namelists_dir)

    if (fromdir == scriptsdir) and not gconfig["update"]:
        if config["general"]["verbose"]:
            print("Started from the experiment folder, continuing...")
        return config
    else:
        if not fromdir == scriptsdir:
            if config["general"]["verbose"]:
                print("Not started from experiment folder, restarting...")
        else:
            print("Tools were updated, restarting...")

        if not os.path.isfile(scriptsdir + "/" + gconfig["scriptname"]):
            oldscript = fromdir + "/" + gconfig["scriptname"]
            if config["general"]["verbose"]:
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
        if config["general"]["verbose"]:
            print(restart_command)
        os.system(restart_command)

        gconfig["profile"] = False
        end_it_all(config)


def _copy_preliminary_files_from_experiment_to_thisrun(config):
    import shutil
    import os
    # I don't like this one bit. DB
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


def _show_simulation_info(config):
    import six
    six.print_()
    six.print_(80 * "=")
    six.print_("STARTING SIMULATION JOB!")
    six.print_("Experiment ID = %s" % config["general"]["expid"])
    six.print_("Setup = %s" % config["general"]["setup_name"])
    if "coupled_setup" in config["general"]:
        six.print_("This setup consists of:")
        for model in config["general"]["valid_model_names"]:
            six.print_("- %s" % model)
    six.print_("Experiment is installed in:")
    six.print_("       %s" % config["general"]["base_dir"] + "/" + config["general"]["expid"])
    six.print_(80 * "=")
    six.print_()
    return config
