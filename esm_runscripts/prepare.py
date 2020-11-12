from . import helpers


def run_job(config):
    helpers.evaluate(config, "prepare", "prepare_recipe")
    return config


def _read_date_file(config):
    import os
    import logging

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
            date, run_number = date_file.readline().strip().split()
            run_number = int(run_number)
        write_file = False
    else:
        logging.info("No date file found %s", date_file)
        logging.info("Initializing run_number=1 and date=18500101")
        date = config["general"].get("initial_date", "18500101")
        run_number = 1
        write_file = True
    config["general"]["run_number"] = run_number
    config["general"]["current_date"] = date
    logging.info("current_date = %s", date)
    logging.info("run_number = %s", run_number)
    return config


def check_model_lresume(config):
    import esm_parser

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

            if isinstance(user_lresume, str) and "${" in user_lresume:
                user_lresume = esm_parser.find_variable(
                    model, user_lresume, config, [], []
                )
            if type(user_lresume) == str:

                if user_lresume == "0" or user_lresume.upper() == "FALSE":
                    user_lresume = False
                elif user_lresume == "1" or user_lresume.upper() == "TRUE":
                    user_lresume = True
            elif isinstance(user_lresume, int):
                if user_lresume == 0:
                    user_lresume = False
                elif user_lresume == 1:
                    user_lresume = True
            config[model]["lresume"] = user_lresume
    for model in config["general"]["valid_model_names"]:
        # Check if lresume contains a variable which might be set in a different model, and resolve this case
        if (
            "lresume" in config[model]
            and isinstance(config[model]["lresume"], str)
            and "${" in config[model]["lresume"]
        ):
            lr = esm_parser.find_variable(
                model, config[model]["lresume"], config, [], []
            )
            config[model]["lresume"] = eval(lr)
    return config


def resolve_some_choose_blocks(config):
    from esm_parser import choose_blocks

    choose_blocks(config, blackdict=config._blackdict)
    return config


def _initialize_calendar(config):
    config = set_restart_chunk(config)
    config = set_leapyear(config)
    config = set_overall_calendar(config)
    config = set_most_dates(config)
    return config


def set_restart_chunk(config):
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
    config["general"]["nyear"] = nyear
    config["general"]["nmonth"] = nmonth
    config["general"]["nday"] = nday
    config["general"]["nhour"] = nhour
    config["general"]["nminute"] = nminute
    config["general"]["nsecond"] = nsecond
    return config


def set_leapyear(config):
    # make sure all models agree on leapyear
    if "leapyear" in config["general"]:
        for model in config["general"]["valid_model_names"]:
            config[model]["leapyear"] = config["general"]["leapyear"]
    else:
        for model in config["general"]["valid_model_names"]:
            if "leapyear" in config[model]:
                for other_model in config["general"]["valid_model_names"]:
                    if "leapyear" in config[other_model]:
                        if (
                            not config[other_model]["leapyear"]
                            == config[model]["leapyear"]
                        ):
                            print(
                                "Models "
                                + model
                                + " and "
                                + other_model
                                + " do not agree on leapyear. Stopping."
                            )
                            sys.exit(43)
                    else:
                        config[other_model]["leapyear"] = config[model]["leapyear"]
                config["general"]["leapyear"] = config[model]["leapyear"]
                break

    if not "leapyear" in config["general"]:
        for model in config["general"]["valid_model_names"]:
            config[model]["leapyear"] = True
        config["general"]["leapyear"] = True
    return config


def set_overall_calendar(config):
    from esm_calendar import Calendar

    # set the overall calendar
    if config["general"]["leapyear"]:
        config["general"]["calendar"] = Calendar(1)
    else:
        config["general"]["calendar"] = Calendar(0)
    return config


def set_most_dates(config):
    from esm_calendar import Calendar, Date

    calendar = config["general"]["calendar"]
    current_date = Date(config["general"]["current_date"], calendar)
    delta_date = (
        config["general"]["nyear"],
        config["general"]["nmonth"],
        config["general"]["nday"],
        config["general"]["nhour"],
        config["general"]["nminute"],
        config["general"]["nsecond"],
    )

    config["general"]["delta_date"] = delta_date
    config["general"]["current_date"] = current_date
    config["general"]["start_date"] = current_date
    config["general"]["initial_date"] = Date(
        config["general"]["initial_date"], calendar
    )
    config["general"]["final_date"] = Date(config["general"]["final_date"], calendar)
    config["general"]["prev_date"] = current_date - (0, 0, 1, 0, 0, 0)

    config["general"]["next_date"] = current_date.add(delta_date)
    config["general"]["last_start_date"] = current_date - delta_date
    config["general"]["end_date"] = config["general"]["next_date"] - (0, 0, 1, 0, 0, 0)

    config["general"]["runtime"] = (
        config["general"]["next_date"] - config["general"]["current_date"]
    )

    config["general"]["total_runtime"] = (
        config["general"]["next_date"] - config["general"]["initial_date"]
    )

    config["general"]["run_datestamp"] = (
        config["general"]["current_date"].format(
            form=9, givenph=False, givenpm=False, givenps=False
        )
        + "-"
        + config["general"]["end_date"].format(
            form=9, givenph=False, givenpm=False, givenps=False
        )
    )

    config["general"]["last_run_datestamp"] = (
        config["general"]["last_start_date"].format(
            form=9, givenph=False, givenpm=False, givenps=False
        )
        + "-"
        + config["general"]["prev_date"].format(
            form=9, givenph=False, givenpm=False, givenps=False
        )
    )
    return config


def _add_all_folders(config):
    all_filetypes = [
        "analysis",
        "config",
        "log",
        "mon",
        "scripts",
        "ignore",
        "unknown",
        "src",
    ]
    config["general"]["out_filetypes"] = [
        "analysis",
        "log",
        "mon",
        "scripts",
        "ignore",
        "unknown",
        "outdata",
        "restart_out",
    ]
    config["general"]["in_filetypes"] = [
        "scripts",
        "input",
        "forcing",
        "bin",
        "config",
        "restart_in",
    ]
    config["general"]["reusable_filetypes"] = ["bin", "src"]

    config["general"]["thisrun_dir"] = (
        config["general"]["experiment_dir"]
        + "/run_"
        + config["general"]["run_datestamp"]
    )

    for filetype in all_filetypes:
        config["general"]["experiment_" + filetype + "_dir"] = (
            config["general"]["experiment_dir"] + "/" + filetype + "/"
        )

    all_filetypes.append("work")
    config["general"]["all_filetypes"] = all_filetypes

    for filetype in all_filetypes:
        config["general"]["thisrun_" + filetype + "_dir"] = (
            config["general"]["thisrun_dir"] + "/" + filetype + "/"
        )

    config["general"]["work_dir"] = config["general"]["thisrun_work_dir"]

    all_model_filetypes = [
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
        "ignore",
    ]

    config["general"]["all_model_filetypes"] = all_model_filetypes

    for model in config["general"]["valid_model_names"]:
        for filetype in all_model_filetypes:
            if "restart" in filetype:
                filedir = "restart"
            else:
                filedir = filetype
            config[model]["experiment_" + filetype + "_dir"] = (
                config["general"]["experiment_dir"] + "/" + filedir + "/" + model + "/"
            )
            config[model]["thisrun_" + filetype + "_dir"] = (
                config["general"]["thisrun_dir"] + "/" + filedir + "/" + model + "/"
            )
            config[model]["all_filetypes"] = all_model_filetypes

    return config


def set_prev_date(config):
    import esm_parser

    """Sets several variables relevant for the previous date. Loops over all models in ``valid_model_names``, and sets model variables for:
    * ``prev_date``
    """
    for model in config["general"]["valid_model_names"]:
        if "time_step" in config[model] and not (
            isinstance(config[model]["time_step"], str)
            and "${" in config[model]["time_step"]
        ):
            config[model]["prev_date"] = config["general"]["current_date"] - (
                0,
                0,
                0,
                0,
                0,
                int(config[model]["time_step"]),
            )

        # NOTE(PG, MAM): Here we check if the time step still has a variable which might be set in a different model, and resolve this case
        elif "time_step" in config[model] and (
            isinstance(config[model]["time_step"], str)
            and "${" in config[model]["time_step"]
        ):
            dt = esm_parser.find_variable(
                model, config[model]["time_step"], config, [], []
            )
            config[model]["prev_date"] = config["general"]["current_date"] - (
                0,
                0,
                0,
                0,
                0,
                int(dt),
            )

        else:
            config[model]["prev_date"] = config["general"]["current_date"]
    return config


def set_parent_info(config):
    import esm_parser

    """Sets several variables relevant for the previous date. Loops over all models in ``valid_model_names``, and sets model variables for:
    * ``parent_expid``
    * ``parent_date``
    * ``parent_restart_dir``
    """

    # Make sure "ini_parent_dir" and "ini_restart_dir" both work:
    for model in config["general"]["valid_model_names"]:
        if not "ini_parent_dir" in config[model]:
            if "ini_restart_dir" in config[model]:
                config[model]["ini_parent_dir"] = config[model]["ini_restart_dir"]
        if not "ini_parent_exp_id" in config[model]:
            if "ini_restart_exp_id" in config[model]:
                config[model]["ini_parent_exp_id"] = config[model]["ini_restart_exp_id"]
        if not "ini_parent_date" in config[model]:
            if "ini_restart_date" in config[model]:
                config[model]["ini_parent_date"] = config[model]["ini_restart_date"]

    # check if parent is defined in esm_tools style
    # (only given for setup)
    setup = config["general"]["setup_name"]
    if not setup in config:
        setup = "general"
    if "ini_parent_exp_id" in config[setup]:
        for model in config["general"]["valid_model_names"]:
            if not "ini_parent_exp_id" in config[model]:
                config[model]["ini_parent_exp_id"] = config[setup]["ini_parent_exp_id"]
    if "ini_parent_date" in config[setup]:
        for model in config["general"]["valid_model_names"]:
            if not "ini_parent_date" in config[model]:
                config[model]["ini_parent_date"] = config[setup]["ini_parent_date"]
    if "ini_parent_dir" in config[setup]:
        for model in config["general"]["valid_model_names"]:
            if not "ini_parent_dir" in config[model]:
                config[model]["ini_parent_dir"] = (
                    config[setup]["ini_parent_dir"] + "/" + model
                )

    # Get correct parent info
    for model in config["general"]["valid_model_names"]:
        if config[model]["lresume"] == True and config["general"]["run_number"] == 1:
            config[model]["parent_expid"] = config[model]["ini_parent_exp_id"]
            if "parent_date" not in config[model]:
                config[model]["parent_date"] = config[model]["ini_parent_date"]
            config[model]["parent_restart_dir"] = config[model]["ini_parent_dir"]
        else:
            config[model]["parent_expid"] = config["general"]["expid"]
            if "parent_date" not in config[model]:
                config[model]["parent_date"] = config[model]["prev_date"]
            config[model]["parent_restart_dir"] = config[model][
                "experiment_restart_in_dir"
            ]
    return config


def finalize_config(config):
    config.finalize()
    return config


def add_submission_info(config):
    import os
    from . import batch_system

    bs = batch_system(config, config["computer"]["batch_system"])

    submitted = bs.check_if_submitted()
    if submitted:
        jobid = bs.get_jobid()
    else:
        jobid = os.getpid()

    config["general"]["submitted"] = submitted
    config["general"]["jobid"] = jobid
    return config


def initialize_batch_system(config):
    from . import batch_system

    config["general"]["batch"] = batch_system(
        config, config["computer"]["batch_system"]
    )
    return config


def initialize_coupler(config):
    if config["general"]["standalone"] == False:
        from . import coupler

        for model in list(config):
            if model in coupler.known_couplers:
                config["general"]["coupler_config_dir"] = (
                    config["general"]["base_dir"]
                    + "/"
                    + config["general"]["expid"]
                    + "/run_"
                    + config["general"]["run_datestamp"]
                    + "/config/"
                    + model
                    + "/"
                )
                config["general"]["coupler"] = coupler.coupler_class(config, model)
                break
        config["general"]["coupler"].add_files(config)
    return config


def set_logfile(config):
    config["general"]["experiment_log_file"] = config["general"].get(
        "experiment_log_file",
        config["general"]["experiment_log_dir"]
        + "/"
        + config["general"]["expid"]
        + "_"
        + config["general"]["setup_name"]
        + ".log",
    )
    return config
