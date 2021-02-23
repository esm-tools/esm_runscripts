import os
import sys
import shutil
import filecmp
import copy
import time
import six
import glob

import esm_parser

import f90nml
import esm_tools

def rename_sources_to_targets(config):
    # Purpose of this routine is to make sure that filetype_sources and
    # filetype_targets are set correctly, and _in_work is unset
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:

            sources = filetype + "_sources" in config[model]
            targets = filetype + "_targets" in config[model]
            in_work = filetype + "_in_work" in config[model]

            if (
                filetype in config["general"]["out_filetypes"]
            ):  # stuff to be copied out of work

                if sources and targets and in_work:
                    if (
                        not config[model][filetype + "_sources"]
                        == config[model][filetype + "_in_work"]
                    ):
                        print(
                            "Mismatch between "
                            + filetype
                            + "_sources and "
                            + filetype
                            + "_in_work in model "
                            + model
                        )
                        sys.exit(-1)

                elif sources and targets and not in_work:
                    # all fine
                    pass

                elif sources and not targets:
                    if config["general"]["verbose"]:
                        print(
                            "Renaming sources to targets for filetype "
                            + filetype
                            + " in model "
                            + model
                        )
                    config[model][filetype + "_targets"] = copy.deepcopy(
                        config[model][filetype + "_sources"]
                    )
                    if in_work:
                        config[model][filetype + "_sources"] = copy.deepcopy(
                            config[model][filetype + "_in_work"]
                        )

                elif targets and not sources:
                    if in_work:
                        config[model][filetype + "_sources"] = copy.deepcopy(
                            config[model][filetype + "_in_work"]
                        )
                    else:
                        config[model][filetype + "sources"] = copy.deepcopy(
                            config[model][filetype + "_targets"]
                        )

            else:  # stuff to be copied into work

                if sources and targets and in_work:
                    if (
                        not config[model][filetype + "_targets"]
                        == config[model][filetype + "_in_work"]
                    ):
                        print(
                            "Mismatch between "
                            + filetype
                            + "_targets and "
                            + filetype
                            + "_in_work in model "
                            + model
                        )
                        sys.exit(-1)

                elif sources and targets and not in_work:
                    # all fine
                    pass

                elif (not sources and in_work) or (not sources and targets):
                    print(filetype + "_sources missing in model " + model)
                    sys.exit(-1)

                elif sources and not targets:
                    if in_work:
                        config[model][filetype + "_targets"] = copy.deepcopy(
                            config[model][filetype + "_in_work"]
                        )
                    else:
                        config[model][filetype + "_targets"] = {}
                        for descrip, name in six.iteritems(
                            config[model][filetype + "_sources"]
                        ):
                            config[model][filetype + "_targets"].update(
                                {descrip: os.path.basename(name)}
                            )

            if in_work:
                del config[model][filetype + "_in_work"]

    return config


def complete_targets(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_sources" in config[model]:
                for categ in config[model][filetype + "_sources"]:
                    if not categ in config[model][filetype + "_targets"]:
                        config[model][filetype + "_targets"][categ] = os.path.basename(
                            config[model][filetype + "_sources"][categ]
                        )
    return config


def complete_sources(config):
    if config["general"]["verbose"]:
        print("Complete sources")
    for filetype in config["general"]["out_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_sources" in config[model]:
                for categ in config[model][filetype + "_sources"]:
                    if not config[model][filetype + "_sources"][categ].startswith("/"):
                        config[model][filetype + "_sources"][categ] = (
                            config["general"]["thisrun_work_dir"]
                            + "/"
                            + config[model][filetype + "_sources"][categ]
                        )
    return config


def reuse_sources(config):
    if config["general"]["run_number"] == 1:
        return config
    for filetype in config["general"]["reusable_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_sources" in config[model]:
                for categ in config[model][filetype + "_sources"]:
                    config[model][filetype + "_sources"][categ] = (
                        config[model]["experiment_" + filetype + "_dir"]
                        + "/"
                        + config[model][filetype + "_targets"][categ].split("/")[-1]
                    )
    return config


def choose_needed_files(config):
    # aim of this function is to only take those files specified in fileytype_files
    # (if exists), and then remove filetype_files

    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:

            if not filetype + "_files" in config[model]:
                continue

            new_sources = new_targets = {}
            for categ, name in six.iteritems(config[model][filetype + "_files"]):
                if not name in config[model][filetype + "_sources"]:
                    print(
                        "Implementation "
                        + name
                        + " not found for filetype "
                        + filetype
                        + " of model "
                        + model
                    )
                    print(config[model][filetype + "_files"])
                    print(config[model][filetype + "_sources"])
                    sys.exit(-1)
                new_sources.update({categ: config[model][filetype + "_sources"][name]})

            config[model][filetype + "_sources"] = new_sources

            all_categs = list(config[model][filetype + "_targets"].keys())
            for categ in all_categs:
                if not categ in config[model][filetype + "_sources"]:
                    del config[model][filetype + "_targets"][categ]

            del config[model][filetype + "_files"]

    return config


def globbing(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_sources" in config[model]:
                oldconf = copy.deepcopy(config[model])
                for descr, filename in six.iteritems(
                    oldconf[filetype + "_sources"]
                ):  # * only in targets if denotes subfolder
                    if "*" in filename:
                        del config[model][filetype + "_sources"][descr]
                        # skip subdirectories in file list, otherwise they
                        # will be listed as missing files later on
                        all_filenames = [f for f in glob.glob(filename) if not os.path.isdir(f)]
                        running_index = 0

                        for new_filename in all_filenames:
                            newdescr = descr + "_glob_" + str(running_index)
                            config[model][filetype + "_sources"][
                                newdescr
                            ] = new_filename
                            if (
                                config[model][filetype + "_targets"][descr] == filename
                            ):  # source and target are identical if autocompleted
                                config[model][filetype + "_targets"][
                                    newdescr
                                ] = os.path.basename(new_filename)
                            else:
                                config[model][filetype + "_targets"][newdescr] = config[
                                    model
                                ][filetype + "_targets"][descr]
                            running_index += 1

                        del config[model][filetype + "_targets"][descr]
    return config


def target_subfolders(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_targets" in config[model]:
                for descr, filename in six.iteritems(
                    config[model][filetype + "_targets"]
                ):  # * only in targets if denotes subfolder
                    if not descr in config[model][filetype + "_sources"]:
                        print(
                            "no source found for target " + name + " in model " + model
                        )
                        sys.exit(-1)
                    if "*" in filename:
                        source_filename = os.path.basename(
                            config[model][filetype + "_sources"][descr]
                        )
                        # directory wildcards are given as /*, wildcards in filenames are handled
                        # seb-wahl: directory wildcards are given as /*, wildcards in filenames are handled
                        # in routine 'globbing' above, if we don't check here, wildcards are handled twice
                        # for files and hence filenames of e.g. restart files are screwed up.
                        if filename.endswith("/*"):
                            config[model][filetype + "_targets"][
                                descr
                            ] = filename.replace("*", source_filename)
                        elif "/" in filename:
                            config[model][filetype + "_targets"][
                                descr
                            ] = "/".join(filename.split("/")[:-1]) + "/" + source_filename.split("/")[-1]
                        else:
                            config[model][filetype + "_targets"][
                                descr
                            ] = source_filename.split("/")[-1]
                    elif filename.endswith("/"):
                        source_filename = os.path.basename(
                            config[model][filetype + "_sources"][descr]
                        )
                        config[model][filetype + "_targets"][descr] = (
                            filename + source_filename
                        )

    return config


def complete_restart_in(config):
    for model in config["general"]["valid_model_names"]:
        if not config[model]["lresume"] and config["general"]["run_number"] == 1: # isn't that redundant? if run_number > 1 then lresume == True?
            if "restart_in_sources" in config[model]:
                del config[model]["restart_in_sources"]
            if "restart_in_targets" in config[model]:
                del config[model]["restart_in_targets"]
            if "restart_in_intermediate" in config[model]:
                del config[model]["restart_in_intermediate"]
        if "restart_in_sources" in config[model]:
            for categ in list(config[model]["restart_in_sources"].keys()):
                if not config[model]["restart_in_sources"][categ].startswith("/"):
                    config[model]["restart_in_sources"][categ] = (
                        config[model]["parent_restart_dir"]
                        + config[model]["restart_in_sources"][categ]
                    )
    return config


def assemble_intermediate_files_and_finalize_targets(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_targets" in config[model]:
                if not filetype + "_intermediate" in config[model]:
                    config[model][filetype + "_intermediate"] = {}
                for category in config[model][filetype + "_targets"]:
                    target_name = config[model][filetype + "_targets"][category]

                    interm_dir = (
                        config[model]["thisrun_" + filetype + "_dir"] + "/"
                    ).replace("//", "/")
                    if filetype in config["general"]["out_filetypes"]:
                        target_dir = (
                            config[model]["experiment_" + filetype + "_dir"] + "/"
                        ).replace("//", "/")
                        source_dir = (
                            config["general"]["thisrun_work_dir"] + "/"
                        ).replace("//", "/")
                        if not config[model][filetype + "_sources"][
                            category
                        ].startswith("/"):
                            config[model][filetype + "_sources"][category] = (
                                source_dir
                                + config[model][filetype + "_sources"][category]
                            )
                    else:
                        target_dir = (config["general"]["thisrun_work_dir"]).replace(
                            "//", "/"
                        )

                    config[model][filetype + "_intermediate"][category] = (
                        interm_dir + target_name
                    )
                    config[model][filetype + "_targets"][category] = (
                        target_dir + target_name
                    )

    return config


def replace_year_placeholder(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_targets" in config[model]:
                if filetype + "_additional_information" in config[model]:
                    for file_category in config[model][
                        filetype + "_additional_information"
                    ]:
                        if file_category in config[model][filetype + "_targets"]:
                            if (
                                "@YEAR@"
                                in config[model][filetype + "_targets"][file_category]
                            ):
                                all_years = [config["general"]["current_date"].year]

                                if (
                                    "need_timestep_before"
                                    in config[model][
                                        filetype + "_additional_information"
                                    ][file_category]
                                ):
                                    all_years.append(
                                        config["general"]["prev_date"].year
                                    )
                                if (
                                    "need_timestep_after"
                                    in config[model][
                                        filetype + "_additional_information"
                                    ][file_category]
                                ):
                                    all_years.append(
                                        config["general"]["next_date"].year
                                    )
                                if (
                                    "need_year_before"
                                    in config[model][
                                        filetype + "_additional_information"
                                    ][file_category]
                                ):
                                    all_years.append(
                                        config["general"]["current_date"].year - 1
                                    )
                                if (
                                    "need_year_after"
                                    in config[model][
                                        filetype + "_additional_information"
                                    ][file_category]
                                ):
                                    all_years.append(
                                        config["general"]["current_date"].year + 1
                                    )

                                all_years = list(
                                    dict.fromkeys(all_years)
                                )  # removes duplicates

                                for year in all_years:

                                    new_category = file_category + "_year_" + str(year)
                                    new_target_name = config[model][
                                        filetype + "_targets"
                                    ][file_category].replace("@YEAR@", str(year))
                                    new_source_name = config[model][
                                        filetype + "_sources"
                                    ][file_category].replace("@YEAR@", str(year))

                                    config[model][filetype + "_targets"][
                                        new_category
                                    ] = new_target_name
                                    config[model][filetype + "_sources"][
                                        new_category
                                    ] = new_source_name

                                del config[model][filetype + "_targets"][file_category]
                                del config[model][filetype + "_sources"][file_category]

    return config


def log_used_files(config):
    filetypes = config["general"]["relevant_filetypes"]
    for model in config["general"]["valid_model_names"] + ["general"]:
        with open(
            config[model]["thisrun_config_dir"]
            + "/"
            + config["general"]["expid"]
            + "_filelist_"
            + config["general"]["run_datestamp"],
            "w",
        ) as flist:
            flist.write(
                "These files are used for \nexperiment %s\ncomponent %s\ndate %s"
                % (
                    config["general"]["expid"],
                    model,
                    config["general"]["run_datestamp"],
                )
            )
            flist.write("\n")
            flist.write(80 * "-")
            for filetype in filetypes:
                if filetype + "_sources" in config[model]:
                    flist.write("\n" + filetype.upper() + ":\n")
                    for category in config[model][filetype + "_sources"]:
#                        esm_parser.pprint_config(config[model]) 
                        flist.write(
                            "\nSource: "
                            + config[model][filetype + "_sources"][category]
                        )
                        flist.write(
                            "\nExp Tree: "
                            + config[model][filetype + "_intermediate"][category]
                        )
                        flist.write(
                            "\nTarget: "
                            + config[model][filetype + "_targets"][category]
                        )
                        if config["general"]["verbose"]:
                            print(
                                "-  "
                                + config[model][filetype + "_targets"][category]
                                + " : "
                                + config[model][filetype + "_sources"][category]
                            )
                        flist.write("\n")
                flist.write("\n")
                flist.write(80 * "-")
    return config


def check_for_unknown_files(config):
    # files = os.listdir(config["general"]["thisrun_work_dir"])
    all_files = glob.iglob(
        config["general"]["thisrun_work_dir"] + "**/*", recursive=True
    )

    known_files = [
        config["general"]["thisrun_work_dir"] + "/" + "hostfile_srun",
        config["general"]["thisrun_work_dir"] + "/" + "namcouple",
        config["general"]["thisrun_work_dir"] + "/" + "coupling.xml",
    ]

    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            if filetype + "_sources" in config[model]:
                known_files += list(config[model][filetype + "_sources"].values())
                known_files += list(config[model][filetype + "_targets"].values())

    known_files = [os.path.realpath(known_file) for known_file in known_files]
    known_files = list(dict.fromkeys(known_files))

    if not "unknown_sources" in config["general"]:
        config["general"]["unknown_sources"] = {}
        config["general"]["unknown_targets"] = {}
        config["general"]["unknown_intermediate"] = {}

    unknown_files = []
    index = 0

    for thisfile in all_files:

        if os.path.realpath(thisfile) in known_files + unknown_files:
            continue
        config["general"]["unknown_sources"][index] = os.path.realpath(thisfile)
        config["general"]["unknown_targets"][index] = os.path.realpath(
            thisfile
        ).replace(
            os.path.realpath(config["general"]["thisrun_work_dir"]),
            os.path.realpath(config["general"]["experiment_unknown_dir"]),
        )
        config["general"]["unknown_intermediate"][index] = os.path.realpath(
            thisfile
        ).replace(
            os.path.realpath(config["general"]["thisrun_work_dir"]),
            os.path.realpath(config["general"]["thisrun_unknown_dir"]),
        )

        unknown_files.append(os.path.realpath(thisfile))

        index += 1
        print("Unknown file in work: " + os.path.realpath(thisfile))

    return config



def resolve_symlinks(file_source):
    if os.path.islink(file_source):
        points_to = os.path.realpath(file_source)

        # deniz: check if file links to itself. In UNIX 
        # ln -s endless_link endless_link is a valid command
        if os.path.abspath(file_source) == points_to:
            if config["general"]["verbose"]:
                print(f"file {file_source} links to itself")
            return file_source
        
        # recursively find the file that the link is pointing to
        return resolve_symlinks(points_to)
    else: 
        return(file_source)



def copy_files(config, filetypes, source, target):

    successful_files = []
    missing_files = {}

    if source == "init":
        text_source = "sources"
    elif source == "thisrun":
        text_source = "intermediate"
    elif source == "work":
        text_source = "sources"

    if target == "thisrun":
        text_target = "intermediate"
    elif target == "work":
        text_target = "targets"


    for filetype in [filetype for filetype in filetypes if not filetype == "ignore"]:
        for model in config["general"]["valid_model_names"] + ["general"]:
            movement_method = get_method(get_movement(config, model, filetype, source, target))
            if filetype + "_" + text_source in config[model]:
                sourceblock = config[model][filetype + "_" + text_source]
                targetblock = config[model][filetype + "_" + text_target]
                for categ in sourceblock:
                    file_source = os.path.normpath(sourceblock[categ])
                    # NOTE(PG): This is a really, really, REALLY bad hack and it
                    # makes me physically ill to look at:
                    # NOTE(MA): The previous implementation was not able to include
                    # namelists that have no ``namelist`` in their name. This is a more
                    # general implementation but it enforces the use of the
                    # ``namelists`` list to be defined for each model with namelists.
                    namelist_candidates = (
                        [item for item in config[model].get("namelists", [])]
                        + ["namelist"]
                    )
                    isnamelist = any(map(file_source.__contains__, namelist_candidates))
                    if source == "init":
                        if isnamelist and file_source.startswith("NONE_YET"):
                            file_source = esm_tools.get_namelist_filepath(
                                file_source.replace("NONE_YET/", "")
                            )
                    file_target = os.path.normpath(targetblock[categ])
                    if config["general"]["verbose"]:
                        print(f"source: {file_source}")
                        print(f"   --> target: {file_target}")
                    if file_source == file_target:
                        if config["general"]["verbose"]:
                            print(
                                f"Source and target paths are identical, skipping {file_source}"
                            )
                        continue
                    dest_dir = os.path.dirname(file_target)
                    file_source = resolve_symlinks(file_source)
                    if not os.path.isdir(file_source):
                        try:
                            if not os.path.isdir(dest_dir):
                                # MA: ``os.makedirs`` creates the specified directory
                                # and the parent directories if the last don't exist
                                # (same as with ``mkdir -p <directory>>``)
                                os.makedirs(dest_dir)
                            if not os.path.isfile(file_source):
                                print(f"File not found: {file_source}...")
                                missing_files.update({file_target: file_source})
                                continue
                            if os.path.isfile(file_target) and filecmp.cmp(
                                file_source, file_target
                            ):
                                if config["general"]["verbose"]:
                                    print(
                                        f"Source and target file are identical, skipping {file_source}"
                                    )
                                continue
                            movement_method(file_source, file_target)
                            #shutil.copy2(file_source, file_target)
                            successful_files.append(file_source)
                        except IOError:
                            print(
                                f"Could not copy {file_source} to {file_target} for unknown reasons."
                            )
                            missing_files.update({file_target: file_source})

    if missing_files:
        if not "files_missing_when_preparing_run" in config["general"]:
            config["general"]["files_missing_when_preparing_run"] = {}
        if config["general"]["verbose"]:
            six.print_("--- WARNING: These files were missing:")
            for missing_file in missing_files:
                print("  - " + missing_file + ": " + missing_files[missing_file])
        config["general"]["files_missing_when_preparing_run"].update(missing_files)
    return config


def report_missing_files(config):
    if "files_missing_when_preparing_run" in config["general"]:
        config = _check_fesom_missing_files(config)
        if not config["general"]["files_missing_when_preparing_run"] == {}:
            six.print_(80 * "=")
            print("MISSING FILES:")
        for missing_file in config["general"]["files_missing_when_preparing_run"]:
            print("--  " + missing_file + ": ")
            print(
                "        --> "
                + config["general"]["files_missing_when_preparing_run"][missing_file]
            )
        if not config["general"]["files_missing_when_preparing_run"] == {}:
            six.print_(80 * "=")
    return config


def _check_fesom_missing_files(config):
    """
     Checks for missing files in FESOM namelist.config

     Parameters
     ----------
     config : dict
         The experiment configuration

     Returns
     -------
     config : dict
     """
    if "fesom" in config["general"]["valid_model_names"]:
        namelist_config = f90nml.read(
            os.path.join(config["general"]["thisrun_work_dir"], "namelist.config")
        )
        for path_key, path in namelist_config["paths"].items():
            if path:  # Remove empty strings
                if not os.path.exists(path):
                    if "files_missing_when_preparing_run" not in config["general"]:
                        config["general"]["files_missing_when_preparing_run"] = {}
                    config["general"]["files_missing_when_preparing_run"][
                        path_key + " (from namelist.config in FESOM)"
                    ] = path
    return config





# FILE MOVEMENT METHOD STUFF

def create_missing_file_movement_entries(config):
    for model in config["general"]["valid_model_names"] + ["general"]:
        if not "file_movements" in config[model]:
            config[model]["file_movements"] = {}
        for filetype in config["general"]["all_model_filetypes"] + ["scripts", "unknown"] :
            if not filetype in config[model]["file_movements"]:
                config[model]["file_movements"][filetype] = {}
    return config



def complete_one_file_movement(config, model, filetype, movement, movetype):
    if not movement in config[model]["file_movements"][filetype]:
        config[model]["file_movements"][filetype][movement] = movetype
    return config



def get_method(movement):
    if movement == "copy":
        return shutil.copy2
    elif movement == "link":
        return os.symlink
    elif movement == "move":
        return os.rename
    print ("Unknown file movement type, using copy (safest option).")
    return shutil.copy2


def complete_all_file_movements(config):
    config = create_missing_file_movement_entries(config)

    for model in config["general"]["valid_model_names"] + ["general"]:
        print(f"Iterating model: {model}")
        mconfig = config[model]
        if model == "general":
            if "defaults.yaml" in mconfig:
                if "per_model_defaults" in mconfig["defaults.yaml"]:
                    if "file_movements" in mconfig["defaults.yaml"]["per_model_defaults"]:
                        mconfig["file_movements"] = mconfig["defaults.yaml"]["per_model_defaults"]["file_movements"]
                        del mconfig["defaults.yaml"]["per_model_defaults"]["file_movements"]
        if "file_movements" in mconfig:
            for filetype in config["general"]["all_model_filetypes"] + ["scripts", "unknown"]:
                if filetype in mconfig["file_movements"]:
                    if "all_directions" in mconfig["file_movements"][filetype]:
                        movement_type = mconfig["file_movements"][filetype]["all_directions"]
                        for movement in ['init_to_exp', 'exp_to_run', 'run_to_work', 'work_to_run']:
                            config = complete_one_file_movement(config, model, filetype, movement, movement_type)
                        del mconfig["file_movements"][filetype]["all_directions"]
            
            if "default" in mconfig["file_movements"]:
                if model == "hdmodel":
                    esm_parser.pprint_config(mconfig)
                if "all_directions" in mconfig["file_movements"]["default"]:
                    movement_type = mconfig["file_movements"]["default"]["all_directions"]
                    for movement in ['init_to_exp', 'exp_to_run', 'run_to_work', 'work_to_run']:
                        config = complete_one_file_movement(config, model, "default", movement, movement_type)
                    del mconfig["file_movements"]["default"]["all_directions"]

                for movement in mconfig["file_movements"]["default"]:
                    movement_type =  mconfig["file_movements"]["default"][movement]
                    for filetype in config["general"]["all_model_filetypes"] + ["scripts", "unknown"]:
                        config = complete_one_file_movement(config, model, filetype, movement, movement_type)
                del mconfig["file_movements"]["default"]
    return config


def get_movement(config, model, filetype, source, target):
    if source == "init":
        if config["general"]["run_number"] == 1 or filetype not in config["general"]["reusable_filetypes"]:
            return config[model]["file_movements"][filetype]["init_to_exp"]
        else:
            return config[model]["file_movements"][filetype]["exp_to_run"]
    elif source == "work":
        return config[model]["file_movements"][filetype]["work_to_run"]
    elif source == "thisrun" and target == "work":
        return config[model]["file_movements"][filetype]["run_to_work"]
    else:
        # This should NOT happen
        print (f"Error: Unknown file movement from {source} to {target}")
        sys.exit(42)


def assemble(config):
    config = complete_all_file_movements(config)
    config = rename_sources_to_targets(config)
    config = choose_needed_files(config)
    config = complete_targets(config)
    config = complete_sources(config)
    config = reuse_sources(config)
    config = replace_year_placeholder(config)

    config = complete_restart_in(config)
    config = globbing(config)
    config = target_subfolders(config)
    config = assemble_intermediate_files_and_finalize_targets(config)
    return config
