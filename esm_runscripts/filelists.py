from loguru import logger

#@staticmethod
def rename_sources_to_targets(config):
    import copy
    import sys, os
    import time
    import six

    # Purpose of this routine is to make sure that filetype_sources and filetype_targets are set correctly,
    # and _in_work is unset
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:

            sources = filetype + "_sources" in config[model]
            targets = filetype + "_targets" in config[model]
            in_work = filetype + "_in_work" in config[model]

            if filetype in config["general"]["out_filetypes"]: # stuff to be copied out of work

                if sources and targets and in_work:
                    if not config[model][filetype + "_sources"] == config[model][filetype + "_in_work"]:
                        logger.debug("Mismatch between " + filetype + "_sources and " + filetype + "_in_work in model " + model)
                        sys.exit(-1)

                elif sources and targets and not in_work:
                    # all fine
                    pass

                elif sources and not targets:
                    logger.debug("Renaming sources to targets for filetype " + filetype + " in model " + model)
                    config[model][filetype + "_targets"] = copy.deepcopy(config[model][filetype + "_sources"])
                    if in_work:
                        config[model][filetype + "_sources"] = copy.deepcopy(config[model][filetype + "_in_work"])

                elif targets and not sources:
                    if in_work:
                        config[model][filetype + "_sources"] = copy.deepcopy(config[model][filetype + "_in_work"])
                    else:
                        config[model][filetype + "sources"] = copy.deepcopy(config[model][filetype + "_targets"])

            else: # stuff to be copied into work

                if sources and targets and in_work:
                    if not config[model][filetype + "_targets"] == config[model][filetype + "_in_work"]:
                        logger.debug("Mismatch between " + filetype + "_targets and " + filetype + "_in_work in model " + model)
                        sys.exit(-1)

                elif sources and targets and not in_work:
                    # all fine
                    pass

                elif (not sources and in_work) or (not sources and targets) :
                    logger.error(filetype + "_sources missing in model " + model)
                    sys.exit(-1)


                elif sources and not targets:
                    if in_work:
                        config[model][filetype + "_targets"] = copy.deepcopy(config[model][filetype + "_in_work"])
                    else:
                        config[model][filetype + "_targets"]= {}
                        for descrip, name in six.iteritems(config[model][filetype + "_sources"]):
                            config[model][filetype + "_targets"].update({ descrip: os.path.basename(name) })

            if in_work:
                del config[model][filetype + "_in_work"]

    return config



def complete_targets(config):
    import os
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_sources" in config[model]:
                for categ in config[model][filetype + "_sources"]:
                    if not categ in config[model][filetype + "_targets"]:
                        config[model][filetype + "_targets"][categ] = os.path.basename(config[model][filetype + "_sources"][categ])
    return config


def complete_sources(config):
    import os
    for filetype in config["general"]["out_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_sources" in config[model]:
                for categ in config[model][filetype + "_sources"]:
                    if not config[model][filetype + "_sources"][categ].startswith("/"):
                        config[model][filetype + "_sources"][categ] = config["general"]["thisrun_work_dir"] + "/" + config[model][filetype + "_sources"][categ]
    return config



#@staticmethod
def choose_needed_files(config):
    import six
    import sys

    # aim of this function is to only take those files specified in fileytype_files
    # (if exists), and then remove filetype_files

    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:

            if not filetype + "_files" in config[model]:
                continue

            new_sources = new_targets = {}
            for categ, name in six.iteritems(config[model][filetype + "_files"]):
                if not name in config[model][filetype + "_sources"]:
                    logger.error("Implementation " + name + " not found for filetype " + filetype + " of model " + model)
                    sys.exit(-1)
                new_sources.update({ categ : config[model][filetype + "_sources"][name]})

            config[model][filetype + "_sources"] = new_sources

            all_categs = list(config[model][filetype + "_targets"].keys())
            for categ in all_categs:
                if not categ in config[model][filetype + "_sources"]:
                    del config[model][filetype + "_targets"][categ]

            del config[model][filetype + "_files"]

    return config



#@staticmethod
def globbing(config):
    import six
    import glob
    import os
    import copy
    import time

    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_sources" in config[model]:
                oldconf = copy.deepcopy(config[model])
                for descr, filename in six.iteritems(oldconf[filetype + "_sources"]):  # * only in targets if denotes subfolder
                    if "*" in filename:
                        del config[model][filetype + "_sources"][descr]
                        all_filenames = glob.glob(filename)
                        running_index = 0

                        for new_filename in all_filenames:
                            newdescr = descr + "_glob_" + str(running_index)
                            config[model][filetype + "_sources"][newdescr] = new_filename
                            if config[model][filetype +  "_targets"][descr] == filename: #source and target are identical if autocompleted
                                config[model][filetype + "_targets"][newdescr] = os.path.basename(new_filename)
                            else:
                                config[model][filetype + "_targets"][newdescr] = config[model][filetype + "_targets"][descr]
                            running_index += 1

                        del config[model][filetype + "_targets"][descr]

    return config




#@staticmethod
def target_subfolders(config):
    import six
    import os
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_targets" in config[model]:
                for descr, filename in six.iteritems(config[model][filetype + "_targets"]):  # * only in targets if denotes subfolder
                        if not descr in config[model][filetype + "_sources"]:
                            logger.error("no source found for target " + name + " in model " + model)
                            sys.exit(-1)
                        if "*" in filename:
                            source_filename = os.path.basename(config[model][filetype + "_sources"][descr])
                            # directory wildcards are given as /*, wildcards in filenames are handled
                            # seb-wahl: directory wildcards are given as /*, wildcards in filenames are handled
                            # in routine 'globbing' above, if we don't check here, wildcards are handled twice
                            # for files and hence filenames of e.g. restart files are screwed up.
                            if filename.endswith("/*"):
                                config[model][filetype + "_targets"][descr] = filename.replace("*", source_filename)
                            else:
                                config[model][filetype + "_targets"][descr] = source_filename
                        elif filename.endswith("/"):
                            source_filename = os.path.basename(config[model][filetype + "_sources"][descr])
                            config[model][filetype + "_targets"][descr] = filename + source_filename

    return config


def complete_restart_in(config):
    import esm_parser
    for model in config["general"]["valid_model_names"]:
        if not config[model]["lresume"] and config["general"]["run_number"] == 1:
            if "restart_in_sources" in config[model]:
                del config[model]["restart_in_sources"]
                del config[model]["restart_in_targets"]
                del config[model]["restart_in_intermediate"]
        if "restart_in_sources" in config[model]:
            for categ in list(config[model]["restart_in_sources"].keys()):
                if not config[model]["restart_in_sources"][categ].startswith("/"):
                    config[model]["restart_in_sources"][categ] = config[model]["parent_restart_dir"] + config[model]["restart_in_sources"][categ]
    return config





#@staticmethod
def assemble_intermediate_files_and_finalize_targets(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_targets" in config[model]:
                if not filetype + "_intermediate" in config[model]:
                    config[model][filetype + "_intermediate"] = {}
                for category in config[model][filetype + "_targets"]:
                    target_name = config[model][filetype + "_targets"][category]

                    interm_dir = (config[model]["thisrun_" + filetype + "_dir"] + "/").replace("//", "/")
                    if filetype in config["general"]["out_filetypes"]:
                        target_dir = (config[model]["experiment_" + filetype + "_dir"] + "/").replace("//", "/")
                        source_dir = (config["general"]["thisrun_work_dir"] + "/").replace("//", "/")
                        if not config[model][filetype + "_sources"][category].startswith("/"):
                            config[model][filetype + "_sources"][category] =  source_dir + config[model][filetype + "_sources"][category]
                    else:
                        target_dir = (config["general"]["thisrun_work_dir"]).replace("//", "/")


                    config[model][filetype + "_intermediate"][category] =  interm_dir + target_name
                    config[model][filetype + "_targets"][category] =  target_dir + target_name

    return config



#@staticmethod
def replace_year_placeholder(config):
    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_targets" in config[model]:
                if filetype + "_additional_information" in config[model]:
                    for file_category in config[model][filetype + "_additional_information"]:
                        if file_category in config[model][filetype + "_targets"]:
                            if "@YEAR@" in config[model][filetype + "_targets"][file_category]:
                                all_years = [config["general"]["current_date"].year]

                                if (
                                   "need_timestep_before" in config[model][filetype + "_additional_information"][file_category]
                                ):
                                    all_years.append(config["general"]["prev_date"].year)
                                if (
                                   "need_timestep_after" in config[model][filetype + "_additional_information"][file_category]
                                ):
                                    all_years.append(config["general"]["next_date"].year)
                                if (
                                   "need_year_before" in config[model][filetype + "_additional_information"][file_category]
                                ):
                                    all_years.append(config["general"]["current_date"].year - 1)
                                if (
                                   "need_year_after" in config[model][filetype + "_additional_information"][file_category]
                                ):
                                    all_years.append(config["general"]["next_date"].year + 1 )

                                all_years = list(dict.fromkeys(all_years)) # removes duplicates

                                for year in all_years:

                                    new_category = file_category + "_year_" + str(year)
                                    new_target_name = config[model][filetype + "_targets"][file_category].replace("@YEAR@", str(year))
                                    new_source_name = config[model][filetype + "_sources"][file_category].replace("@YEAR@", str(year))

                                    config[model][filetype + "_targets"][new_category] = new_target_name
                                    config[model][filetype + "_sources"][new_category] = new_source_name

                                del config[model][filetype + "_targets"][file_category]
                                del config[model][filetype + "_sources"][file_category]

    return config




#@staticmethod
def log_used_files(config, filetypes):
    for model in config["general"]["valid_model_names"]:
        with open(
            config[model]["thisrun_config_dir"]
            + "/"                + config["general"]["expid"]
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
                        flist.write("\nSource: " + config[model][filetype + "_sources"][category])
                        flist.write("\nExp Tree: " + config[model][filetype + "_intermediate"][category])
                        flist.write("\nTarget: " + config[model][filetype + "_targets"][category])
                        flist.write("\n")
                flist.write("\n")
                flist.write(80 * "-")
    return config



def find_correct_source(mconfig, file_source, year): # not needed in compute anymore, moved to jobclass
    if isinstance(file_source, dict):
        logger.debug(
            "Checking which file to use for this year: %s",
            year,
        )
        for fname, valid_years in six.iteritems(file_source):
            logger.debug("Checking %s", fname)
            min_year = float(valid_years.get("from", "-inf"))
            max_year = float(valid_years.get("to", "inf"))
            logger.debug("Valid from: %s", min_year)
            logger.debug("Valid to: %s", max_year)
            logger.debug(
                "%s <= %s --> %s",
                min_year,
                year,
                min_year <= year,
            )
            logger.debug(
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



    #@staticmethod
def check_for_unknown_files(config):
    import glob
    import os
    import time
    #files = os.listdir(self.config["general"]["thisrun_work_dir"])
    all_files = glob.iglob(config["general"]["thisrun_work_dir"] + '**/*', recursive = True)

    known_files = [config["general"]["thisrun_work_dir"] + "/" + "hostfile_srun", config["general"]["thisrun_work_dir"] + "/" + "namcouple"]

    for filetype in config["general"]["all_model_filetypes"]:
        for model in config["general"]["valid_model_names"]:
            if filetype + "_sources" in config[model]:
                known_files += list(config[model][filetype + "_sources"].values())
                known_files += list(config[model][filetype + "_targets"].values())

    known_files = [os.path.realpath(known_file) for known_file in known_files]
    known_files = list(dict.fromkeys(known_files))

    unknown_files = []
    index = 0

    for thisfile in all_files:


        if os.path.realpath(thisfile) in known_files + unknown_files:
            continue
        if not "unknown_sources" in config["general"]:
            config["general"]["unknown_sources"] = {}
            config["general"]["unknown_targets"] = {}
            config["general"]["unknown_intermediate"] = {}

        config["general"]["unknown_sources"][index] = os.path.realpath(thisfile)
        config["general"]["unknown_targets"][index] = os.path.realpath(thisfile).replace(os.path.realpath(config["general"]["thisrun_work_dir"]), os.path.realpath(config["general"]["experiment_unknown_dir"]))
        config["general"]["unknown_intermediate"][index] = os.path.realpath(thisfile).replace(os.path.realpath(config["general"]["thisrun_work_dir"]), os.path.realpath(config["general"]["thisrun_unknown_dir"]))

        unknown_files.append(os.path.realpath(thisfile))

        index += 1
        logger.info("File is not in list: " + os.path.realpath(thisfile) )

    return config



