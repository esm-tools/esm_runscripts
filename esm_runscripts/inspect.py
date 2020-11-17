import filecmp
import os
import glob

def run_job(config):
    from .helpers import evaluate
    config = evaluate(config, "inspect", "inspect_recipe")
    return config


def inspect_overview(config):
    if config["general"]["inspect"] == "overview":
        from .compute import _show_simulation_info
        config = _show_simulation_info(config)
    return config


def inspect_namelists(config):
    if config["general"]["inspect"] == "namelists":
        from .namelists import Namelist
        for model in config["general"]["valid_model_names"]:
            config[model] = Namelist.nmls_load(config[model])
            config[model] = Namelist.nmls_output(config[model])
    return config


def inspect_folder(config):
    import os
    checkpath = config["general"]["thisrun_dir"] + "/" + config["general"]["inspect"]
    if os.path.isdir(checkpath):
        all_files = os.listdir(checkpath)
        print(f"Files in folder {checkpath}:")
        for thisfile in sorted(all_files):
            print(f" -- {thisfile}")
    return config


def inspect_file(config):
    import os
    exclude = ["work"]
    knownfiles = {}
    if config["general"]["inspect"] == "lastlog":
        maybe_file = config["computer"]["thisrun_logfile"].replace("%j", "*")
    else:
        maybe_file = config["general"]["inspect"]

    for path, subdirs, files in os.walk(config["general"]["thisrun_dir"]):
        if not path.endswith("work"): # skip work for now
            for full_filepath in glob.iglob(os.path.join(path, maybe_file)):
                cat_file(full_filepath)
                knownfiles.update({os.path.basename(full_filepath): full_filepath})

    for path, subdirs, files in os.walk(config["general"]["thisrun_dir"] + "/work"):
        for full_filepath in glob.iglob(os.path.join(path, maybe_file)):
            somefile = os.path.basename(full_filepath)
            if somefile in knownfiles:
                if filecmp.cmp(knownfiles[somefile], full_filepath):
                    print(f"File {full_filepath} is identical to {knownfiles[somefile]}, skipping.")
                    continue
                else:
                    print(f"File {full_filepath} differs from {knownfiles[somefile]}.")
            cat_file(full_filepath)
    return config

def cat_file(full_filepath):
    if os.path.isfile(full_filepath):
        print (f"Content of {full_filepath}:")
        with open(full_filepath, "r") as log:
            print(log.read())
