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
