import os

import esm_parser
import esm_rcfile

from . import chunky_parts

def init_first_user_config(command_line_config, user_config):

    if not user_config:
        user_config = get_user_config_from_command_line(command_line_config)
       
    # maybe switch to another runscript, if iterative coupling
    if user_config["general"].get("iterative_coupling", False):
        user_config = chunky_parts.setup_correct_chunk_config(user_config)
        next_model = user_config["general"]["original_config"]["general"]["model_queue"][1]
        scriptname = user_config["general"]["original_config"][next_model]["runscript"]
        os.chdir(user_config["general"]["started_from"])
        print(os.listdir("."))
        command_line_config["scriptname"] = scriptname
        model_config = get_user_config_from_command_line(command_line_config)
        user_config = esm_parser.new_deep_update(user_config, model_config)

        esm_parser.pprint_config(user_config)

    if user_config["general"].get("debug_obj_init", False):
        pdb.set_trace()

    return user_config



def complete_config_from_user_config(user_config):
    config = get_total_config_from_user_config(user_config)

    if "verbose" not in config["general"]:
        config["general"]["verbose"] = False

    config["general"]["reset_calendar_to_last"] = False
    
    if config["general"].get("inspect"):
        config["general"]["jobtype"] = "inspect"
        
        if config["general"].get("inspect") not in [
                "workflow",
                "overview",
                "config",
                ]: 
            config["general"]["reset_calendar_to_last"] = True

    return config



def save_command_line_config(config, command_line_config):
    if command_line_config:
        config["general"]["command_line_config"] = command_line_config
    else:
        config["general"]["command_line_config"] = {}

    return config




def get_user_config_from_command_line(command_line_config):
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





def get_total_config_from_user_config(user_config):

    if "version" in user_config["general"]:
        version = str(user_config["general"]["version"])
    else:
        setup_name = user_config["general"]["setup_name"]
        if "version" in user_config[setup_name.replace("_standalone","")]:
            version = str(user_config[setup_name.replace("_standalone","")]["version"])
        else:
            version = "DEFAULT"

    config = esm_parser.ConfigSetup(user_config["general"]["setup_name"].replace("_standalone",""),
                                         version,
                                         user_config)

    config = add_esm_runscripts_defaults_to_config(config)

    config["computer"]["jobtype"] = config["general"]["jobtype"]
    config["general"]["experiment_dir"] = config["general"]["base_dir"] + "/" + config["general"]["expid"]

    return config



def add_esm_runscripts_defaults_to_config(config):
    if config['general'].get("use_venv") or esm_rcfile.FUNCTION_PATH.startswith("NONE_YET"):
        path_to_file = esm_tools.get_config_filepath("esm_software/esm_runscripts/defaults.yaml")
    else:
        path_to_file = esm_rcfile.FUNCTION_PATH + "/esm_software/esm_runscripts/defaults.yaml"
    default_config = esm_parser.yaml_file_to_dict(path_to_file)
    config["general"]["defaults.yaml"] = default_config
    config = distribute_per_model_defaults(config)
    return config


def distribute_per_model_defaults(config):
    default_config = config["general"]["defaults.yaml"]
    if "per_model_defaults" in default_config:
        for model in config["general"]["valid_model_names"]:
            config[model] = esm_parser.new_deep_update(config[model], default_config["per_model_defaults"])
    return config




