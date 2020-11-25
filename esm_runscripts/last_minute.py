import os
import copy
import esm_parser

class last_minute_changes:
    def __init__(self, config):
        self.modify_config_file = config["general"]["command_line_config"].get("modify_config_file")

        if self.modify_config_file:
            self.modify_config_file_abspath = os.path.abspath(modify_config_file)
            self.modify_config = esm_parser.yaml_file_to_dict(modify_config_file_abspath)
            
            config["general"]["modify_config"] = copy.deepcopy(self.modify_config)
            config["general"]["modify_config_file_abspath"] = self.modify_config_file_abspath 

            config["general"]["original_command"] = config["general"]["original_command"].replace(
                    self.modify_config_file, 
                    self.modify_config_file_abspath
                    )

        else:
            self.modify_config_file_abspath = self.modify_config = None


def apply_last_minute_changes(config):
    config["general"]["modify_config_memo"] = last_minute_changes(config)

    modify_config = config["general"]["modify_config_memo"].modify_config

    if modify_config:
        settings = modify_config.get("build_and_run_modifications", {}).get("machine", {}).get("chooseable_settings")
        _modify_config_with_settings(config, settings)

        settings = modify_config.get("build_only_modifications", {}).get("machine", {}).get("environment_settings")
        _modify_config_with_settings(config, settings)

        settings = modify_config.get("run_only_modifications", {}).get("machine", {}).get("chooseable_settings")
        _modify_config_with_settings(config, settings)
        
        settings = modify_config.get("run_only_modifications", {}).get("batch_system", {}).get("direct_settings")
        _modify_config_with_settings(config, settings)

    return config


def restore_protected_last_minute_changes(config):
    if config["general"]["modify_config_memo"]:
        if config["general"]["modify_config_memo"].config:
            config["general"]["modify_config"] = config["general"]["modify_config_memo"].config
        del  config["general"]["modify_config_memo"]

    if config["general"].has_key("modify_config_memo"): # Entry could exist but be False
        del  config["general"]["modify_config_memo"]

    return config


def _modify_config_with_settings(self, config, settings):
    if settings:
        for k, v in settings.items():
            path_to_key = k.split(".")
            entry = path_to_key.pop()
            selected_config = config
            for k2 in path_to_key:
                selected_config = selected_config[k2]
            if type(selected_config) == dict:
                selected_config[entry] = v
            elif type(selected_config) == list:
                selected_config.append(entry + "=" + v)

            else:
                raise ValueError("unexpected container type (neither dict nor list")

