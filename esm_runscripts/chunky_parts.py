import os, copy
import sys
import yaml
import esm_parser


def setup_correct_chunk_config(config):
    # to be called from the top of prepare

    if not config["general"].get("iterative_coupling", False):
        return config

    print ("Starting the iterative coupling business")

    chunk_config = _restore_original_config(config)
    chunk_config = _initialize_chunk_date_file(chunk_config) # make sure file exists and points to NEXT run
    chunk_config = _read_chunk_date_file_if_exists(chunk_config)

 #   if _called_from_tidy_job(chunk_config):
#        chunk_config = _update_chunk_date_file(chunk_config)
    
    chunk_config = _set_model_queue(chunk_config)
    config = _store_original_config(chunk_config)

    return config




def _update_run_in_chunk(config):
    if not config["general"].get("iterative_coupling", False):
        return config

    config = _is_first_run_in_chunk(config)
    config = _is_last_run_in_chunk(config)
    config = _find_next_model_to_run(config)
    config = _find_next_chunk_number(config)
    return config
        


def _update_chunk_date_file(config):
    if not config["general"].get("iterative_coupling", False):
        return config

    # to be called at the end of tidy
    with open(config["general"]["chunk_date_file"], "w+") as chunk_dates:
        chunk_dates.write(config["general"]["next_chunk_number"] + " " + config["general"]["next_setup_name"])
    config["general"]["setup_name"] = config["general"]["next_setup_name"]
    config["general"]["chunk_number"] = config["general"]["next_chunk_number"]
    return config





########################################   END OF API ###############################################



def _called_from_tidy_job(config):
    """
    At the beginning of a prepare job, the date file isn't read yet,
    so run_number doesn't exist. At the end of a tidy job it does...
    Don't know if that is the best criterium to use. DB
    """
    if "general" in config:
        if "run_number" in config["general"]:
            return True
    return False



def _restore_original_config(config):
    if "general" in config:
        if "original_config" in config["general"]:
            resubmit = True
            return copy.deepcopy(config["general"]["original_config"]) #, resubmit
    resubmit = False
    #return config, resubmit
    return config


def _store_original_config(config):
    new_config = {}
    new_config={"original_config" : copy.deepcopy(config)}
    config["general"].update(new_config)
    return config


def _read_chunk_date_file_if_exists(config):
    config["general"]["chunk_date_file"] = (
            config["general"]["base_dir"] 
            + "/" 
            + config["general"]["expid"] 
            + "/scripts/" 
            + config["general"]["expid"] 
            + "_chunk_date"
            )

    if os.path.isfile(config["general"]["chunk_date_file"]):
        with open(config["general"]["chunk_date_file"], "r") as chunk_dates:
            chunk_number, setup_name = chunk_dates.read().split()

        config["general"]["setup_name"] = setup_name
        config["general"]["chunk_number"] = chunk_number

        index = 1

        while "model" + str(index) in config:
            if config["model" + str(index)]["setup_name"] == setup_name:
                config["general"]["this_chunk_size"] = (
                        config["model" + str(index)]["chunk_size"]
                        )
                break
            index += 1

    return config


def _initialize_chunk_date_file(config):
    config["general"]["setup_name"] = config["model1"]["setup_name"]
    config["general"]["chunk_number"] = 1
    config["general"]["this_chunk_size"] = (
            config["model1"]["chunk_size"]
            )
    return config


def _set_model_queue(config):
    index = 1
    model_queue = []
    model_named_queue = []

    while "model" + str(index) in config:
        model_queue += ["model" + str(index)]
        model_named_queue += [config["model" +  str(index)]["setup_name"]]
        index += 1

    index = model_named_queue.index(config["general"]["setup_name"]) + 1
    index = index % len(model_queue)

    config["general"]["model_queue"] = model_queue[index:] + model_queue[:index]
    config["general"]["model_named_queue"] = model_named_queue[index:] + model_named_queue[:index]

    return config

    
def _is_first_run_in_chunk(config):
    if config["general"]["run_number"] % config["general"]["this_chunk_size"] == 1:
        config["general"]["first_run_in_chunk"] = True
    else:
        config["general"]["first_run_in_chunk"] = False
    return config


def _is_last_run_in_chunk(config):
    if config["general"]["run_number"] % config["general"]["this_chunk_size"] == 0:
        config["general"]["last_run_in_chunk"] = True
    else:
        config["general"]["last_run_in_chunk"] = False
    return config


def _find_next_model_to_run(config):
    if config["general"]["last_run_in_chunk"]:
        config["general"]["next_model"] = config["general"]["model_queue"][0]
    else:
        config["general"]["next_model"] = config["general"]["setup_name"]
    return config


def _find_next_chunk_number(config):
    if config["general"]["last_run_in_chunk"]:
        config["general"]["next_chunk_number"] = config["general"]["chunk_number"] + 1
    else:
        config["general"]["next_chunk_number"] = config["general"]["chunk_number"] 
    return config

