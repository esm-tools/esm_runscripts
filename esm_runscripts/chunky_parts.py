import os, copy
import yaml
import esm_parser



def setup_correct_chunk(chunk_config, resubmit = False):
    chunk_config = initialize_chunk_date_file(chunk_config) # make sure file exists and points to NEXT run
    if resubmit:
        chunk_config = update_chunk_date_file(chunk_config)

    chunk_config = read_chunk_date_file(chunk_config)
    chunk_config = set_model_queue(chunk_config)
    chunk_config = is_first_run_in_chunk(chunk_config)
    chunk_config = is_last_run_in_chunk(chunk_config)
    chunk_config = find_next_model_to_run(chunk_config)
    chunk_config = find_next_chunk_number(chunk_config)
    config = remove_unnecessary_stuff(chunk_config)
    config = merge_in_setup_config(config)

    return config



def merge_in_setup_config(config):
    # at this point, config should only have a general section
    model_config = yaml read...

    model_config = esm_parser.deep_update(config, model_config)
    return model_config


def remove_unnecessary_stuff(config):
    for model in config["general"]["model_queue"]:
        del config[model]
    return config


def read_chunk_date_file(config):
    with open(config["general"]["chunk_date_file"], "r") as chunk_dates:
        chunk_number, setup_name, run_number, start_date = chunk_dates.read().split()

    config["general"]["setup_name"] = setup_name
    config["general"]["chunk_number"] = chunk_number
    return config



def initialize_chunk_date_file(config):
    chunk_date_file = config["general"]["expid"] + "_chunks.date"
    config["general"]["chunk_date_file"] = chunk_date_file
    if not os.path.isfile(chunk_date_file):
        with open(chunk_date_file, "x") as chunk_dates:
            chunk_dates.write("1 " + config["general"]["model1"]["setup_name"])
    return config


def update_chunk_date_file(config):
    with open(config["general"]["chunk_date_file"], "x") as chunk_dates:
        chunk_dates.write(config["general"]["next_chunk_number"] + " " + config["general"]["next_setup_name"])
    return config


def set_model_queue(config):
    index = 1
    model_queue = []

    while "model" + str(index) in config:
        model_queue += "model" + str(index)
        model_named_queue += config["model" +  str(index)]["setup_name"]
        index += 1

    index = model_named_queue.index(config["general"]["setup_name"]) + 1
    index = index % len[model_queue]

    config["general"]["model_queue"] = model_queue[index:] + model_queue[:index]
    config["general"]["model_named_queue"] = model_named_queue[index:] + model_named_queue[:index]

    return config

    

def is_first_run_in_chunk(config):
    if config["general"]["run_number"] % config["general"]["this_chunk_size"] == 1:
        config["general"]["first_run_in_chunk"] = True
    else:
        config["general"]["first_run_in_chunk"] = False
    return config


def is_last_run_in_chunk(config):
    if config["general"]["run_number"] % config["general"]["this_chunk_size"] == 0:
        config["general"]["last_run_in_chunk"] = True
    else:
        config["general"]["last_run_in_chunk"] = False
    return config


def find_next_model_to_run(config):
    if config["general"]["last_run_in_chunk"]:
        config["general"]["next_model"] = config["general"]["model_queue"][0]
    else:
        config["general"]["next_model"] = config["general"]["setup_name"]
    return config


def find_next_chunk_number(config):
    if config["general"]["last_run_in_chunk"]:
        config["general"]["next_chunk_number"] = config["general"]["chunk_number"] + 1
    else:
        config["general"]["next_chunk_number"] = config["general"]["chunk_number"] 
    return config

