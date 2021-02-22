def read_chunk_date_file(config):
    chunk_date_file = config["general"]["expid"] + "_chunks.date"

    with open(chunk_date_file, "r") as chunk_dates:
        chunk_number, setup_name, run_number, start_date = chunk_dates.read().split()

    config["general"]["setup_name"] = setup_name
    config["general"]["chunk_number"] = chunk_number
    return config



def write_chunk_date_file(config):
    chunk_date_file = config["general"]["expid"] + "_chunks.date"  
    with open(chunk_date_file, "x") as chunk_dates:
        chunk_dates.write(config["general"]["next_chunk_number"] + " " + config["general"]["next_setup_name"])
    return config



def pick_next_chunk(config):
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
        config["general"]["last_run_in_chunk"] = True
    else:
        config["general"]["last_run_in_chunk"] = False
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

