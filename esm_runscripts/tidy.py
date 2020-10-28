from . import helpers

def run_job(config):
    config["general"]["relevant_filtypes"] = ["log", "mon", "outdata", "restart_out","bin", "config", "forcing", "input", "restart_in", "ignore"]
    helpers.evaluate(config, "tidy", "tidy_recipe")
    return config
