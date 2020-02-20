# ESM Framework for organizing python code in plugins / yaml recipes

import sys
import esm_parser

def read_recipe(recipefile, additional_dict):
    # recipefile = esm_runscripts.yaml
    # additional_dict = {job_type: compute}

    recipe = esm_parser.yaml_file_to_dict(recipefile)
    recipe.update(additional_dict)
    esm_parser.basic_choose_blocks(recipe, recipe)
    esm_parser.recursive_run_function([], recipe, "atomic", esm_parser.find_variable, recipe, [], True)

    return recipe


def read_plugin_information(pluginfile, recipe):
    # pluginfile = esm_plugins.yaml
    extra_info = ["location", "git-url"]
    plugins = {}
    plugins_bare = esm_parser.yaml_file_to_dict(pluginfile)
    for workitem in recipe["recipe"]:
        found = False
        for module_type in ["core", "plugins"]:
            if module_type in plugins_bare:
                for module in plugins_bare[module_type]:
                    for submodule in plugins_bare[module_type][module]:
                        if submodule in extra_info:
                            continue
                        functionlist = plugins_bare[module_type][module][submodule]
                        if workitem in functionlist:
                            plugins[workitem] = {"module": module, 
                                                 "submodule": submodule,
                                                 "type": module_type
                                                 }
                            for extra in extra_info:
                                if extra in plugins_bare[module_type][module]:
                                    plugins[workitem].update({extra: plugins_bare[module_type][module][extra]})
                            found = True
                            break
                    if found:
                        break
                if found: 
                    break
            if found:
                break

    return plugins

def check_plugin_availability(plugins):
    something_missing = False
    for workitem in list(plugins.keys()):
        if plugins[workitem]["type"] == "core":
            pass
        else:
            print ("Checking if function " + plugins[workitem]["module"] + "." +
                    plugins[workitem]["submodule"] + "." + workitem + " can be imported...")
            try:
                if sys.version_info >= (3, 5):
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(plugins[workitem]["module"], plugins[workitem]["location"] + "/" + plugins[workitem]["module"] + ".py")
                    thismodule = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(thismodule)
            except:
                print ("Couldn't import " + plugins[workitem]["module"] + " from " + plugins[workitem]["location"]) 
                something_missing = True
    if something_missing:
        sys.exit(-1)


def work_through_recipe(recipe, plugins, config):
    for workitem in list(plugins.keys()):
        if plugins[workitem]["type"] == "core":
            thismodule = __import__(plugins[workitem]["module"])
            submodule = getattr(thismodule, plugins[workitem]["submodule"])
            config = getattr(submodule, workitem)(config)
        else:
            if sys.version_info >= (3, 5):
                import importlib.util
                spec = importlib.util.spec_from_file_location(plugins[workitem]["module"], plugins[workitem]["location"] + "/" + plugins[workitem]["module"] + ".py")
                thismodule = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(thismodule)
                config = getattr(thismodule, workitem)(config)
    return config


