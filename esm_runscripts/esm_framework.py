# ESM Framework for organizing python code in plugins / yaml recipes


import esm_parser

def read_recipe(recipefile, additional_dict)
    # recipefile = esm_runscripts.yaml
    # additional_dict = {job_type: compute}

    recipe = esm_parser.yaml_file_to_dict(recipefile)
    recipe.update(additional_dict)
    esm_parser.basic_choose_blocks(recipe, recipe)
    esm_parser.recursive_run_function([], recipe, "atomic", esm_parser.find_variable, recipe, [], True)

    return recipe


def read_plugin_info(plugin_file, recipe)
    # pluginfile = esm_plugins.yaml
    extra_info = ["location", "git-url"]
    plugins = {}
    plugins_bare = esm_parser.yaml_file_to_dict(pluginfile)
    for workitem in recipe["recipe"]:
        found = False
        for moduletype in ["core", "plugins"]
            if moduletype in plugins_bare:
                for module in plugins_bare[moduletype]:
                    for submodule in plugins_bare[module_type][module]:
                        if submodule in extra_info:
                            break
                        functionlist = plugins_bare[module_type][module][submodule]
                        if workitem in functionlist:
                            plugins[workitem] = {"module": module, 
                                                 "submodule": submodule,
                                                 "type": moduletype
                                                 }
                            for extra in extra_info:
                                if extra in plugins_bare[module_type][module]:
                                    plugins[workitem].append({extra: plugins_bare[module_type][module][extra]})
                            found = True
                            break
                    if found:
                        break
                if found: 
                    break
            if found:
                break

    return plugins



