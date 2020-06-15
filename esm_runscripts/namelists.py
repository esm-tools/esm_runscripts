import f90nml
import six


class namelist:

    @staticmethod
    def nmls_load(mconfig):
        import os
        import logging
        nmls = mconfig.get("namelists", [])
        mconfig["namelists"] = dict.fromkeys(nmls)
        for nml in nmls:
            if os.path.isfile( os.path.join(mconfig["thisrun_config_dir"], nml)):
                logging.debug("Loading %s", nml)
                mconfig["namelists"][nml] = f90nml.read(
                    os.path.join(mconfig["thisrun_config_dir"], nml)
                )
            else:
                mconfig["namelists"][nml] = f90nml.namelist.Namelist()
            if mconfig.get("namelist_case") == "uppercase":
                mconfig["namelists"][nml].uppercase = True
        return mconfig

    @staticmethod
    def nmls_remove(mconfig):
        import esm_parser
        import logging
        namelist_changes = mconfig.get("namelist_changes", {})
        namelist_removes = []
        esm_parser.pprint_config(namelist_changes)
        print(list(namelist_changes))
        for namelist in list(namelist_changes):
            changes = namelist_changes[namelist]
            esm_parser.pprint_config(changes)
            logging.debug("Determining remove entires for %s", namelist)
            logging.debug("All changes: %s", changes)
            for change_chapter in list(changes):
                change_entries = changes[change_chapter]
                for key in list(change_entries):
                    value = change_entries[key]
                    if value == "remove_from_namelist":
                        namelist_removes.append((namelist, change_chapter, key))
                        del namelist_changes[namelist][change_chapter][key]
        for remove in namelist_removes:
            namelist, change_chapter, key = remove
            logging.debug("Removing from %s: %s, %s", namelist, change_chapter, key)
            if key in mconfig["namelists"][namelist][change_chapter]:
                del mconfig["namelists"][namelist][change_chapter][key]
        return mconfig


    @staticmethod
    def nmls_modify(mconfig):
        import six
        namelist_changes = mconfig.get("namelist_changes", {})
        for namelist, changes in six.iteritems(namelist_changes):
            print(str(mconfig["namelists"][namelist]))
            mconfig["namelists"][namelist].patch(changes)
        return mconfig


    @staticmethod
    def nmls_finalize(mconfig):
        import os
        import sys
        import six
        all_nmls = {}
        for nml_name, nml_obj in six.iteritems(mconfig.get("namelists", {})):
            with open(os.path.join(mconfig["thisrun_config_dir"], nml_name), "w") as nml_file:
                nml_obj.write(nml_file)
            all_nmls[nml_name] = nml_obj  # PG or a string representation?
        six.print_(
            "\n" "- Namelists modified according to experiment specifications..."
        )
        for nml_name, nml in all_nmls.items():
            six.print_("Contents of ", nml_name, ":")
            nml.write(sys.stdout)
            six.print_("\n", 40 * "+ ")
        return mconfig

