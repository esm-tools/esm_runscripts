import esm_parser
import f90nml
import logging
import os
import six
import sys
import warnings


class Namelist:
    """Methods for dealing with FORTRAN namelists"""
    @staticmethod
    def nmls_load(mconfig):
        """
        Loads Fortran namelists into the configuration dictionary. The
        namelists are represented by f90nml Namelist objects, and are
        stored under::

            mconfig["namelists"]["namelist.echam"]``

        This would point to the ECHAM namelist as a f90nml object, which
        closely resembles a dictionary.

        The actual namelists to load are listed in the raw configuration as a
        list of strings::

            mconfig['namelists'] = ['nml1', 'nml2', 'nml3', ...]

        Namelists are assumed to have been copied to
        ``mconfig["thisrun_config_dir"]``, and are loaded from there.

        If the ``mconfig`` has a key ``"namelist_case"`` equal to "uppercase",
        the uppercase attribute of the f90nml representation of the namelist is
        set to ``True``.

        Parameters
        ----------
        mconfig : dict
            The model (e.g. ECHAM, FESOM, NEMO or OIFS) configuration

        Returns
        -------
        mconfig : dict
            The modified configuration.
        """
        nmls = mconfig.get("namelists", [])
        mconfig["namelists"] = dict.fromkeys(nmls)
        for nml in nmls:
            if os.path.isfile(os.path.join(mconfig["thisrun_config_dir"], nml)):
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
        namelist_changes = mconfig.get("namelist_changes", {})
        namelist_removes = []
        #esm_parser.pprint_config(namelist_changes)
        #print(list(namelist_changes))
        for namelist in list(namelist_changes):
            changes = namelist_changes[namelist]
            #esm_parser.pprint_config(changes)
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
        namelist_changes = mconfig.get("namelist_changes", {})
        for namelist, changes in six.iteritems(namelist_changes):
            #print(str(mconfig["namelists"][namelist]))
            mconfig["namelists"][namelist].patch(changes)
        return mconfig


    @staticmethod
    def apply_echam_disturbance(config):
        """
        Applies a disturbance to the DYNCTL chapter of the echam namelist via the enstdif

        Relevant configuration entries:
        * disturbance_years (list of int): Which year to apply the disturbance
        * distrubance (float): Value to apply. Default can be found in echam.yaml
        """
        if "echam" in config["general"]["valid_model_names"]:
            # Get the echam namelist:
            nml = config['echam']['namelists']['namelist.echam']
            # Get the current dynctl chapter or make a new empty one:
            dynctl = nml.get("dynctl", f90nml.namelist.Namelist())
            # Determine which years the user wants to have disturbed:
            disturbance_years = config["echam"].get("disturbance_years", [])
            current_year = config['general']['current_date'].year
            if current_year in disturbance_years:
                dynctl['enstdif'] = config['echam']['disturbance']
                nml['dynctl'] = dynctl
        return config

    @staticmethod
    def nmls_finalize(mconfig):
        all_nmls = {}

        for nml_name, nml_obj in six.iteritems(mconfig.get("namelists", {})):
            with open(os.path.join(mconfig["thisrun_config_dir"], nml_name), "w") as nml_file:
                nml_obj.write(nml_file)
            all_nmls[nml_name] = nml_obj  # PG: or a string representation?
        six.print_(
            "\n" "- Namelists modified according to experiment specifications..."
        )
        for nml_name, nml in all_nmls.items():
            six.print_("Final Contents of ", nml_name, ":")
            nml.write(sys.stdout)
            six.print_("\n", 40 * "+ ")
        return mconfig


class namelist(Namelist):
    """Legacy class name. Please use Namelist instead!"""
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "Please change your code to use Namelist!",
            DeprecationWarning,
            stacklevel=2,
        )
        super(namelist, self).__init__(*args, **kwargs)
