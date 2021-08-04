"""
``esm-runscripts`` Core Plugins for dealing with Fortran Namelists.

Provides plugins for loading, modifying, deleting, and writing Fortran
Namelists as part of the ``esm-runscripts`` recipe. All plugins are found under
the class Namelist as static methods. A deprecated class ``namelist`` (small "n") is
provided, which warns you when it is used.
"""
import logging
import os
import sys
import warnings

import f90nml
import six


class Namelist:
    """Methods for dealing with FORTRAN namelists"""

    @staticmethod
    def nmls_load(mconfig):
        """
        Loads Fortran namelists into the configuration dictionary.

        User Information
        ----------------
        To associate namelists with a specific model, you should have a section
        in your configuration that lists the namelists::

            fesom:
                namelists:
                    - "namelist.config"
                    - "namelist.oce"
                    - "namelist.ice"
                    - "namelist.diag"

        Programmer Information
        ----------------------
        The namelists are represented by f90nml Namelist objects, and are
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
        """
        Removes an element from a namelist chapter.

        User Information
        ----------------
        In the configuration file, assume you have::

            echam:
                namelist_changes:
                    namelist.echam:
                        radctl:
                            co2vmr: "remove_from_namelist"

        In this case, the entry co2vmr would be deleted from the radctl section
        of namelist.echam.

        Programmer Information
        ----------------------
        IDEA(PG): Maybe we can provide examples of how these functions are used
        in the code?

        Parameters
        ----------
        mconfig : dict
            The model (e.g. ECHAM, FESOM, NEMO or OIFS) configuration

        Returns
        -------
        mconfig : dict
            The modified configuration.
        """
        namelist_changes = mconfig.get("namelist_changes", {})
        namelist_removes = []
        for namelist in list(namelist_changes):
            changes = namelist_changes[namelist]
            logging.debug("Determining remove entires for %s", namelist)
            logging.debug("All changes: %s", changes)
            for change_chapter in list(changes):
                change_entries = changes[change_chapter]
                for key in list(change_entries):
                    value = change_entries[key]
                    if value == "remove_from_namelist":
                        namelist_removes.append((namelist, change_chapter, key))

                        # the key is probably coming from esm_tools config
                        # files or from a user runscript. It can contain lower
                        # case, but the original Fortran namelist could be in
                        # any case combination. Here `original_key` is coming
                        # from the default namelist and may contain mixed case.
                        # `key` is the processed variable from f90nml module and
                        # is lowercase.
                        remove_original_key = False

                        # traverse the namelist chapter and see if a mixed case
                        # variable is also found
                        for key2 in namelist_changes[namelist][change_chapter]:
                            # take care of the MiXeD FORTRAN CaSeS
                            if key2.lower() == key.lower() and key2 != key:
                                original_key = key2
                                remove_original_key = True
                                namelist_removes.append((namelist, change_chapter, original_key))

                        # remove both lowercase and mixed case variables
                        del namelist_changes[namelist][change_chapter][key]
                        if remove_original_key:
                            del namelist_changes[namelist][change_chapter][original_key]

                        # mconfig instead of config, Grrrrr
                        print(f"- NOTE: removing the variable: {key} from the namelist: {namelist}")

        for remove in namelist_removes:
            namelist, change_chapter, key = remove
            logging.debug("Removing from %s: %s, %s", namelist, change_chapter, key)
            if key in mconfig["namelists"][namelist][change_chapter]:
                del mconfig["namelists"][namelist][change_chapter][key]
        return mconfig

    @staticmethod
    def nmls_modify(mconfig):
        """
        Performs namelist changes.

        User Information
        ----------------
        In the configuration file, you should have a section as::

            echam:
                namelist_changes:
                    namelist.echam:
                        radctl:
                            co2vmr: 1200e-6


        This would change the value of the echam namelist (namelist.echam),
        subsection radctl, entry co2vmr to the value 1200e-6.

        Programmer Information
        ----------------------
        IDEA(PG): Maybe we can provide examples of how these functions are used
        in the code?

        Note
        ----
        Actual changes are performed by the f90nml package patch fuction. See
        here: https://tinyurl.com/y4ydz363

        Parameters
        ----------
        mconfig : dict
            The model (e.g. ECHAM, FESOM, NEMO or OIFS) configuration

        Returns
        -------
        mconfig : dict
            The modified configuration.
        """
        namelist_changes = mconfig.get("namelist_changes", {})
        for namelist, changes in six.iteritems(namelist_changes):
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
            nml = config["echam"]["namelists"]["namelist.echam"]
            # Get the current dynctl chapter or make a new empty one:
            dynctl = nml.get("dynctl", f90nml.namelist.Namelist())
            # Determine which years the user wants to have disturbed:
            if os.path.isfile(
                config["general"]["experiment_scripts_dir"] + "/disturb_years.dat"
            ):
                with open(
                    config["general"]["experiment_scripts_dir"] + "/disturb_years.dat"
                ) as f:
                    disturbance_file = [
                        int(line.strip()) for line in f.readlines() if line.strip()
                    ]
                if config["general"]["verbose"]:
                    print(disturbance_file)
            else:
                disturbance_file = None
                if config["general"]["verbose"]:
                    print("WARNING: "
                        + config["general"]["experiment_scripts_dir"]
                        + "/disturb_years.dat",
                        "was not found",
                    )
            disturbance_years = disturbance_file or config["echam"].get(
                "disturbance_years", []
            )
            current_year = config["general"]["current_date"].year
            if current_year in disturbance_years:
                print("-------------------------------------------------------")
                print("")
                print("              > Applying disturbance in echam namelist!")
                print("")
                print("-------------------------------------------------------")
                dynctl["enstdif"] = config["echam"].get("disturbance", 1.000001)
                nml["dynctl"] = dynctl
            else:
                if config["general"]["verbose"]:
                    print("Not applying disturbance in echam namelist.")
                    print(
                        "Current year",
                        current_year,
                        "disturbance_years",
                        disturbance_years,
                    )
        return config

    @staticmethod
    def echam_determine_streams_from_nml(config):
        if "echam" in config["general"]["valid_model_names"]:
            nml = config["echam"]["namelists"]["namelist.echam"]
            mvstreams = nml["mvstreamctl"]
            mvstreams_tags = [nml.get("filetag") for nml in mvstreams]
            # NOTE(PG): There may still be warnings about missing files -- we
            # still need to implement an "allowed missing files" feature, but
            # this should now add all of the mvstreams that have a filetag to
            # ECHAM's stream list.
            if not config["echam"].get("override_streams_from_namelist", False):
                config["echam"]["streams"] += mvstreams_tags
            else:
                # NOTE(PG): I honestly am not sure if this will work, maybe the
                # restart will get messed up horribly. This just overrides
                # whatever was there in the default. It might be dangerous.
                config["echam"]["streams"] = mvstreams_tags
            # As reference from the echam YAML:
            # outdata_files:
            #   "[[streams-->STREAM]]": STREAM
            #   "[[streams-->STREAM]]_codes": STREAM_codes
            #   "[[streamsnc-->STREAM]]_nc": STREAM_nc
            #
            # outdata_sources:
            #       "[[streams-->STREAM]]": ${general.expid}_${start_date!syear}*.${start_date!sday}_STREAM
            #       "[[streams-->STREAM]]_codes": ${general.expid}_${start_date!syear}*.${start_date!sday}_STREAM.codes
            #       "[[streamsnc-->STREAM]]_nc": ${general.expid}_${start_date!syear!smonth}.${start_date!sday}_STREAM.nc
            for stream in config['echam']['streams']:
                config['echam']['outdata_files'][f'{stream}_codes'][stream] = f'{stream}_codes'
                config['echam']['outdata_files'][f'{stream}_nc'][stream] = f'{stream}_nc'
                config['echam']['outdata_sources'][f'{stream}'] = config['general']['expid']+"_*_"+stream
                config['echam']['outdata_sources'][f'{stream}_codes'] = config['general']['expid']+"_*_"+stream+".codes"
                config['echam']['outdata_sources'][f'{stream}_nc'] = config['general']['expid']+"_*_"+stream+".nc"
            print("Hey bro, we need some debugging:")
            print("We write the config as it is right now to the current working directory as >> stream_config.yaml <<")
            import yaml
            with open("stream_config.yaml", "w") as f:
                yaml.dump(config, f)
            print(config['echam']['outdata_files'])
            print(config['echam']['outdata_sources'])
        return config

    @staticmethod
    def nmls_finalize(mconfig, verbose):
        """
        Writes namelists to disk after all modifications have finished.

        User Information
        ----------------
        Part of the main log output will be a section specifing the actual
        namelists that have been used for your simulation, including all
        relevant additions, removals, or changes.

        Programmer Information
        ----------------------
        A copy of the f90nml object representations of the namelists is stored
        under the dictionary key "namelist_objs", as a dictionary of
        ("namelist_name", f90nml_objfect) key/value pairs.

        Warning
        -------
        Removing this step from your recipe might result in a broken run,
        as the namelists will not be present in their desired form! Even if
        your model runs, it might not contain all user-required changes.

        Parameters
        ----------
        mconfig : dict
            The model (e.g. ECHAM, FESOM, NEMO or OIFS) configuration

        Returns
        -------
        mconfig : dict
            The modified configuration.
        """
        all_nmls = {}

        for nml_name, nml_obj in six.iteritems(mconfig.get("namelists", {})):
            with open(
                os.path.join(mconfig["thisrun_config_dir"], nml_name), "w"
            ) as nml_file:
                nml_obj.write(nml_file)
            all_nmls[nml_name] = nml_obj  # PG: or a string representation?
        mconfig["namelist_objs"] = all_nmls
        if verbose:
            mconfig = Namelist.nmls_output(mconfig)
        return mconfig

    @staticmethod
    def nmls_output(mconfig):
        all_nmls = {}

        for nml_name, nml_obj in six.iteritems(mconfig.get("namelists", {})):
            all_nmls[nml_name] = nml_obj  # PG: or a string representation?
        for nml_name, nml in all_nmls.items():
            message = f'\nFinal Contents of {nml_name}:'
            six.print_(message)
            six.print_(len(message) * '-')
            nml.write(sys.stdout)
            print('-' * 80)
            print(f'::: end of the contents of {nml_name}\n')
        return mconfig


    @staticmethod
    def nmls_output_all(config):
        six.print_(
            "\n" "- Namelists modified according to experiment specifications..."
        )
        for model in config["general"]["valid_model_names"]:
            config[model] = nmls_output(config[model], config["general"]["verbose"])
        return config


class namelist(Namelist):
    """Legacy class name. Please use Namelist instead!"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "Please change your code to use Namelist!",
            DeprecationWarning,
            stacklevel=2,
        )


        super(namelist, self).__init__(*args, **kwargs)
