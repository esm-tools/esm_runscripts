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
                        del namelist_changes[namelist][change_chapter][key]
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
    def nmls_finalize(mconfig):
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
        six.print_(
            "\n" "- Namelists modified according to experiment specifications..."
        )
        for nml_name, nml in all_nmls.items():
            six.print_("Contents of ", nml_name, ":")
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
