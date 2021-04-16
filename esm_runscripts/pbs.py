"""
Contains functions for dealing with PBS-based batch systems
"""
import os
import subprocess
import sys
import esm_parser

class Pbs:
    """
    Deals with PBS, allowing you to check if a job is submitted, get the
    current job ID, generate the MOAB commands (e.g. ``aprun``), get the current job
    state, and check if a job is still running.


    Attributes
    ----------
    filename : str
        The filename for srun commands, defaults to ``hostfile_srun``
    path : str
        Full path to this file, defaults to ``thisrun_scripts_dir / filename``

    Parameters
    ----------
    config : dict
        The run configuration, needed to determine where the script directory
        for this particular run is.
    """
    def __init__(self, config):
        # No hostfile for PBS
        self.filename = ""
        self.path = ""

    @staticmethod
    def check_if_submitted():
        """
        Determines if a job is submitted in the currently running shell by
        checking for ``PBS_JOBID`` in the environment

        Returns
        -------
        bool
        """
        return "PBS_JOBID" in os.environ

    @staticmethod
    def get_jobid():
        """
        Gets the current PBS JOB ID

        Returns
        -------
        str or None
        """
        return os.environ.get("PBS_JOBID")

    @staticmethod
    def calc_launcher_flags(config, model):
        launcher = config["computer"]["launcher"]
        launcher_flags = config["computer"]["launcher_flags"]
        if "nproc" in config[model]:
            # aprun flags commented following the conventions in p. 14 of the ALEPH ppt
            # manual day_1.session_2.advanced_use_of_aprun.ppt

            # Total number of PEs (MPI-ranks) (aprun -n)
            nproc = config[model]["nproc"]
            # Cores per node
            cores_per_node = config['computer']['cores_per_node']
            # Define OMP threads if heterogeneous MPI-OMP
            if config["general"].get("taskset", False):
                omp_num_threads = config[model].get("omp_num_threads", 1)
            # Define OMP threads if only MPI
            else:
                omp_num_threads = 1
            # Number of nodes needed
            nodes = (
                int(nproc * omp_num_threads / cores_per_node)
                + ((nproc * omp_num_threads) % cores_per_node > 0)
            )
            # PEs (MPI-ranks) per compute node (aprun -N)
            nproc_per_node = int(nproc / nodes)
            # CPUs per MPI-rank (aprun -d)
            cpus_per_proc = config[model].get("cpus_per_proc", omp_num_threads)
        elif "nproca" in config[model] and "procb" in config[model]:
            esm_parser.user_error(
                "nproc", "nproca and nprocb not supported yet for pbs"
            )

        # Replace tags in the laucher flags
        replacement_tags = [
            ("@nproc@", nproc),
            ("@nproc_per_node@", nproc_per_node),
            ("@cpus_per_proc@", cpus_per_proc),
            ("@omp_num_threads@", omp_num_threads),
        ]
        # Replace all tags
        for (tag, repl) in replacement_tags:
            launcher_flags = launcher_flags.replace(tag, str(repl))

        return launcher_flags

    def calc_requirements(self, config):
        """
        Calculates requirements and writes them to ``self.path``.
        """
        # PBS does not support yet multi_apruns
        #if config['general'].get('multi_apruns'):
        #    self.calc_requirements_multi_aprun(config)
        #    return
        component_lines = []
        sep = config["computer"].get("launcher_comp_sep", "\\\n    ") + " "
        for model in config["general"]["valid_model_names"]:
            command = None
            if "execution_command" in config[model]:
                command = config[model]["execution_command"]
            elif "executable" in config[model]:
                command = config[model]["executable"]
            if command:
                launcher_flags = self.calc_launcher_flags(config, model)
                component_lines.append(
                    f'{launcher_flags} ./{command} '
                )
        components = sep.join(component_lines)
        config["computer"]["execution_command"] = (
            config["computer"]["execution_command"].replace("@components@", components)
        )

    @staticmethod
    def get_job_state(jobid):
        """
        Returns the jobstate full name. See ``man squeue``, section ``JOB STATE CODES`` for more details.

        Parameters
        ----------
        jobid :
            ``str`` or ``int``. The SLURM job id as displayed in, e.g. ``squeue``

        Returns
        -------
        str :
            The short job state.
        """
        state_command = f"qstat {str(jobid)}"

        qstat_output = subprocess.Popen(
            state_command.split(),
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
        ).communicate()[0]
        qstat_split = str(qstat_output).split()
        if len(qstat_split) > 2:
            return qstat_split[-3]

    @staticmethod
    def job_is_still_running(jobid):
        """Returns a boolean if the job is still running"""
        return bool(Pbs.get_job_state(jobid))
