"""
Contains functions for dealing with SLURM-based batch systems
"""
import os
import subprocess
import sys

class Slurm:
    """
    Deals with SLURM, allowing you to check if a job is submitted, get the
    current job ID, generate a srun hostfile, get the current job state, and
    check if a job is still running.


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
        folder = config["general"]["thisrun_scripts_dir"]
        self.filename = "hostfile_srun"
        self.path = folder + "/" + self.filename

    @staticmethod
    def check_if_submitted():
        """
        Determines if a job is submitted in the currently running shell by
        checking for ``SLURM_JOB_ID`` in the environment

        Returns
        -------
        bool
        """
        return "SLURM_JOB_ID" in os.environ

    @staticmethod
    def get_jobid():
        """
        Gets the current SLURM JOB ID

        Returns
        -------
        str or None
        """
        return os.environ.get("SLURM_JOB_ID")

    def write_hostfile(self, config):
        """
        Gathers previously prepared requirements 
        (batch_system.calculate_requirements) and writes them to ``self.path``.
        """
        with open(self.path, "w") as hostfile:
            for model in config["general"]["valid_model_names"]:
                end_proc = config[model].get("end_proc", None)
                start_proc = config[model].get("start_proc", None)

                if not start_proc or not end_proc:
                    continue
                
                if "execution_command" in config[model]:
                    command = "./" + config[model]["execution_command"]
                elif "executable" in config[model]:
                    command = "./" + config[model]["executable"]
                else:
                    continue
                hostfile.write(str(start_proc) + "-" + str(end_proc) + "  " + command + "\n")


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
        state_command = ["squeue -j" + str(jobid) + ' -o "%T"']

        squeue_output = subprocess.Popen(state_command, stdout = subprocess.PIPE, stderr = subprocess.PIPE).communicate()[0]
        if len(squeue_output) == 2:
            return squeue_output[0]

    @staticmethod
    def job_is_still_running(jobid):
        """Returns a boolean if the job is still running"""
        return bool(Slurm.get_job_state(jobid))
