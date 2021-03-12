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
    hostlist : str
        The hostlist for srun commands, defaults to ``hostlist``
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
        self.folder = folder + "/" 

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

    def calc_requirements_multi_srun(self, config):
        print("Paul was here...")
        for run_type in list(config['general']['multi_srun']):
            current_hostfile = self.path+"_"+run_type
            print(f"Writing to: {current_hostfile}")
            start_proc = 0
            end_proc = 0
            with open(current_hostfile, "w") as hostfile:
                for model in config['general']['multi_srun'][run_type]['models']:
                    start_proc, end_proc = self.mini_calc_reqs(config, model, hostfile, start_proc, end_proc)
            config['general']['multi_srun'][run_type]['hostfile'] = os.path.basename(current_hostfile)


    @staticmethod
    def mini_calc_reqs(self,config, model, start_proc, end_proc):
        if "nproc" in config[model]:
            if "omp_num_proc" in config[model]:
                end_proc = start_proc + int(config[model]["nproc"])*int(config[model]["omp_num_proc"]) - 1
            else:
                end_proc = start_proc + int(config[model]["nproc"]) - 1
        elif "nproca" in config[model] and "nprocb" in config[model]:
            end_proc = start_proc + int(config[model]["nproca"])*int(config[model]["nprocb"]) - 1

            # KH 30.04.20: nprocrad is replaced by more flexible
            # partitioning using nprocar and nprocbr
            if "nprocar" in config[model] and "nprocbr" in config[model]:
                if config[model]["nprocar"] != "remove_from_namelist" and config[model]["nprocbr"] != "remove_from_namelist":
                    end_proc += config[model]["nprocar"] * config[model]["nprocbr"]

        else:
            return start_proc, end_proc
        if "execution_command" in config[model]:
            command = "./" + config[model]["execution_command"]
        elif "executable" in config[model]:
            command = "./" + config[model]["executable"]
        else:
            return start_proc, end_proc

        if "taskset" in config["general"]:
            scriptname="script_"+model+".ksh"
            with open(self.folder+scriptname, "w") as f:
                f.write("#!/bin/ksh"+"\n")
                f.write("export OMP_NUM_THREADS=$(("+str(config[model]["omp_num_proc"])+"))"+"n")
                f.write(command+"\n")

            progname="prog_"+model+".sh"
            print(progname)
            import pdb
            pdb.set_trace()
            with open(self.folder+progname, "w") as f:
                f.write("#!/bin/sh"+"\n")
                f.write("(( init = "+str(end_proc)+" + \$1 ))"+"\n")
                f.write("(( index = init * "+str(config[model]["omp_num_proc"])+")) ))"+"\n")
                #import pdb
                #pdb.set_trace()
                f.write("(( slot = index % "+str(config["computer"]["cores_per_node"])+"))"+"\n")  #TODO: We need this line, no the one below. But this line fails
                f.write("echo "+model+" taskset -c \$slot\"-\"\$((slot + "+str(config[model]["omp_num_proc"])+" - 1"+"\n")
                f.write("taskset -c \$slot\"-\"\$((slot + "+str(config[model]["omp_num_proc"])+")) - 1)) ./script_"+model+".ksh"+"\n")


        with open(self.path, "a") as hostfile:
            hostfile.write(str(start_proc) + "-" + str(end_proc) + "  " + command + "\n")
            start_proc = end_proc + 1
        return start_proc, end_proc


    def calc_requirements(self, config):
        """
        Calculates requirements and writes them to ``self.path``.
        """
        if config['general'].get('multi_srun'):
            self.calc_requirements_multi_srun(config)
            return
        start_proc = 0
        end_proc = 0 
        for model in config["general"]["valid_model_names"]:
            start_proc, end_proc = self.mini_calc_reqs(self,config, model, start_proc, end_proc)


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
