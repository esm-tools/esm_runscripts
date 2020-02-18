import sys
known_batch_systems = ["slurm"]

class esm_batch_system:
    def __init__(self, config, name):
        self.name = name
        if name == "slurm":
            import slurm
            self.bs = slurm.slurm(config)
        else:
            print ("Unknown batch system: ", name)
            sys.exit(1)

    def check_if_submitted(self):
        return self.bs.check_if_submitted()

    def get_jobid(self):
        return self.bs.get_jobid()

    def calc_requirements(self, config):
        return self.bs.calc_requirements(config)

    def get_job_state(self, jobid):
        return self.bs.get_job_state(jobid)

    def job_is_still_running(self, jobid):
        return self.bs.job_is_still_running(jobid)
