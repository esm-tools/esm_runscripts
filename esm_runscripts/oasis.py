import sys

class oasis:
    def __init__(self, nb_of_couplings = 1, coupled_execs = ["echam", "fesom"], runtime = 1, debug_level=1,
	              nnorest="F", mct_version="4.0", lucia=False):
        if isinstance(mct_version, tuple):
            pass
        elif isinstance(mct_version, int):
            mct_version = (mct_version,0) 
        elif isinstance(mct_version, float):
            mct_version = tuple(int(x) for x in str(mct_version).split("."))
        # PG: Fixup version to be tuples:
        elif isinstance(mct_version, str):
            mct_version = tuple(int(x) for x in mct_version.split("."))
        else:
            print("Init of Oasis needs the argument mct_version to be either a tuple or a string!")
            sys.exit(1)
        self.namcouple = ["# This namcouple was automatically generated by the esm-tools (Python)"]
        self.namcouple += [" $NFIELDS", "            " + str(nb_of_couplings), " $END"]
        exec_entry = ""
        for exe in coupled_execs:
            exec_entry = exec_entry + " " + exe
        exec_entry = str(len(coupled_execs)) + exec_entry
        self.namcouple += [" $NBMODEL", "            " + str(exec_entry)," $END"]
        self.namcouple += [" $RUNTIME", "           " + str(runtime), " $END"]
        # seb-wahl: add lucia support 
        if lucia:
            self.namcouple += [" $NLOGPRT", "           " + "1 -1", " $END"]
        else:
            self.namcouple += [" $NLOGPRT", "           " + str(debug_level), " $END"]
        if mct_version >= (4, 0):
            # If true, OASIS can start without restart files
            self.namcouple += [" $NNOREST", "           " + str(nnorest), " $END "]
        self.namcouple += [" $STRINGS"]
        self.namcouple += ["###############################################################################"]
        self.namcouple += ["###############################################################################"]
        self.next_coupling=1
        self.name = "oasis3mct"

    def add_input_coupling(self, field_name, freq, field_filepath):
        self.namcouple += ["#"]
        nb = self.next_coupling
        self.namcouple += [f"{field_name} {field_name} {nb} {freq} 0 {field_filepath} INPUT"]
        self.namcouple += "#"
        self.next_coupling += 1

    def add_coupling(self, lefts, lgrid, rights, rgrid, direction, transformation, restart_file, time_step, lresume):
        import sys
        self.namcouple += ["#"]

        nb = self.next_coupling

        left = sep = ""
        for lefty in lefts:

            restart_out_file = lefty + "_"

            left += sep + lefty
            self.next_coupling += 1
            sep = ":"

        right = sep = ""
        for righty in rights:
            right += sep + righty
            sep = ":" 

        if lresume == False:
            lag = str(0)
            export_mode = "EXPOUT"
        else:
            lag = direction.get("lag", "0")
            export_mode = "EXPORTED"
         
        # if a transformation method for CONSERV (e.g. GLOBAL) is set below, 
        # increase seq (=number of lines describing the transformation) by 1
        seq = int(direction.get("seq", "2"))
        if transformation.get("postprocessing", {}).get("conserv", {}).get("method"):
            seq += 1

        self.namcouple += [right + " " + left + " " + str(nb) + " " + str(time_step) + " " + str(seq) + " " + str(restart_file) + " " + export_mode]
        if lgrid and rgrid:
            self.namcouple += [str(rgrid["nx"]) + " " + str(rgrid["ny"]) + " " + str(lgrid["nx"]) + " " + str(lgrid["ny"]) + " " + rgrid["name"] + " " + lgrid["name"] + " LAG=" + str(lag)]
        
        p_rgrid = p_lgrid = "0"
        if "number_of_overlapping_points" in rgrid:
            p_rgrid = str(rgrid["number_of_overlapping_points"])
        if "number_of_overlapping_points" in lgrid:
            p_lgrid = str(lgrid["number_of_overlapping_points"])

        self.namcouple += ["P " + p_rgrid +" P " +  p_lgrid]

        trafo_line = ""
        trafo_details = []

        alltimes = transformation.get("time_transformation", "bla")
        if not type(alltimes) == list:
            alltimes = [alltimes]
        for time in alltimes:
            detail_line = ""
            if time.lower() in ["instant", "accumul", "average", "t_min", "t_max"]:
                trafo_line = "LOCTRANS"
                detail_line = time.upper()
                trafo_details.append(detail_line.strip())


        allpres=transformation.get("preprocessing", "bla")
        if not type(allpres) == list:
            allpres = [allpres]
        for pre in allpres:
            detail_line = ""
            if type(pre) == dict:
                pre = list(pre.keys())[0]
            if pre.lower() == "checkin":
                trafo_line += " CHECKIN"
                detail_line = "INT = 1"
                trafo_details.append(detail_line.strip())
            elif pre.lower() == "blasold":
                trafo_line += " BLASOLD"
                coefficient = transformation["preprocessing"][pre].get("xmult", None)
                if not coefficient:
                    print ("xmult needs to be defined for preprocessing BLASOLD")
                    sys.exit(2)
                add_scalar = transformation["preprocessing"][pre].get("add_scalar", None)
                if not add_scalar:
                    print ("add_scalar needs to be defined (0 or 1) for preprocessing BLASOLD")
                    sys.exit(2)
                detail_line = str(coefficient)  + " " + str(add_scalar)
                trafo_details.append(detail_line.strip())
                if str(add_scalar) == "1":
                    scalar_to_add = transformation["preprocessing"][pre].get("scalar_to_add", None)
                    if not add_scalar:
                        print ("scalar_to_add needs to be defined if add_scalar is set to  1 for preprocessing BLASOLD")
                        sys.exit(2)
                    detail_line = " CONSTANT" + str(add_scalar)
                    trafo_details.append(detail_line.strip())

        alltrans = transformation.get("remapping", {"bla": "blub"})
        if not type(alltrans) == list:
            alltrans = [alltrans]
        for thistrans in alltrans:
            (trans, transform) = list(thistrans.items())[0]
            detail_line = ""
            if "mapping" == trans.lower():
                trafo_line += " MAPPING"
                mapname = transform.get("mapname", None)
                if not mapname:
                    print ("mapname needs to be defined for transformation MAPPING")
                    sys.exit(2)
                maploc = transform.get("map_regrid_on", "")
                mapstrat = transform.get("mapstrategy", "")
                detail_line = mapname + " " + maploc + " " + mapstrat
                trafo_details.append(detail_line.strip())

            elif trans.lower() in ["distwgt", "bicubic", "bilinear", "gauswgt", "conserv"]:
                trafo_line += " SCRIPR"
                srcgridtype = str(rgrid["oasis_grid_type"]).upper()
                search_bin = transform.get("search_bin", None)
                if not search_bin:
                    print ("search_bin (LATITUDE or LATLON) needs to be defined for transformations DISTWGT, GAUSWGT, BILINEAR, BICUBIC")
                    sys.exit(2)
                bins = transform.get("nb_of_search_bins", "1")
                detail_line = trans.upper() + " " + srcgridtype.upper() + " SCALAR " + search_bin.upper() + " " + str(bins)
                if trans.lower() in ["distwgt", "gauswgt"]:
                    nb_of_neighbours = transform.get("nb_of_neighbours", None)
                    if not nb_of_neighbours:
                        print ("nb_of_neighbours needs to be defined for transformations DISTWGT and GAUSWGT")
                        sys.exit(2)
                    detail_line += " " + str(nb_of_neighbours)
                if trans.lower() == "gauswgt":
                    weight = transform.get("weight", None)
                    if not weight:
                        print ("weight needs to be defined for transformation GAUSWGT")
                        sys.exit(2)
                    detail_line += " " + str(weight)
                if trans.lower() == "conserv":
                    normalization = transform.get("normalization", None)
                    if not normalization:
                        print ("normalization (FRACAREA, DESTAREA or FRACNNEI) needs to be defined for transformations CONSERV")
                        sys.exit(2)
                    order = transform.get("order", None)
                    if not order:
                        print ("order (FIRST or SECOND) needs to be defined for transformation CONSERV")
                        sys.exit(2)
                    detail_line += " " + normalization.upper() + " " + order.upper()
                trafo_details += [detail_line.strip()]
            

        allpost = transformation.get("postprocessing", "bla")
        if not type(allpost) == list:
            allpost = list(allpost)
        for post in allpost:
            detail_line = ""
            if post.lower() == "conserv":
                trafo_line += " CONSERV"
                method = transformation["postprocessing"][post].get("method", None)
                if not method:
                    print (" a method (GLOBAL, GLBPOS, BASBAL or BASPOS) needs to be defined for postprocessing CONSERV")
                    sys.exit(2)
                algorithm = transformation["postprocessing"][post].get("algorithm", "")
                detail_line = method.upper() + " " + algorithm.upper()
                trafo_details.append(detail_line.strip())
            elif post.lower() == "checkout":
                trafo_line += " CHECKOUT"
                detail_line = "INT = 1"
                trafo_details.append(detail_line.strip())
            elif post.lower() == "blasnew":
                trafo_line += " BLASNEW"
                coefficient = transformation["postprocessing"][post].get("xmult", None)
                if not coefficient:
                    print ("xmult needs to be defined for postprocessing BLASNEW")
                    sys.exit(2)
                add_scalar = transformation["postprocessing"][post].get("add_scalar", None)
                if not add_scalar:
                    print ("add_scalar needs to be defined (0 or 1) for preprocessing BLASOLD")
                    sys.exit(2)
                detail_line = str(coefficient)  + " " + str(add_scalar)
                trafo_detail.append(detail_line.strip())
                if str(add_scalar) == "1":
                    scalar_to_add = transformation["postprocessing"][post].get("scalar_to_add", None)
                    if not add_scalar:
                        print ("scalar_to_add needs to be defined if add_scalar is set to  1 for postprocessing BLASNEW")
                        sys.exit(2)
                    detail_line = " CONSTANT" + str(add_scalar)
                    trafo_details.append(detail_line.strip())

        self.namcouple += [trafo_line]
        for line in trafo_details:
            self.namcouple += [line]

        self.namcouple += ["#"]
        self.namcouple += ["#"]
        self.namcouple += ["#"]
        self.namcouple += ["###############################################################################"]




    def print_config_files(self):
        for line in self.namcouple:
            print (line)
        

    def add_output_file(self, lefts, rights, leftmodel, rightmodel, config):
        out_file = []

        coupling = self.next_coupling

        if self.next_coupling < 10:
            this_coupling = "0" + str(coupling)
        else:
            this_coupling = str(coupling)

        for lefty in lefts:
            out_file.append(lefty + "_" + leftmodel + "_" + this_coupling + ".nc")
        for righty in rights:
            out_file.append(righty + "_" + rightmodel + "_" + this_coupling + ".nc") 
    
        self.next_coupling += 1

        if not "outdata_files" in config:
            config["outdata_files"] = {}
        if not "outdata_in_work" in config:
            config["outdata_in_work"] = {}
        if not "outdata_sources" in config:
            config["outdata_sources"] = {}
        
        for thisfile in out_file:

            config["outdata_files"][thisfile] = thisfile
            config["outdata_in_work"][thisfile] = thisfile
            config["outdata_sources"][thisfile] = thisfile


    def add_restart_files(self, restart_file, fconfig):
        config = fconfig[self.name]
        gconfig = fconfig["general"]
        #enddate = "_" + str(gconfig["end_date"].year) + str(gconfig["end_date"].month) + str(gconfig["end_date"].day)
        #parentdate = "_" + str(config["parent_date"].year) + str(config["parent_date"].month) + str(config["parent_date"].day)
        enddate = "_" + gconfig["end_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
        parentdate = "_" + config["parent_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
 

        if not "restart_out_files" in config:
            config["restart_out_files"] = {}
        if not "restart_out_in_work" in config:
            config["restart_out_in_work"] = {}
        if not "restart_out_sources" in config:
            config["restart_out_sources"] = {}

        if not "restart_in_files" in config:
            config["restart_in_files"] = {}
        if not "restart_in_in_work" in config:
            config["restart_in_in_work"] = {}
        if not "restart_in_sources" in config:
            config["restart_in_sources"] = {}

        config["restart_out_files"][restart_file] = restart_file        
        config["restart_out_files"][restart_file + "_recv"] = restart_file + "_recv"

        config["restart_out_in_work"][restart_file] = restart_file #+ enddate
        config["restart_out_in_work"][restart_file + "_recv"] = restart_file + "_recv" #+ enddate

        config["restart_out_sources"][restart_file] = restart_file
        config["restart_out_sources"][restart_file + "_recv"] = restart_file + "_recv"

        config["restart_in_files"][restart_file] = restart_file
        config["restart_in_in_work"][restart_file] = restart_file 
        if not restart_file in config["restart_in_sources"]:
            config["restart_in_sources"][restart_file] = restart_file




    def prepare_restarts(self, restart_file, all_fields, model, config):
        enddate = "_" + config["general"]["end_date"].format(
                form=9, givenph=False, givenpm=False, givenps=False
            )
        #enddate = "_" + str(config["general"]["end_date"].year) + str(config["general"]["end_date"].month) + str(config["general"]["end_date"].day)
        import glob
        import os
        import subprocess
        print("Preparing oasis restart files from initial run...")
        exe = config[model]["executable"]
        print (restart_file, all_fields, model, exe)
        cwd = os.getcwd()
        os.chdir(config["general"]["thisrun_work_dir"])
        filelist = ""
        for field in all_fields:
            print (field + "-" + model)
            thesefiles = glob.glob(field + "_" + exe + "_*.nc")
            print (thesefiles)
            for thisfile in thesefiles:
                print("cdo showtime " + thisfile + " 2>/dev/null | wc -w")
                lasttimestep = subprocess.check_output("cdo showtime " + thisfile + " 2>/dev/null | wc -w", shell=True).decode("utf-8").rstrip()
                #print (lasttimestep)

                print("cdo -O seltimestep," + str(lasttimestep) + " " + thisfile + " onlyonetimestep.nc")
                os.system("cdo -O seltimestep," + str(lasttimestep) + " " + thisfile + " onlyonetimestep.nc")
                print("ncwa -O -a time onlyonetimestep.nc notimestep_" + field + ".nc")
                os.system("ncwa -O -a time onlyonetimestep.nc notimestep_" + field + ".nc")
                filelist += "notimestep_" + field + ".nc "
                print (filelist)
        print("cdo merge " + filelist + " " + restart_file )#+ enddate)
        os.system("cdo merge " + filelist + " " + restart_file )# + enddate)
        rmlist = glob.glob("notimestep*")
        rmlist.append("onlyonetimestep.nc")
        for rmfile in rmlist:
            print("rm " + rmfile)
            os.system("rm " + rmfile)
        os.chdir(cwd)
        

    def finalize(self, destination_dir):
        self.namcouple += [" $END"]
        endline=""
        with open(destination_dir+"/namcouple", "w") as namcouple:
            for line in self.namcouple:
                namcouple.write(endline)
                namcouple.write(line)
                endline="\n"



	
