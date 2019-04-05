import os
import sys
import glob
import time
import argparse
import math
import re

#########################################################################
#########################################################################
#########################################################################

datestring_regex_old = re.compile(r'.*(\d\d\d\d\d\d_\d\d\d\d).*')
datestring_regex_new = re.compile(r'.*(\d\d\d\d\d\d\d\d-\d\d\d\d\d\d).*')

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

sys.path.append(dname)

if os.path.isfile(dname + "/paths.py") is False:
	print("move a copy of paths.py into this folder: " + dname)
	exit(2)

import paths

#########################################################################
# START FUNCTIONS #######################################################
#########################################################################


def write_splitLFP(session_dir, nsx_fpath):

	sub_cmd_fname = "splitLFP.sh"
	sub_cmd_log_fname = "_splitLFP.log"
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=1")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start splitLFP\"\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.splitLFP_matlab_dir + "/_splitLFP; ./run_splitLFP_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("nsx_fpath")
	sub_cmd.append(nsx_fpath)
	sub_cmd.append("save_dir")
	sub_cmd.append(session_dir)
	sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end splitLFP\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_process_split_channels(session_dir, job_name):

	sub_cmd_fname = "process_split_channels.sh"
	sub_cmd_log_fname = "_process_split_channels.log"
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=5g")
	sbatch_header.append("#SBATCH --cpus-per-task=1")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:1")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start process_split_channels\"\n")

	sub_cmd = []
	sub_cmd.append("python3")
	sub_cmd.append(paths.lfp_pipeline_dir + "/construct_lfp_split_swarm.py")
	sub_cmd.append(session_dir + "/lfp_splits")
	sub_cmd.append(job_name)
	sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end process_split_channels\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_variance_and_lineNoise_exclusion(session_dir, session_nsx_fpath, session_analog_fpath, session_nev_fpath, session_jacksheet_fpath, job_name):

	sub_cmd_fname = "variance_and_lineNoise_exclusion.sh"
	sub_cmd_log_fname = "_variance_and_lineNoise_exclusion.log"
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=1")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")
	sbatch_header.append("#SBATCH --dependency=singleton")
	sbatch_header.append("#SBATCH --job-name=" + job_name)

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start variance_and_lineNoise_exclusion\"\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.variance_exclusion_matlab_dir + "/_variance_and_lineNoise_exclusion; ./run_variance_and_lineNoise_exclusion_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)

	sub_cmd.append("session_path")
	sub_cmd.append(session_dir)

	sub_cmd.append("split_path")
	sub_cmd.append(session_dir + "/lfp_splits")

	sub_cmd.append("nsx_physio_fpath")
	sub_cmd.append(session_nsx_fpath)

	sub_cmd.append('jacksheet_fpath')
	sub_cmd.append(session_jacksheet_fpath)

	if session_analog_fpath != []:
		sub_cmd.append("analog_pulse_fpath")
		sub_cmd.append(session_analog_fpath[0])  # ns3_fpath

	if session_nev_fpath != []:
		sub_cmd.append("nev_fpath")
		sub_cmd.append(session_nev_fpath[0])  # nev_fpath

	sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end variance_and_lineNoise_exclusion\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_session_scripts(subj_path, sess, session_nsx_fpath, session_jacksheet_fpath, analog_pulse_ext, timestamp):

	session_dir = subj_path + "/" + sess + "/lfp"
	job_name = "FRNU--" + timestamp + "--" + sess

	bash_fname = "lfp_run_all.sh"
	bash_log_fname = "_lfp_run_all.log"

	# get pulse filepaths
	analog_pulse_glob = glob.glob(subj_path + "/" + sess + "/*." + analog_pulse_ext)
	nev_glob = glob.glob(subj_path + "/" + sess + "/*.nev")

	if os.path.isdir(session_dir) is False:
		os.mkdir(session_dir)

	for log_file in glob.glob(session_dir + "/*.log"):
		print(" ... removing old log files", end="")
		os.remove(log_file)

	time_log_fpath = session_dir + "/time.log"
	lfp_sbatch_file = open(session_dir + "/" + bash_fname, 'w')

	# write the sbatch header for combo bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=220g")
	sbatch_header.append("#SBATCH --cpus-per-task=1")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + bash_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + bash_log_fname)
	sbatch_header.append("#SBATCH --time=10:00:00")
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		lfp_sbatch_file.write(l + "\n")

	lfp_sbatch_file.write("\n\n")

	# remove an existing _ignore_me.txt
	lfp_sbatch_file.write("if [ -f " + session_dir + "/_ignore_me.txt ]; then\n")
	lfp_sbatch_file.write("rm " + session_dir + "/_ignore_me.txt\n")
	lfp_sbatch_file.write("fi\n\n")

	#################################
	#################################
	# subcommand: splitLFP
	#################################

	sub_cmd_fpath = write_splitLFP(session_dir, session_nsx_fpath, session_jacksheet_fpath)

	# add sub_cmd to combo_run file
	lfp_sbatch_file.write("################################\n")
	lfp_sbatch_file.write("# splitLFP\n")
	lfp_sbatch_file.write("################################\n")

	lfp_sbatch_file.write("start_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo -n \"" + sess + ":start_splitLFP:$start_time\" > " + time_log_fpath + "; ")
	lfp_sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

	lfp_sbatch_file.write("bash " + sub_cmd_fpath + "\n\n")

	lfp_sbatch_file.write("done_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo \"" + sess + ":done_splitLFP:$done_time\" >> " + time_log_fpath + ";\n\n")

	lfp_sbatch_file.write("if [ ! -f " + session_dir + "/_ignore_me.txt ]; then\n")

	#################################
	#################################
	# subcommand: process_split_channels
	#################################

	sub_cmd_fpath = write_process_split_channels(session_dir, job_name)

	# add sub_cmd to combo_run file
	lfp_sbatch_file.write("################################\n")
	lfp_sbatch_file.write("# process_split_channels\n")
	lfp_sbatch_file.write("################################\n")

	lfp_sbatch_file.write("start_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo -n \"" + sess + ":start_process_split_channels:$start_time\" >> " + time_log_fpath + "; ")
	lfp_sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

	lfp_sbatch_file.write("bash " + sub_cmd_fpath + "\n\n")

	lfp_sbatch_file.write("done_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo \"" + sess + ":done_process_split_channels:$done_time\" >> " + time_log_fpath + ";\n\n")

	#################################
	#################################
	# subcommand: variance_and_lineNoise_exclusion
	#################################

	sub_cmd_fpath = write_variance_and_lineNoise_exclusion(session_dir, session_nsx_fpath, analog_pulse_glob, nev_glob, session_jacksheet_fpath, job_name)

	# add sub_cmd to combo_run file
	lfp_sbatch_file.write("################################\n")
	lfp_sbatch_file.write("# variance_and_lineNoise_exclusion\n")
	lfp_sbatch_file.write("################################\n")

	lfp_sbatch_file.write("start_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo -n \"" + sess + ":start_variance_and_lineNoise_exclusion:$start_time\" >> " + time_log_fpath + "; ")
	lfp_sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

	lfp_sbatch_file.write("sbatch " + sub_cmd_fpath + "\n\n")

	lfp_sbatch_file.write("done_time=$(date +%s)\n")
	lfp_sbatch_file.write("echo \"" + sess + ":done_variance_and_lineNoise_exclusion:$done_time\" >> " + time_log_fpath + ";\n\n")

	lfp_sbatch_file.write("fi\n\n")
	lfp_sbatch_file.close()

	return(session_dir + "/" + lfp_sbatch_file)

#########################################################################
# END FUNCTIONS #########################################################
#########################################################################


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='create bash files for spike sorting and lfp extraction')

	parser.add_argument('subj_path')

	args = parser.parse_args()

	subj_path = args.subj_path

	timestamp = time.strftime("%d_%m_%Y--%H_%M_%S")

	subj_path_files = os.listdir(subj_path)
	subj_path_files.sort()

	lfp_big_bash_list = []

	for sess in subj_path_files:

		print("")
		print("looking at session " + sess, end="")

		if os.path.isdir(subj_path + "/" + sess) is True:

			print(" ... is a directory", end="")

			# read the session info file, if there is one
			session_jacksheet_glob = glob.glob(subj_path + "/" + sess + "/jacksheetBR_complete.csv")
			session_info_glob = glob.glob(subj_path + "/" + sess + "/*_info.txt")

			if session_info_glob != [] and session_jacksheet_glob != []:

				print(" ... has jacksheet + info", end="")

				session_info_fpath = session_info_glob[0]
				sesion_jacksheet_fpath = session_jacksheet_glob[0]

				# open the info.txt for this session
				session_info_file = open(session_info_fpath)
				session_info = [l.strip("\n") for l in session_info_file]
				session_info_file.close()

				analog_pulse_ext = session_info[0]
				nsx_ext = session_info[1]
				nsp_suffix = session_info[2]

				session_nsx_glob = glob.glob(subj_path + "/" + sess + "/*." + nsx_ext)

				# there is a nsx file, a jacksheet, and an info file. good to go
				if session_nsx_glob != []:

					print(" ... has an nsx file! zoom!", end="")

					session_nsx_fpath = session_nsx_glob[0]

					session_bash = write_session_scripts(subj_path, sess, session_nsx_fpath, sesion_jacksheet_fpath, analog_pulse_ext, timestamp)
					lfp_big_bash_list.append(session_bash)

	# make subj_path/run_files if it doesnt exist, bash scripts go in there
	swarm_files_path = subj_path + "/_swarms"

	if os.path.isdir(swarm_files_path) is False:
		os.mkdir(swarm_files_path)
		os.mkdir(swarm_files_path + "/log_dump")

	big_bash_fname = "lfp_initial_big_bash.sh"
	swarm_fname = "lfp_initial_swarm.sh"

	target_num_bundle_groups = 15

	swarm_command = "swarm -g 200 -b %s -t 1 --time 2:00:00 --gres=lscratch:1 --merge-output --logdir "
	swarm_command += swarm_files_path + "/log_dump"
	swarm_command += " -f "

	bundle_size = math.ceil(len(lfp_big_bash_list) / target_num_bundle_groups)
	swarm_command = swarm_command % str(bundle_size)

	# write sort swarm file
	swarm_file = open(swarm_files_path + "/" + swarm_fname, 'w')
	swarm_file.write(swarm_command + " " + swarm_files_path + "/" + big_bash_fname)
	swarm_file.close()

	# write sort sort big bash file
	big_bash_file = open(swarm_files_path + "/" + big_bash_fname, 'w')
	for f in lfp_big_bash_list:
		big_bash_file.write(f + "\n")
	big_bash_file.close()
