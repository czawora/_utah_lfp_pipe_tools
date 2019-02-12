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

from paths import *


bash_fname = "lfp_run_all.sh"
bash_log_fname = "_lfp_run_all.log"

big_bash_fname = "lfp_initial_big_bash.sh"
swarm_fname = "lfp_initial_swarm.sh"

target_num_bundle_groups = 15
swarm_command = "swarm -g 200 -b %s -t 1 --time 2:00:00 --gres=lscratch:1 --merge-output --logdir "

#########################################################################
#########################################################################
#########################################################################


parser = argparse.ArgumentParser(description='create bash files for spike sorting and lfp extraction')

parser.add_argument('subj_path')

args = parser.parse_args()

subj_path = args.subj_path

timestamp = time.strftime("%d_%m_%Y--%H_%M_%S")

# make subj_path/run_files if it doesnt exist, bash scripts go in there
swarm_files_path = subj_path + "/_swarms"

if os.path.isdir(swarm_files_path) is False:
    os.mkdir(swarm_files_path)
    os.mkdir(swarm_files_path + "/log_dump")

swarm_command += swarm_files_path + "/log_dump"
swarm_command += " -f "


subj_path_files = os.listdir(subj_path)

bundle_size = math.ceil(len(subj_path_files) / target_num_bundle_groups)
swarm_command = swarm_command % str(bundle_size)

# write sort swarm file
swarm_file = open(swarm_files_path + "/" + swarm_fname, 'w')
swarm_file.write(swarm_command + " " + swarm_files_path + "/" + big_bash_fname)
swarm_file.close()

# write sort sort big bash file
big_bash_file = open(swarm_files_path + "/" + big_bash_fname, 'w')


for sess in subj_path_files:

    print(sess)
    # if the sess is raw blackrock file string, need to change it
    if re.match(datestring_regex_new, sess) is not None:

        datestring_match = re.findall(datestring_regex_new, sess)[0]
        print("found datestring match: " + datestring_match)
        datestring_match_splitter = datestring_match
        print("split on datestring match: " + str(sess.split(datestring_match_splitter)))

        output_subj_str = sess.split(datestring_match_splitter)[0].strip("_")
        output_ymd_hms_str = datestring_match[2:len(datestring_match)-2].replace('-', "_")
        output_nsp_str = sess.split(datestring_match_splitter)[1]

        if output_nsp_str[0] == "-":
            output_nsp_str = output_nsp_str[1:]

    elif re.match(datestring_regex_old, sess) is not None:

        datestring_match = re.findall(datestring_regex_old, sess)[0]
        print("found datestring match: " + datestring_match)
        datestring_match_splitter = datestring_match
        print("split on datestring match: " + str(sess.split(datestring_match_splitter)))

        output_subj_str = sess.split(datestring_match_splitter)[0].strip("_")
        output_ymd_hms_str = datestring_match
        output_nsp_str = sess.split(datestring_match_splitter)[1]

        if output_nsp_str[0] == "_":
            output_nsp_str = output_nsp_str[1:]

    if os.path.isdir(subj_path + "/" + sess) is True and sess != "_swarms":

        print("looking at session " + sess + " ymd_hms --> " + output_ymd_hms_str)

        # read the session info file, if there is one
        session_info_glob = glob.glob(subj_path + "/" + sess + "/*_info.txt")
        session_elementInfo_glob = glob.glob(subj_path + "/" + sess + "/*_elementInfo.txt")

        if session_info_glob != [] and session_elementInfo_glob != []:

            session_info_file = open(session_info_glob[0])
            session_info = [l.strip("\n") for l in session_info_file]
            session_info_file.close()

            analog_pulse_ext = session_info[0]
            nsx_ext = session_info[1]

            session_dir = subj_path + "/" + sess + "/lfp"

            if os.path.isdir(session_dir) is False:
                os.mkdir(session_dir)

            # find the nsx file in this session
            g = glob.glob(subj_path + "/" + sess + "/*." + nsx_ext)

            if g != []:

                print(session_dir)

                job_name = "FRNU--" + timestamp + "--" + sess

                nsx_fpath = g[0]
                nsx_fname = nsx_fpath.split("/")[-1]

                ns3_glob = glob.glob(subj_path + "/" + sess + "/*." + analog_pulse_ext)
                nev_glob = glob.glob(subj_path + "/" + sess + "/*.nev")

                big_bash_file.write("bash " + session_dir + "/" + bash_fname + "\n")

                time_log_fpath = session_dir + "/time.log"
                sbatch_file = open(session_dir + "/" + bash_fname, 'w')

                # write the sbatch header for combo bash file
                sbatch_header = []
                sbatch_header.append("#!/bin/bash")

                # if os.path.getsize(nsx_fpath)/1e9 > 25:
                #     sbatch_header.append("#SBATCH --mem=120g")
                # else:
                #     sbatch_header.append("#SBATCH --mem=120g")
                sbatch_header.append("#SBATCH --mem=220g")
                sbatch_header.append("#SBATCH --cpus-per-task=1")
                sbatch_header.append("#SBATCH --error=" + session_dir + "/" + bash_log_fname)
                sbatch_header.append("#SBATCH --output=" + session_dir + "/" + bash_log_fname)
                sbatch_header.append("#SBATCH --time=10:00:00")
                sbatch_header.append("#SBATCH --gres=lscratch:1")

                for l in sbatch_header:
                    sbatch_file.write(l + "\n")

                sbatch_file.write("\n\n")

                # remove an existing _ignore_me.txt
                sbatch_file.write("if [ -f " + session_dir + "/_ignore_me.txt ]; then\n")
                sbatch_file.write("rm " + session_dir + "/_ignore_me.txt\n")
                sbatch_file.write("fi\n\n")

                ###################################################################################################
                ###################################################################################################
                ###################################################################################################

                #################################
                #################################
                # subcommand: splitLFP, write subcommand bash
                #################################

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
                sbatch_header.append("#SBATCH --gres=lscratch:1")

                for l in sbatch_header:
                    sub_cmd_file.write(l + "\n")

                sub_cmd_file.write("\n\n")

                sub_cmd_file.write("echo \"start splitLFP\"\n")

                matlab_command = "cd " + splitLFP_matlab_dir + "/_splitLFP; ./run_splitLFP_swarm.sh " + matlab_compiler_ver_str

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

                #################################
                #################################
                # subcommand: splitLFP, include subcommand bash in combo bash
                #################################

                sbatch_file.write("################################\n")
                sbatch_file.write("# splitLFP\n")
                sbatch_file.write("################################\n")

                sbatch_file.write("start_time=$(date +%s)\n")
                sbatch_file.write("echo -n \"" + sess + ":start_splitLFP:$start_time\" > " + time_log_fpath + "; ")
                sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

                sbatch_file.write("bash " + sub_cmd_fpath + "\n\n")

                sbatch_file.write("done_time=$(date +%s)\n")
                sbatch_file.write("echo \"" + sess + ":done_splitLFP:$done_time\" >> " + time_log_fpath + ";\n\n")

                sbatch_file.write("if [ ! -f " + session_dir + "/_ignore_me.txt ]; then\n")

                ###################################################################################################
                ###################################################################################################
                ###################################################################################################

                #################################
                #################################
                # subcommand: process_split_channels, write subcommand bash
                #################################

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
                sub_cmd.append(lfp_pipeline_dir + "/construct_lfp_split_swarm.py")
                sub_cmd.append(session_dir + "/lfp_splits")
                sub_cmd.append(job_name)
                sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                sub_cmd_file.write("echo \"end process_split_channels\"\n")

                sub_cmd_file.close()

                #################################
                #################################
                # subcommand: process_split_channels, include subcommand bash in combo bash
                #################################

                sbatch_file.write("################################\n")
                sbatch_file.write("# process_split_channels\n")
                sbatch_file.write("################################\n")

                sbatch_file.write("start_time=$(date +%s)\n")
                sbatch_file.write("echo -n \"" + sess + ":start_process_split_channels:$start_time\" >> " + time_log_fpath + "; ")
                sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

                sbatch_file.write("bash " + sub_cmd_fpath + "\n\n")

                sbatch_file.write("done_time=$(date +%s)\n")
                sbatch_file.write("echo \"" + sess + ":done_process_split_channels:$done_time\" >> " + time_log_fpath + ";\n\n")

                ###################################################################################################
                ###################################################################################################
                ###################################################################################################

                #################################
                #################################
                # subcommand: variance_and_lineNoise_exclusion, write subcommand bash
                #################################

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
                sbatch_header.append("#SBATCH --gres=lscratch:5")
                sbatch_header.append("#SBATCH --dependency=singleton")
                sbatch_header.append("#SBATCH --job-name=" + job_name)

                for l in sbatch_header:
                    sub_cmd_file.write(l + "\n")

                sub_cmd_file.write("\n\n")

                sub_cmd_file.write("echo \"start variance_and_lineNoise_exclusion\"\n")

                matlab_command = "cd " + variance_exclusion_matlab_dir + "/_variance_and_lineNoise_exclusion; ./run_variance_and_lineNoise_exclusion_swarm.sh " + matlab_compiler_ver_str

                sub_cmd = []
                sub_cmd.append(matlab_command)

                sub_cmd.append("subj_str")
                sub_cmd.append(output_subj_str)

                sub_cmd.append("time_str")
                sub_cmd.append(output_ymd_hms_str)

                sub_cmd.append("nsp_str")
                sub_cmd.append(output_nsp_str)

                sub_cmd.append("split_path")
                sub_cmd.append(session_dir + "/lfp_splits")

                sub_cmd.append("nsx_physio_fpath")
                sub_cmd.append(nsx_fpath)

                sub_cmd.append('elementInfo_fpath')
                sub_cmd.append(session_elementInfo_glob[0])

                if ns3_glob != []:
                    sub_cmd.append("ns3_pulse_fpath")
                    sub_cmd.append(ns3_glob[0])  # ns3_fpath

                if nev_glob != []:
                    sub_cmd.append("nev_fpath")
                    sub_cmd.append(nev_glob[0])  # nev_fpath

                sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                sub_cmd_file.write("echo \"end variance_and_lineNoise_exclusion\"\n")

                sub_cmd_file.close()

                #################################
                #################################
                # subcommand: variance_and_lineNoise_exclusion, include subcommand bash in combo bash
                #################################

                sbatch_file.write("################################\n")
                sbatch_file.write("# variance_and_lineNoise_exclusion\n")
                sbatch_file.write("################################\n")

                sbatch_file.write("start_time=$(date +%s)\n")
                sbatch_file.write("echo -n \"" + sess + ":start_variance_and_lineNoise_exclusion:$start_time\" >> " + time_log_fpath + "; ")
                sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

                sbatch_file.write("sbatch " + sub_cmd_fpath + "\n\n")

                sbatch_file.write("done_time=$(date +%s)\n")
                sbatch_file.write("echo \"" + sess + ":done_variance_and_lineNoise_exclusion:$done_time\" >> " + time_log_fpath + ";\n\n")

                ###################################################################################################
                ###################################################################################################
                ###################################################################################################

                sbatch_file.write("fi\n\n")
                sbatch_file.close()

big_bash_file.close()
