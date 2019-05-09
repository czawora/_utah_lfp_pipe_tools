import sys
import os
from subprocess import call

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

sys.path.append(dname)

if os.path.isfile(dname + "/paths.py") is False:
    print("move a copy of paths.py into this folder: " + dname)
    exit(2)

import paths

# check for correct amount of inputs
if len(sys.argv) < 3:
    print("first argument should be path to split lfp channel mat files")
    print("second argument should be the job name to give the resulting swarm file")
    exit(-1)

# grab inputs
split_path = sys.argv[1]
job_name = sys.argv[2]

swarm_command = "swarm -g 15 -b 10 -t 1 --partition norm --time 2:00:00 -J " + job_name + " --gres=lscratch:15 --merge-output --logdir "

filt_chan_substr = "filt"
bash_file = "split_big_bash.sh"
swarm_file = "split_swarm.sh"

matlab_command = "cd " + paths.filter_matlab_dir + "/_filter_downsample_detrend_rmLineNoise; ./run_filter_downsample_detrend_rmLineNoise_swarm.sh " + paths.matlab_compiler_ver_str

# set parent directory
split_path_parent_dir = split_path + "/.."

bash_fpath = split_path_parent_dir + "/" + bash_file
swarm_fpath = split_path_parent_dir + "/" + swarm_file

split_log_dump_path = split_path + "/../split_log_dump"
if os.path.isdir(split_log_dump_path) is False:
    os.mkdir(split_log_dump_path)

# complete swarm command
swarm_command += split_log_dump_path
swarm_command += " -f " + bash_fpath

# write swarm file
swarm_file = open(swarm_fpath, 'w')
swarm_file.write(swarm_command)
swarm_file.close()

# open big bash file
bash_file = open(bash_fpath, 'w')

split_path_files = os.listdir(split_path)

for iFile in split_path_files:
    # ignore old resulting filtered files
    if filt_chan_substr not in iFile:
        bash_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;\n")
        bash_file.write(matlab_command + " channel_fpath " + split_path + "/" + iFile + " \n")

# close big bash file
bash_file.close()

call(["bash", swarm_fpath])
