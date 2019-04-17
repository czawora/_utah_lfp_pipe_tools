import sys
import glob
import os
import argparse
import math

parser = argparse.ArgumentParser()

parser.add_argument('subj_path')
parser.add_argument('--rerun', action='store_true')

args = parser.parse_args()

subj_dir = args.subj_path
rerun = args.rerun

if os.path.isdir(subj_dir) is False:
	print(subj_dir + "is not a valid directory")
	exit(1)


all_session_glob = glob.glob(subj_dir + "/*/*.ns*")
all_session_names = list(set(["/".join(path.split("/")[:-1]) for path in all_session_glob]))

print("** session folders with nsx files: " + str(len(all_session_names)))


output_glob = glob.glob(subj_dir + "/*/lfp/outputs")
output_sessions_unique = list(set(["/".join(path.split("/")[:-2]) for path in output_glob]))

print("** session folders with lfp/outputs directories: " + str(len(output_sessions_unique)))


ignore_glob = glob.glob(subj_dir + "/*/lfp/_ignore_me.txt")
ignore_sessions_unique = list(set(["/".join(path.split("/")[:-2]) for path in ignore_glob]))

print("** session folders with lfp/_ignore_me.txt: " + str(len(ignore_sessions_unique)))


output_refset_dict = dict.fromkeys(all_session_names, 0)
output_variance_dict = dict.fromkeys(all_session_names, 0)
output_noreref_dict = dict.fromkeys(all_session_names, 0)
output_processed_dict = dict.fromkeys(all_session_names, 0)

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/variance.csv"):

	for session in all_session_names:

		if session in globObj:

			output_variance_dict[session] += 1

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/*noreref.mat"):

	for session in all_session_names:

		if session in globObj:

			output_noreref_dict[session] += 1

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/*processed.mat"):

	for session in all_session_names:

		if session in globObj:

			output_processed_dict[session] += 1

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/microDev*"):

	for session in all_session_names:

		if session in globObj:

			output_refset_dict[session] += 1

all_outputs = 0
missing_outputs = 0
ignore_outputs = 0

incomplete_sessions = []
complete_session = []
ignore_session = []

noreref_strings = []
processed_strings = []
varaince_strings = []
refset_strings = []
ignore_strings = []

for session in output_variance_dict.keys():

	if glob.glob(session + "/lfp/_ignore_me.txt") != []:

		ignore_file = open(session + "/lfp/_ignore_me.txt")
		ignore_file_lines = ignore_file.readlines()
		ignore_strings.append("IGNORE: " + session + "  -- " + ignore_file_lines[0])
		ignore_file.close()

		ignore_session.append(session)
		ignore_outputs += 1

	else:

		missing_piece = 0

		if output_noreref_dict[session] == 0:

			missing_piece = 1
			noreref_strings.append(session + " is missing noreref.mat")

		if output_processed_dict[session] == 0:

			missing_piece = 1
			processed_strings.append(session + " is missing processed.mat")

		if output_variance_dict[session] == 0:

			missing_piece = 1
			varaince_strings.append(session + " is missing variance.csv")

		if output_refset_dict[session] == 0:

			missing_piece = 1
			refset_strings.append(session + " -- has 0 microDev folders")

		if missing_piece == 0:

			complete_session.append(session)
			all_outputs += 1

		else:

			incomplete_sessions.append(session)
			missing_outputs += 1


print("")

for s in ignore_strings:
	print(s)

print("---------------------------------------------------------------------")

for s in noreref_strings:
	print(s)

print("---------------------------------------------------------------------")

for s in processed_strings:
	print(s)

print("---------------------------------------------------------------------")

for s in varaince_strings:
	print(s)

print("---------------------------------------------------------------------")

for s in refset_strings:
	print(s)

considered_sessions = incomplete_sessions + complete_session + ignore_session
nonstarter_sessions = [sess for sess in all_session_names if sess not in considered_sessions]

for sess in nonstarter_sessions:
	print(sess + " no outputs directory")

print("** session folders with nsx files (all sessions): " + str(len(all_session_names)))
print()
print("** sessions with ignore files (not expecting lfp/outputs): " + str(ignore_outputs))
print("** all sessions - ignore sessions: " + str(len(all_session_names) - ignore_outputs))
print()
print("** sessions with lfp/outputs directories (should equal all_sessions - ignore_sessions): " + str(missing_outputs + all_outputs))
print("** sessions with some outputs missing: " + str(missing_outputs))
print("** sessions with all outputs present: " + str(all_outputs))
# print("** sessions with no outputs directory: " + str(len(nonstarter_sessions)))

if rerun is True and len(incomplete_sessions) > 0:

	rewrite_big_bash_fpath = subj_dir + "/_swarms/lfp_rerun_big_bash.sh"
	rewrite_swarm_fpath = subj_dir + "/_swarms/lfp_rerun_swarm.sh"

	print("\nwriting rerun scripts to:")
	print(rewrite_big_bash_fpath + " + " + rewrite_swarm_fpath)

	target_num_bundle_groups = 15

	swarm_command = "swarm -g 200 -b %s -t 1 --time 2:00:00 --gres=lscratch:15 --merge-output --logdir "
	swarm_command += subj_dir + "/_swarms" + "/log_dump"
	swarm_command += " -f "

	bundle_size = math.ceil(len(incomplete_sessions) / target_num_bundle_groups)
	swarm_command = swarm_command % str(bundle_size)

	# write sort swarm file
	swarm_file = open(rewrite_swarm_fpath, 'w')
	swarm_file.write(swarm_command + " " + rewrite_big_bash_fpath)
	swarm_file.close()

	# write sort sort big bash file
	big_bash_file = open(rewrite_big_bash_fpath, 'w')
	for f in incomplete_sessions:
		big_bash_file.write("bash " + f + "/lfp/lfp_run_all.sh" + "\n")
	big_bash_file.close()
