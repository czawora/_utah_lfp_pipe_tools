import sys
import glob
import os

expected_output_num = 9

if len(sys.argv) < 2:
    print("first argument should be the subject directory")
    exit(1)

subj_dir = sys.argv[1]

if os.path.isdir(subj_dir) is False:
    print(subj_dir + "is not a valid directory")
    exit(1)


all_session_glob = glob.glob(subj_dir + "/*/*.ns*")
all_session_names = list(set(["/".join(path.split("/")[:-1]) for path in all_session_glob]))

print("** session folders with nsx files: " + str(len(all_session_names)))


output_glob = glob.glob(subj_dir + "/*/lfp/outputs")
output_sessions_unique = list(set(["/".join(path.split("/")[:-2]) for path in output_glob]))

print("** session folders with lfp/outputs directories: " + str(len(output_sessions_unique)))


output_dict = dict.fromkeys(all_session_names, 0)
output_noreref_dict = dict.fromkeys(all_session_names, 0)
output_processed_dict = dict.fromkeys(all_session_names, 0)

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/*"):

    for session in all_session_names:

        if session in globObj:

            output_dict[session] += 1

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/*noreref.mat"):

    for session in all_session_names:

        if session in globObj:

            output_noreref_dict[session] += 1

for globObj in glob.glob(subj_dir + "/*/lfp/outputs/*processed.mat"):

    for session in all_session_names:

        if session in globObj:

            output_processed_dict[session] += 1



all_outputs = 0
missing_outputs = 0
ignore_outputs = 0

incomplete_sessions = []
complete_session = []
ignore_session = []

noreref_strings = []
processed_strings = []
incomplete_strings = []
ignore_strings = []

for session in output_dict.keys():

    if output_noreref_dict[session] == 0:

        noreref_strings.append(session + " is missing noreref.mat")

    if output_processed_dict[session] == 0:

        processed_strings.append(session + " is missing processed.mat")

    if output_dict[session] < expected_output_num:

        if glob.glob(session + "/lfp/_ignore_me.txt") != []:

            ignore_file = open(session + "/lfp/_ignore_me.txt")
            ignore_file_lines = ignore_file.readlines()
            ignore_strings.append("IGNORE: " + session + "  -- " + ignore_file_lines[0])
            ignore_file.close()

            ignore_session.append(session)
            ignore_outputs += 1

        else:

            incomplete_strings.append(session + " -- only has " + str(output_dict[session]) + " outputs ( expected " + str(expected_output_num) + " )")

            missing_outputs += 1
            incomplete_sessions.append(session)

    else:

        complete_session.append(session)
        all_outputs += 1

print("")

for s in noreref_strings:
    print(s)

print("---------------------------------------------------------------------")

for s in processed_strings:
    print(s)

print("---------------------------------------------------------------------")

for s in incomplete_strings:
    print(s)

print("---------------------------------------------------------------------")

for s in ignore_strings:
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
