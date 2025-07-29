# Parse the .nextflow.log file in the current directory to get the current status of the pipeline run.
# Relies on nf-core process names "PIPELINE:SUBWORKFLOW:PROCESS (meta)" to get process and sample info.

# Jul-29 08:42:52.308 [Actor Thread 100] INFO  nextflow.processor.TaskProcessor - [fd/dc57cd] Cached process > NFCORE_TARGETASSEMBLY:TARGETASSEMBLY:BITSCOREFILTER (105603_P001_WB07)

# Jul-29 09:52:31.076 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[jobId: 5379053; id: 46618; name: NFCORE_TARGETASSEMBLY:TARGETASSEMBLY:CLEANHEADERS (105603_P022_WB06); status: COMPLETED; exit: 0; error: -; workDir: /nobackup/autodelete/usr/pcart14/250720_assemble_all_20kb/c9/f26af3f314bd006426f0aafbb1a115 started: 1753804351069; exited: 2025-07-29T15:52:30.705134304Z; ]

# Jul-29 09:52:30.084 [Task submitter] INFO  nextflow.Session - [91/076f86] Submitted process > NFCORE_TARGETASSEMBLY:TARGETASSEMBLY:CLEANHEADERS (105603_P022_WB09)

import re, time, sys, datetime
from collections import defaultdict

# Compile regexes once for performance
process_pattern = re.compile(r".* (\w+:\w+:\w+).*")
exit_pattern = re.compile(r"exit: (\d+);")

first = True

print('')

while True:

    status = defaultdict(lambda: {'submitted': 0, 'completed': 0, 'error': 0})

    with open('.nextflow.log') as f:
        for line in f:  # Stream line by line instead of loading all
            # Check all conditions once
            is_cached = 'Cached process' in line
            is_submitted = 'Submitted process' in line  
            is_completed = 'Task completed' in line
            
            if not (is_cached or is_submitted or is_completed):
                continue
                
            # Extract process name
            match = process_pattern.match(line)
            if not match:
                continue
            process = match.group(1)
            
            # Update counters
            if is_cached:
                status[process]['submitted'] += 1
                status[process]['completed'] += 1
            elif is_submitted:
                status[process]['submitted'] += 1
            elif is_completed:
                status[process]['completed'] += 1
                
                # Check for errors
                exit_match = exit_pattern.search(line)
                if exit_match and exit_match.group(1) != '0':
                    status[process]['error'] += 1

    width = max([len(x) for x in status.keys()])

    output_lines = len(status.keys()) + 1
    now = '\033[2K' + datetime.datetime.now().isoformat() + '\n'

    if first:
        for _ in range(output_lines): sys.stdout.write('\033[2Ktest\n')
        sys.stdout.flush()
        first = False

    sys.stdout.write(f"\033[{output_lines}A")
    sys.stdout.write(now)
    for process, counts in status.items():
        error_string = f" ({counts['error']} errored)" if counts['error'] else ''
        sys.stdout.write(f"\033[2K[ {process.ljust(width)} ]: {counts['completed']} / {counts['submitted'] + counts['error']}{error_string}\n")

    sys.stdout.flush()

    if not first: time.sleep(10)
