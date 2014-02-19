hive-postmortem
===============

Analyze Hive logs and summarize key statistics such as data read, number of errors, counters, etc.

Requires numpy to be installed. (pip install numpy)
Currently only supports Hive on Tez.

Examples:

If you have downloaded a YARN log (yarn logs -applicationId)
hive-postmortem -f applicationlog.log

If you are on your Hadoop cluster you can use -j to download the log for you.
hive-postmortem -j application_1392325107376_0195

This tool is still a work in progress.
