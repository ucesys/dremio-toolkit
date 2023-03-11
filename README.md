# Dremio Toolkit

Dremio Toolkit is a python based utility that helps with day-to-day Dremio operations.  These functions include: object backup (create_snapshot), object restore (push_snapshot) and differences between two backups (diff_snapshot)

While Dremio provides a backup utility out of the box. It allows to take a backup and restore a Dremio environment in its entirety. However, sometimes it's not practical to restore the entire environment. You may need to restore only a few of virtual data sets, spaces, or sources without impacting the entire Dremio environment.  Using a combination of these functions allows you to create a CI/CD deployment of Dremio objects.

Another useful application of this utility is to create a clone of the existing environment.

Dremio Toolkit uses Dremio API’s only and can be run without impact on Dremio operation.

## create_snapshot

Dremio provides a backup utility out of the box. 
It allows to take a backup and restore a Dremio environment in its entirety. 
However, sometimes it's not practical to restore the entire environment. 
You may need to restore only a few of virtual data sets, spaces, or sources without impacting the entire Dremio environment. 
<br><br>
<b>create-snapshot</b> comes to help in such scenarios. It takes a snapshot of the entire Dremio environment including Spaces, Sources, and VDS into a single JSON file. It uses Dremio API only and can be run without impact on Dremio operation.
We recommend to take a snapshot of the Development environment on a daily basis.

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/create_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -o <OUTPUT_FILE>
```


## push_snapshot

This simple command reads json file produced by <b>create_snapshot</b> command and pushes it to a target environment. You can make modifications to the json file prior to pushing it to reflect only changes that you desire to push.
<br><br>

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/push_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -i <INPUT_FILE>
```


## diff_snapshot

This  command reads two json files (base and comp) produced by <b>create_snapshot</b> command and produces a json file with a report on difference. 
<br><br>It's useful functionality when you clone an environment and want to validate that the code in the cloned (comp) Dremio environment is the same to the base environment.

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/diff_snapshot.py -b <BASE_JSON_FILE> -c <COMP_JSON_FILE> -r <REPORT_JSON_FILE>
```
