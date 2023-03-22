# Dremio Toolkit

Dremio Toolkit is a python based utility that helps with day-to-day Dremio operations.  These functions include: object backup (create_snapshot), object restore (push_snapshot) and differences between two backups (diff_snapshot)

While Dremio provides a backup utility out of the box. It allows to take a backup and restore a Dremio environment in its entirety. However, sometimes it's not practical to restore the entire environment. You may need to restore only a few of virtual data sets, spaces, or sources without impacting the entire Dremio environment.  Using a combination of these functions allows you to create a CI/CD deployment of Dremio objects.

Another useful application of this utility is to create a clone of the existing environment.

Dremio Toolkit uses Dremio API’s only and can be run without impact on Dremio operation.

Dremio Toolkit supports only Dremio Enterprise software. It does not support Community Edition.

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
PYTHONPATH=./ python dremio_toolkit/create_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -o <OUTPUT_FILE> -r <REPORT_JSON_FILE>
```

### Arguments
    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : Dremio user name. User must be a Dremio admin.
    -p or --password : Dremio user password.
    -o or --output-filename : Json file name to save Dremio environment.
    -r or --report-filename : CSV file name for the exception report.
    -e or --report-delimiter : Delimiter to use in the exception report. Default is tab.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -v or --verbose : Set Log to verbose to print object definitions instead of object IDs.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT.


## push_snapshot

This simple command reads json file produced by <b>create_snapshot</b> command and pushes it to a target environment. You can make modifications to the json file prior to pushing it to reflect only changes that you desire to push.
<br><br>

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/push_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -i <INPUT_FILE>
```

### Arguments
    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : User name. User must be a Dremio admin.
    -p or --password : User password.
    -i or --input-filename : Json file name with snapshot of Dremio environment.
    -y or --dry-run : Whether it's a dry run or changes should be made to the target.
    -r or --report-filename : CSV file name for the exception' report.
    -e or --report-delimiter : Delimiter to use in the exception report. Default is tab.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -v or --verbose : Set Log to verbose to print object definitions instead of object IDs.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT."


## diff_snapshot

This  command reads two json files (base and comp) produced by <b>create_snapshot</b> command and produces a json file with a report on difference. 
<br><br>It's useful functionality when you clone an environment and want to validate that the code in the cloned (comp) Dremio environment is the same to the base environment.

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/diff_snapshot.py -b <BASE_JSON_FILE> -c <COMP_JSON_FILE> -r <REPORT_JSON_FILE>
```
 
### Arguments

    -b or --base-filename : Json file name with snapshot of the 'base' Dremio environment.
    -c or --comp-filename : Json file name with snapshot of the 'comp' Dremio environment.
    -r or --report-filename : Json file name for the 'diff' report.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -v or --verbose : Set Log to verbose to print object definitions instead of object IDs.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT.
