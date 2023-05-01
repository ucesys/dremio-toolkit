# Dremio Toolkit

Dremio Toolkit is a python based utility that helps with day-to-day Dremio operations.  These functions include: object backup (create_snapshot), object restore (push_snapshot) and differences between two backups (diff_snapshot)

While Dremio provides a backup utility out of the box. It allows to take a backup and restore a Dremio environment in its entirety. However, sometimes it's not practical to restore the entire environment. You may need to restore only a few of virtual data sets, spaces, or sources without impacting the entire Dremio environment.  Using a combination of these functions allows you to create a CI/CD deployment of Dremio objects.

Another useful application of this utility is to create a clone of the existing environment.

Dremio Toolkit uses Dremio API’s only and can be run without impact on Dremio operation.

Dremio Toolkit supports only Dremio Enterprise software. It does not support Community Edition.

#### All Dremio Toolkit commands exit with these OS codes:
    0 if no error has been encountered, 
    1 for a fatal error, and 
    2 if only non-fatal errors have been encountered.
Note that some errors may be absolutely fine depending on the state of the environment. For example, a Data Source maybe not available at the time of running the script which is probably normal for a development environment.

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
PYTHONPATH=./ python dremio_toolkit/create_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -o <OUTPUT_FILE> -r <REPORT_FILE>
```

### Arguments
    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : Dremio user name. User must be a Dremio admin.
    -p or --password : Dremio user password.
    -m or --output-mode : Whether create a single output JSON file or a directory with individual files for each object.
    -o or --output-path : Json file name or a directory name to save Dremio environment.
    -r or --report-filename : File name for the tab delimited exception report report.
    -e or --report-delimiter : Delimiter to use in the exception report. Default is tab.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -v or --verbose : Set Log to verbose to print object definitions instead of object IDs.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT.


## push_snapshot

This simple command reads json file produced by <b>create_snapshot</b> command and pushes it to a target environment. You can make modifications to the json file prior to pushing it to reflect only changes that you desire to push.
<br><br>

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/push_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -i <INPUT_FILE> -r <REPORT_FILE>
```

### Arguments
    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : User name. User must be a Dremio admin.
    -p or --password : User password.
    -m or --output-mode : Whether create a single output JSON file or a directory with individual files for each object.
    -i or --input-path : Json file name or a directory name with a snapshot of a Dremio environment.
    -y or --dry-run : Whether it's a dry run or changes should be made to the target.
    -r or --report-filename : File name for the tab delimited exception' report.
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
    -r or --report-filename : File name for the JSON 'diff' report.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -v or --verbose : Set Log to verbose to print object definitions instead of object IDs.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT.

## exec_sql

This  command executes SQL code from a specified file. The file can contain a number of SQL commands which can be separated with ";". 

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/exec_sql.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -s <SQL_FILE> -r <REPORT_FILE>
```
 
### Arguments

    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : User name. User must be a Dremio admin.
    -p or --password : User password.
    -s or --sql-filename : File with SQL code to execute.
    -r or --report-filename : File name for the JSON report.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT."


## rebuild_metadata

This command forces Dremio to forget and then refresh metadata for all or selected physical data sets as per supplied arguments. 

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/rebuild_metadata.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -s <DATASOURCE> -c <CONCURRENCY> -r <REPORT_FILE>
```
 
### Arguments

    -d or --dremio-environment-url : URL to Dremio environment.
    -u or --user : User name. User must be a Dremio admin.
    -p or --password : User password.
    -s or --datasource : Limits the scope of the metadata refresh to physical datasets in a specified datasource. If not specified, metadata for all physical datasets in all datasources will be refreshed.
    -c or --concurrency : Concurrency for executing metadata refresh. It is not recommended to set it higher than 4 if dremio.iceberg.enabled is not set to True. Default concurrency is 1.
    -r or --report-filename : File name for the JSON report.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT."


## explode_snapshot

This  command reads Dremio Environment definition from a single JSON file and saves it into a target directory as a set of JSON files.
Each JSON file represents a single Dremio object, such as VDS, Reflection, etc. <br>
This function is very useful when you need to extract a VDS or other individual objects from a large JSON file. 
Another scenario is when you created a Dremio snapshot as a single JSON file and need to check it in into your repository at a more granular level.

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/explode_snapshot.py -i <input_path> -o <output_path>
```
 
### Arguments

    -i or --input-path : Path to a JSON file with Dremio environment definition.
    -o or --output-path : Path to a target directory to save exploded view of the Dremio environment as a set of JSON files.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT."

## implode_snapshot

This command reads Dremio Environment definition from a directory with a set of JSON files and saves it into a single JSON file.
It is opposite to _explode_snapshot_ command <br>

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/implode_snapshot.py -i <input_path> -o <output_path>
```
 
### Arguments

    -i or --input-path : Path to a directory with the exploded view of the Dremio environment as a set of JSON files.
    -o or --output-path : Path to a target JSON file to save imploded view of the Dremio environment definition.
    -l or --log-level : Set Log Level to DEBUG, INFO, WARN, ERROR.
    -f or --log-filename : Set Log to write to a specified file instead of STDOUT."
