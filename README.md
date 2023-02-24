# Dremio Toolkit

Dremio Toolkit is a python based set of utilities that help with day-to-day Dremio operation.

## create_snapshot

Dremio provides a backup utility out of the box. 
It allows to take a backup and restore a Dremio environment in its entirety. 
However, sometimes it's not practical to restore the entire environment. 
You may need to restore only a few of virtual data sets, spaces, or sources without impacting the entire Dremio environment. 
<br><br>
<b>env-snapshot</b> comes to help in such scenarios. It takes a snapshot of the entire Dremio environment including Spaces, Sources, and VDS into a single JSON file. It uses Dremio API only and can be run without impact on Dremio operation.
We recommend to take a snapshot of the Development environment on a daily basis.

### Syntax
```commandline
PYTHONPATH=./ python dremio_toolkit/create_snapshot.py -d <DREMIO_HOST>:<DREMIO_PORT> -u <USER> -p  <PASSWORD> -o <OUTPUT_FILE>
```
