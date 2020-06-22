# Counting Occupants Using Network Technology (COUNT)
This software uses Wi-Fi data to infer approximate occupancy in a large campus. It runs a SNMP query on the Wireless Local Area Network controller (WLAN controller) to obtain the number of connected devices per access point in the whole campus and this can be used as a proxy for the actual number of occupants in the campus.  

## Setup

### Python 
* `python3`
* `pip install -r requirements`

### Data_Post_Processor

* Using the `Data_Post_Processor` as an interface, develop a data processor to parse, process and filter the data after the snmp query
* Currently, a `Cisco_Processor` is included to support parsing SNMP queries from Cisco Aironet 3800 access points

### Data Storage
 
* Currently SQLite3 and InfluxDB databases are supported to be used as local or remote storage or both
 
### Configuration

* Copy `count_config_template.yaml` to `count_config.yaml` and add your configuration there

### Main Scripts To Run

* get_wifi_data.py: Gathers the data, instantiates the data processor and parses, processes and filters the data and pushes it into a local database
* push_to_remote_db.py: Queries the data from the local database, pushes to a remote database 

## How to Run? 
This can be set up as a cronjob to run every few minutes. Below is the example to run it every 10 minutes.

`$ crontab -e` 

At the end of the file add: \
* `*/10 * * * * python /path/to/git/repo/get_wifi_data.py`
* `*/10 * * * * python /path/to/git/repo/push_to_remote_db.py`

## Copyright

Counting Occupants Using Network Technology (COUNT) Copyright (c) 2020,
The Regents of the University of California, through Lawrence Berkeley National
Laboratory (subject to receipt of any required approvals from the U.S. Dept. of
Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.

## License

Counting Occupants Using Network Technology (COUNT) is available under the following [license](https://github.com/LBNL-ETA/COUNT/blob/main/LICENSE.txt).

## Development and contribution

You may report any issues with using the [Issues](https://github.com/LBNL-ETA/COUNT/issues) button.

Contributions in the form of Pull Requests are always welcome.
