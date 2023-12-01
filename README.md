# Electronic_load_DL24

Python software for the Atorch DL24 electronic load.

This project was forked from https://github.com/misdoro/Electronic_load_px100.

# Binary protocol

See the [v2.70 binary Protocol description](protocol_PX-100_2_70.md)

# Control software

### Main features:

- Control all load features
- Voltage and Current plot vs time (new: power and MOSFET temperature can also be plotted)
- Save logs to CSV at exit and at device reset
- Internal resistance measurement at user-defined voltage steps
- Software-defined CC-CV discharge to speed up capacity tests for low current discharge

### Changes compared to the original project:
- added power (Watts) and MOSFET temperature (°C) to the readings in the sidebar
- added power (Watts) and MOSFET temperature plots/graphs with an additional secondary Y-axis each
- added visibility toggle for each graph
- added using the "cell label" text field value as graph title 
- force "tight layout" for the graph
- combined all graph legends into a single legend

### Changes from hendkai:
- added streaming to mariadb
  Be aware that prefix column is set by Labe in the GUI if you press
  the "reset" button. 

# DISCLAIMER

For now, this is my personal fork.
- I do not plan to maintain it
- I do not plan to create binaries/installers for it (although it's probably easy since the original author has already set up Github Actions for it, so I might look into it at some point)
- PRs are welcome, but I will likely not try to fix bugs myself that do not affect my own use of the application

The changes I have made were created with a lot of trial and error since python is not a language I know. Basically,
I have no idea what I am doing here.

# Installing

Sorry, no installer. Using this software requires the same steps on Win, Linux and macOS:
- get python if you don't have it; I used 3.8
- clone the repo or use "download zip"
- run `pip install --user -r requirements.txt`
- you can start the application by running `python main.py`

