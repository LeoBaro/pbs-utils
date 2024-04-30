#!/bin/bash

# Check https://www.nas.nasa.gov/hecc/support/kb/commonly-used-pbs-commands_174.html

# To display the resources availability for each node
alias nodes="pbsnodes -aSj"

# To display all waiting jobs in the 'gpu' queue
alias gpuq="qstat -i | grep 'gpu\|Queue'"
alias cpuq="qstat -i | grep 'cpu\|Queue'"