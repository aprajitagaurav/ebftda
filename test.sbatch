#!/bin/bash
#SBATCH --nodes=2                        # requests 3 compute servers
#SBATCH --ntasks-per-node=25             # runs 2 tasks on each server
#SBATCH --cpus-per-task=1                # uses 1 compute core per task
#SBATCH --time=20:00:00
#SBATCH --mem=60GB
#SBATCH --job-name=ebftda-exp-7-40gb
#SBATCH --output=greene-exp7-40gb.txt

module purge
module load openmpi/intel/4.0.5           # NOTE: we are loading "openmpi" module here

export LANG=C
export LC_ALL=C

mpicxx -o ebftda ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
time srun ebftda
