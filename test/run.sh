#!/usr/bin/sh

################################################
mpidp="../mpidp"
# table="./table/table";
# table="./table/table.new";
# table="./table/table.new1";
# table="./table/table.new2";
# table="./table/table.new3";
table="./table/table.new4";
################################################

mpi_opt="--allow-run-as-root"

echo "START>>> mpidp"

echo "mpirun $mpi_opt -np 4 $mpidp -tb $table -jo 1"
mpirun $mpi_opt -np 4 $mpidp -tb $table -jo 1

echo "END>>>>> mpidp"
