#
# Makefile for *.cpp
#
# for TSUBAME parallel processors (MPI)
#
CC = mpicxx
CFLAGS = -O3 -c -DSYSTEMCALL
#CFLAGS = -O3 -c -DSYSTEMCALL -DDEBUG_LOG
LDFLAGS = -lm
#
# for RICC parallel processors (MPI)
#
#CC = mpic++ -pc -high
#CFLAGS = -c -DSYSTEMCALL
#LDFLAGS = -lm

LOAD = ./mpidp
$(LOAD) : mpidp.o
	$(CC) -o $@ *.o $(LDFLAGS)
mpidp.o : mpidp.cpp
	$(CC) $(CFLAGS) mpidp.cpp

clean:
	rm -f *.o *~ core.*
