SHELL = /bin/sh
CC = g++
CFLAGS = -Wall -Wextra -pedantic -Wshadow -funroll-loops -DNDEBUG -O3 -march=native -std=c++0x -pthread
#CFLAGS = -Wall -Wextra -pedantic -Wshadow -g2 -std=c++0x -pthread
#AUX_DISK_FLAGS = -DMONITOR_DISK_USAGE

all: compute_sa

compute_sa:
	$(CC) $(CFLAGS) -o compute_sa src/main.cpp src/fsais_src/utils.cpp -fopenmp $(AUX_DISK_FLAGS)

clean:
	/bin/rm -f *.o

nuclear:
	/bin/rm -f compute_sa *.o
