SHELL = /bin/sh
CC = $(CXX)
CFLAGS = -Wall -Wextra -pedantic -Wshadow -funroll-loops -DNDEBUG -O3 -march=native -std=c++0x -pthread
#CFLAGS = -Wall -Wextra -pedantic -Wshadow -g2 -std=c++0x -pthread

all: test

test:
	$(CC) $(CFLAGS) -o test main.cpp utils.cpp -fopenmp -DSAIS_DEBUG

clean:
	/bin/rm -f *.o

nuclear:
	/bin/rm -f test *.o
