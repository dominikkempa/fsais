SHELL = /bin/sh
CC = $(CXX)
#CFLAGS = -Wall -Wextra -pedantic -Wshadow -funroll-loops -DNDEBUG -O3 -march=native -std=c++11 -pthread
CFLAGS = -Wall -Wextra -pedantic -Wshadow -g2 -O3 -std=c++11 -pthread

all: test

test:
	$(CC) $(CFLAGS) -o test main.cpp utils.cpp -fopenmp

clean:
	/bin/rm -f *.o

nuclear:
	/bin/rm -f test *.o
