SHELL = /bin/sh
CC = $(CXX)
CFLAGS = -Wall -Wextra -pedantic -Wshadow -funroll-loops -DNDEBUG -O3 -march=native -std=c++11 -pthread
#CFLAGS = -Wall -Wextra -pedantic -Wshadow -g2 -O3 -std=c++11 -pthread

all: test test2

test:
	$(CC) $(CFLAGS) -o test main.cpp utils.cpp -fopenmp

test2:
	$(CC) $(CFLAGS) -o test2 main2.cpp utils.cpp -fopenmp -DALLOW_ANY_RADIX_LOG

clean:
	/bin/rm -f *.o

nuclear:
	/bin/rm -f test test2 *.o
