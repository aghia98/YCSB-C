CC=g++
CFLAGS=-std=c++17 -c -g -Wall -fgnu-tm
INCLUDES=-I../ -I$(HOME)/usr/local/include
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)


all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

