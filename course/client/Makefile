CC		= g++
FLAGS	= -O2 -c -D_FILE_OFFSET_BITS=64
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)
	ar x bdb/libdb_stl-6.0.a

%.o : %.cpp
	$(CC) $(FLAGS) -o $@ $<

.PHONY : clean

clean :
	rm -f *.o



