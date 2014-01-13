CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64
BDBLOC=/usr/local/BerkeleyDB.6.0/lib
.PHONY : all clean tar

all :
	$(MAKE) --directory=client
	$(MAKE) --directory=tool
	$(CC) $(FLAGS) -o run main.cpp client/*.o tool/*.o -L$(BDBLOC) -ldb_stl -lpthread -Wl,-rpath=$(BDBLOC)

clean :
	$(MAKE) --directory=client clean
	$(MAKE) --directory=tool clean
	rm -f run *.tar.gz data/*

tar :
	tar zcvf submission.tar.gz client


