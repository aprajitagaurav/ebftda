ebftda: ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
	    mpicxx -std=c++11 -o output ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
		mpirun -np 4 ./output
clean:
		rm output
