ebftda: ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
	    mpicxx -o output ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
		mpirun ./output
clean:
		rm output
