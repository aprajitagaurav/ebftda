ebftda: ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
	    mpicxx -o ebftda ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
		mpirun ./ebftda
clean:
		rm ebftda
