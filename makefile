ebftda: ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
		mkdir output
		mpicxx -o ebftda ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
		mpirun ./ebftda
clean:
		rm ebftda
		rm -r output
