ebftda: ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp
	    g++ -std=c++11 ./Sources/main.cpp ./Sources/Reader.cpp ./Sources/Transaction.cpp

clean:
		rm ./a.out
