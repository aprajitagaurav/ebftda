//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"

#include <iostream>
#include <vector>
#include <queue>
#include <unistd.h>

using namespace std;

int main(int argc, char** argv) {
    // change to read from command line / MPI ...
    // support multiple files per process
    string inputFile = "./data/eth-tx-0-999999.txt";

    // Input file reading
    Reader reader;
    reader.init(inputFile);

    while (!reader.isEofReached()){
        vector<string> txn = reader.getProcessValues();
        string from = txn.at(txn.size() - 3);
        string to = txn.at(txn.size() - 2);

        printf("%s %s\n", from.c_str(), to.c_str());

        reader.populateNextLine();
    }

    return 0;
}
