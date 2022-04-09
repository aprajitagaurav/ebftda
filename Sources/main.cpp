//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <math.h>
#include <mpi.h>
#include <unistd.h>

using namespace std;

int numberOfProcessors;
int processorId;
int fileCount = 5;

void readFiles(int processorId) {

    string inputFileName = "./data/eth-tx-";

    int filesPerProcessor = ceil(fileCount/(float) numberOfProcessors);
    
    vector<Transaction> transactions;
    set<string> addresses;

    for(int i = 0; i < filesPerProcessor; i++){
        int index = processorId*filesPerProcessor + i + 1;
        
        if(index > fileCount)
            break;
        
        string inputFile = inputFileName + to_string(index) + ".txt";
        Reader reader;
        reader.init(inputFile);

        while (!reader.isEofReached()){
            vector<string> txn = reader.getProcessValues();
            string from = txn.at(txn.size() - 3);
            string to = txn.at(txn.size() - 2);

            reader.populateNextLine();

            Transaction transaction(from, to);

            transactions.push_back(transaction);

            addresses.insert(transaction.getFrom());
            addresses.insert(transaction.getTo());
        }

    }

    // for (int i = 0; i < transactions.size(); i++){
    //     std::cout << "Processor Id: " << processorId << " From: " << transactions.at(i).getFrom() << " To: " << transactions.at(i).getTo() << "\n";
    // }

    for (std::set<std::string>::iterator it=addresses.begin(); it!=addresses.end(); ++it)
        std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";

}

int main(int argc, char** argv) {
    // TODO : change to read from command line / MPI ...
    //        support multiple files per process

    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    readFiles(processorId);

    MPI_Finalize();
    

    return 0;
}
