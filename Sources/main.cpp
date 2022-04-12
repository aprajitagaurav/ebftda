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

    // for (std::set<std::string>::iterator it=addresses.begin(); it!=addresses.end(); ++it)
    //     std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
}

void generateGraph(int processorId){
    // TODO : persist throughout processor's runtime...
    //  1. store unsorted transactions
    //  2. sorted transactions
    //  3. unsorted nodes
    //  4. sorted nodes
    //  5. maintain a list of per processor range of global IDs based on received data
    //  6. adjacency list being returned by generateGraph

    // TODO : sort addresses using parallel sample sort []

    // TODO : remove duplicates

    // TODO : global ID assignment using parallel scan

    // TODO : step 5 - read the old transactions and assign local IDs to them (from the old address set)

    // TODO : local ID (old address ID) -> global ID
    //      : local first
    //      : send receive stuff

    // TODO : sort transactions (by global IDs) (edges) - parallel sample sort,
    //  partition such that source addresses exist in the processor and all edges of source node exist together

    // TODO : return adjacency list (of global IDs)
}

int main(int argc, char** argv) {
    // TODO : change to read from command line / MPI ...
    //        support multiple files per process

    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    readFiles(processorId);

    generateGraph(processorId);

    MPI_Finalize();

    return 0;
}
