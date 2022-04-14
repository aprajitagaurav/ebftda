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
#include <map>

using namespace std;

int numberOfProcessors;
int processorId;
int fileCount = 5;

struct graphData{
    vector<Transaction> unsortedTransactions;
    vector<string> unsortedAddresses;
    set<string> unsortedAddressesSet;
    vector<string> sortedAddresses;
    vector<pair<string, string>> sortedTransactions;

    map<string, int> addressGlobalIdMapping;
    map<Transaction, pair<int, int>> transactionOldLocalIdMapping;
};

int getIndex(vector<string> v, string key)
{
    auto it = find(v.begin(), v.end(), key);

    int index = -1;

    if (it != v.end())
    {
        index = it - v.begin();
    }

    return index;
}

void readFiles(int processorId, graphData * g) {
    string inputFileName = "./data/eth-tx-";

    int filesPerProcessor = fileCount/numberOfProcessors;
    int rem = fileCount%numberOfProcessors;
    int start, stop;

    if(processorId < rem){
        start = processorId*(filesPerProcessor + 1);
        stop = start + filesPerProcessor;
    }
    else{
        start = processorId*filesPerProcessor + rem;
        stop = start + filesPerProcessor - 1;
    }

    for(int i = start; i <= stop; i++){
        int index = i + 1;
        
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

            g->unsortedTransactions.push_back(transaction);

            g->unsortedAddressesSet.insert(transaction.getFrom());
            g->unsortedAddressesSet.insert(transaction.getTo());

            // TODO : To be replaced with sorting code
            g->sortedAddresses.push_back(transaction.getFrom());
            g->sortedAddresses.push_back(transaction.getTo());
        }
    }

//     for (int i = 0; i < g->unsortedTransactions.size(); i++){
//         std::cout << "Processor Id: " << processorId << " From: " << g->unsortedTransactions.at(i).getFrom() << " To: " << g->unsortedTransactions.at(i).getTo() << "\n";
//     }

     for (std::set<std::string>::iterator it=g->unsortedAddressesSet.begin(); it!=g->unsortedAddressesSet.end(); ++it){
         g->unsortedAddresses.push_back(*it);
//         std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
     }
}

void removeDuplicates(graphData * g){
    // TODO : Should be removed after parallel sort
    sort(g->sortedAddresses.begin(), g->sortedAddresses.end());

    g->sortedAddresses.erase(unique( g->sortedAddresses.begin(), g->sortedAddresses.end() ), g->sortedAddresses.end());
}

void globalIdAssignment(int processorId, graphData * g){
    int numberOfCumulativeElements;
    int numberOfLocalElements = g->sortedAddresses.size();

    MPI_Scan(&numberOfLocalElements, &numberOfCumulativeElements, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    int offset = numberOfCumulativeElements - numberOfLocalElements;

    for (int i = 0; i < g->sortedAddresses.size(); i++){
        g->addressGlobalIdMapping[g->sortedAddresses.at(i)] = i + offset;
    }
}

void generateGraph(int processorId, graphData * g){
    // TODO : persist throughout processor's runtime...
    //  1. store unsorted transactions
    //  2. sorted transactions
    //  3. unsorted nodes
    //  4. sorted nodes
    //  5. maintain a list of per processor range of global IDs based on received data
    //  6. adjacency list being returned by generateGraph

    // TODO : sort addresses using parallel sample sort []

    // TODO : remove duplicates - DONE
    removeDuplicates(g);

//    for (int i = 0; i < g->sortedAddresses.size(); i++){
//        std::cout << "Processor Id: " << processorId << " Address: " << g->sortedAddresses.at(i) << "\n";
//    }

    // TODO : global ID assignment using parallel scan - DONE
    globalIdAssignment(processorId, g);

//    map<string, int>::iterator itr;

//    for (itr = g->addressGlobalIdMapping.begin(); itr != g->addressGlobalIdMapping.end(); ++itr) {
//        cout << "Processor "<< processorId << " " << itr->first << " " << itr->second << endl;
//    }

    // TODO : step 5 - read the old transactions and assign local IDs to them (from the old address set)
     for (int i = 0; i < g->unsortedTransactions.size(); i++){
         pair<int, int> val;
         val.first = getIndex(g->unsortedAddresses, g->unsortedTransactions.at(i).getFrom());
         val.second = getIndex(g->unsortedAddresses, g->unsortedTransactions.at(i).getTo());

         // TODO : Fix map assignment - DONE
         g->unsortedTransactions.at(i).setLocalFromId(val.first);
         g->unsortedTransactions.at(i).setLocalToId(val.second);

//         cout << g->unsortedTransactions.at(i).getLocalFromId() << " " << g->unsortedTransactions.at(i).getLocalToId() << endl;
     }

    // TODO : local ID (old address ID) -> global ID
    //      : local first
    //      : send receive stuff

    // send receive stuff
    map<string, int> localAddressToGlobalIdMapBuffer;

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

//    graphData g;
//
//    readFiles(processorId, &g);
//
//    generateGraph(processorId, &g);

    int token = processorId;
    for (int i=0; i<numberOfProcessors-1; i++)
    {
        int receive;

        int receiver = (processorId + 1) % numberOfProcessors;
        int source = numberOfProcessors - 1;
        if (processorId != 0){
            source = processorId - 1;
        }

        MPI_Send(&token, 1, MPI_INT, receiver,
                 0, MPI_COMM_WORLD);

        MPI_Recv(&receive, 1, MPI_INT, source, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        printf("Processor %d Sent %d to %d Received %d from %d\n", processorId, token,receiver, receive, source);

        token = receive;
    }

    MPI_Finalize();

    return 0;
}
