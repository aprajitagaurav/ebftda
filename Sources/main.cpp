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

         printf("%d %d\n", val.first, val.second);

         // TODO : Fix map assignment
//         g->transactionOldLocalIdMapping[g->unsortedTransactions.at(i)] = val;
     }

    // TODO : local ID (old address ID) -> global ID
    //      : local first
    //      : send receive stuff

    // TODO : sort transactions (by global IDs) (edges) - parallel sample sort,
    //  partition such that source addresses exist in the processor and all edges of source node exist together

    // TODO : return adjacency list (of global IDs)
}



void todo(graphData * g) {
    // prereq : set with sorted, locally unique addresses
    // TODO :
    //   ----------------SORTING ROUND 1----------------
    //   def fn peek :
    //     return the smallest element of the set / stack...
    //   def fn pop (global ID) :
    //     create a map entry : address -> global id
    //   def fn forceCreateTransactionEntry(globalId) // to be called by leader once per global ID
    //     populate transaction map with given global ID and empty list as value....
    //
    /** Disclaimer - always check the lowest element (from peek) belongs to P0 (leader) handle differently..... */
    // TODO : LEADER - P0 -
    //    FIRST TIME : receive peek on all processors, after each pop
    //    while (not p-1 stopComms ...)
    //      3. find the ith index for lowest element / address
    //      4. check if previous peek != current peek :
    //                      increment global ID for this element and store that along with current element locally.
    //                      set forceCreateTransactionEntry flag
    //      5. send pop(globalId) to i where i is the processor id with lowest peek in current run..., wait for peek on popped process as long communication stop hasn't been received.
    //
    // TODO : FOLLOWER - p1 ... pn
    //   FIRST TIME : call peek fn, send peek data
    //   while set / stack not empty :
    //      1. wait for pop instruction - call pop
    //      2. peek if not fully empty
    //   send stopComms flag..
    //
    // example struct::::
    //  struct message{
    //     bool peek ->
    //     bool pop ->
    //     int peekdata
    //     int popdata
    //     bool stopComms
    //  }
    // TODO :
    //   ----------------SORTING ROUND 2----------------
    //  pre-processing : Generate transactions global id -> global id mapping
    // local sort by source,destination unique transactions, store each transaction as a struct...
    // TODO : LEADER - P0 -
    //  pop call to all followers
    //  receive calls from all followers
    //  while (not p-1 empty flags):
    //    maintain received data, find minimum
    //    send minimum struct along with address to (source%p)th processor (handle p0 case) - saveTransaction
    //    pop call and wait for receive call on the minimum process id
    //  send p-1 stop comm messages to all followers
    //
    // TODO FOLLOWER :
    //  while (!stopcomms){
    //      listen - receive..
    //      if receive is pop : call pop and send popped entry along with the address for the global id, handle if empty - send empty flag..
    //      if receive is save transaction : save transaction, along with global id <-> address mapping if not already present, build adjacency list
    //      handle stop comms flag
    //  }

    // TODO saveTransaction :
    //  get it ready to populate return structure
    //  struct {
    //     map adjList <int, vector> global id to list of dest addresses (global IDs)
    //     map mapping <string, int> address <> global ID
    //  }
}

int main(int argc, char** argv) {
    // TODO : change to read from command line / MPI ...
    //        support multiple files per process

    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    graphData g;

    readFiles(processorId, &g);

    todo(&g);

    MPI_Finalize();

    return 0;
}
