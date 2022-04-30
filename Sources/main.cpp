//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <fstream>
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

#define DEFAULT_NULL -1
#define PEEK_MESSAGE 0
#define POP_MESSAGE 1
#define PEEK_DATA 2
#define POP_DATA 3

struct graphData{
    set<pair<string, string>> localTransactionsSet;
    set<string> localAddressSet;

    set<pair<int,int>> transactionGlobalIdSet;

    map<string, int> addressGlobalIdMapping;
};

struct metaData{
    bool peek;
    bool pop;
    bool stopComms;
    bool forceTransactionCreate;
};

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

            g->localTransactionsSet.insert(pair<string, string>(transaction.getFrom(),transaction.getTo()));

            g->localAddressSet.insert(transaction.getFrom());
            g->localAddressSet.insert(transaction.getTo());
        }
    }

//     for (int i = 0; i < g->unsortedTransactions.size(); i++){
//         std::cout << "Processor Id: " << processorId << " From: " << g->unsortedTransactions.at(i).getFrom() << " To: " << g->unsortedTransactions.at(i).getTo() << "\n";
//     }

     for (std::set<std::string>::iterator it=g->localAddressSet.begin(); it!=g->localAddressSet.end(); ++it){
//         std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
     }
}

string peek(graphData * g){
    if (g->localAddressSet.size() ==0){
        return "";
    }

    return g->localAddressSet.begin()->c_str();
}

void pop(int globalId, graphData * g){
    set<string>::iterator it = g->localAddressSet.begin();

    string address = it->c_str();

    g->localAddressSet.erase(it);

    g->addressGlobalIdMapping[address] = globalId;
}

void forceCreateTransactionEntry(int globalId, graphData * g){
//    printf("%d force transaction created for global id : %d\n", processorId, globalId);

    g->transactionGlobalIdSet.insert(pair<int, int>(globalId, DEFAULT_NULL));
}

void printAddressSet(graphData * g){
    printf("%d Size: %lu\n",processorId, g->localAddressSet.size());

    for (std::set<std::string>::iterator it=g->localAddressSet.begin(); it!=g->localAddressSet.end(); ++it){
        std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
    }
}

void printAddressGlobalIdMapping(graphData * g){
    string fileName = "./output/processor-"+to_string(processorId);
    printf("HELLO %s\n", fileName.c_str());

    FILE * fp = fopen(fileName.c_str(), "w");

    for (auto const& x : g->addressGlobalIdMapping)
    {
        fprintf(fp, "Processor %d, address : %s, global id : %d\n",processorId, x.first.c_str(), x.second);
    }
}

void printTransactionSet(graphData * g){
    printf("%d Size: %lu\n",processorId, g->localTransactionsSet.size());

    for (std::set<pair<string, string>>::iterator it=g->localTransactionsSet.begin(); it!=g->localTransactionsSet.end(); ++it){
        std::cout << "Processor Id: " << processorId << " Transaction: " << it->first << " " << it->second << "\n";
    }
}

void printGlobalIdTransactionSet(graphData * g){
    printf("%d Size: %lu\n",processorId, g->transactionGlobalIdSet.size());

    for (std::set<pair<int, int>>::iterator it=g->transactionGlobalIdSet.begin(); it!=g->transactionGlobalIdSet.end(); ++it){
        std::cout << "Processor Id: " << processorId << " Transaction: " << it->first << " " << it->second << "\n";
    }
}

bool checkIfStopComms(bool arr[numberOfProcessors]){
    for (int i=0; i<numberOfProcessors; i++){
        if (!arr[i]){
            return false;
        }
    }

    return true;
}

void todo(graphData * g) {
    // prereq : set with sorted, locally unique addresses
    // TODO : DONE
    //   ----------------SORTING ROUND 1----------------
    //   def fn peek :
    //     return the smallest element of the set / stack...
    //   def fn pop (global ID) :
    //     create a map entry : address -> global id
    //   def fn forceCreateTransactionEntry(globalId) // to be called by leader once per global ID
    //     populate transaction map with given global ID and empty list as value....
    //

//    string add =  peek(g).c_str();
//    printf("%s\n", peek(g).c_str());
//    pop(processorId, g);
//    forceCreateTransactionEntry(processorId, g);
//    printf("%s %d \n", add.c_str(), g->addressGlobalIdMapping[add]);
//    printGlobalIdTransactionSet(&g);


    int lengths[6] = { 1, 1, 1, 1};
    MPI_Datatype metaDataType;

    MPI_Aint displacements[4];
    struct metaData dummy;
    MPI_Aint base_address;

    MPI_Get_address(&dummy, &base_address);
    MPI_Get_address(&dummy.peek, &displacements[0]);
    MPI_Get_address(&dummy.pop, &displacements[1]);
    MPI_Get_address(&dummy.stopComms, &displacements[2]);
    MPI_Get_address(&dummy.forceTransactionCreate, &displacements[3]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);
    displacements[2] = MPI_Aint_diff(displacements[2], base_address);
    displacements[3] = MPI_Aint_diff(displacements[3], base_address);

    MPI_Datatype types[4] = { MPI_C_BOOL, MPI_C_BOOL, MPI_C_BOOL, MPI_C_BOOL};
    MPI_Type_create_struct(4, lengths, displacements, types, &metaDataType);
    MPI_Type_commit(&metaDataType);

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

    if (processorId == 0){
        bool stopComms;
        bool stopCommsArray[numberOfProcessors];

        for (int x=0; x<numberOfProcessors; x++){
            stopCommsArray[x] = false;
        }

        string currentAddress = "";
        unsigned long long globalId = 0;

        string add = peek(g);

        metaData receiver[numberOfProcessors];
        char** messageReceiver = new char*[numberOfProcessors];

        for(int i = 0; i < numberOfProcessors; i++)
        {
            messageReceiver[i] = new char[200];
        }

        strcpy(messageReceiver[0], peek(g).c_str());

        for (int i=1; i<numberOfProcessors; i++){
            MPI_Recv(&receiver[i], 1, metaDataType, i, PEEK_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(messageReceiver[i], 200, MPI_CHAR, i, PEEK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (receiver[i].stopComms){
                stopCommsArray[i] = true;
            }
        }

        // TODO : handle edge case where p0 is out of processes
        //  or other processes are not sending etc - DONE

        string minString = messageReceiver[0];
        int minIndex = 0;

        stopComms = checkIfStopComms(stopCommsArray);

        while (!stopComms){
            minString = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            minIndex = -1;

            for (int i=0; i<numberOfProcessors; i++){
                if (!stopCommsArray[i] && (messageReceiver[i] < minString)){
                    minIndex = i;
                    minString = messageReceiver[i];
                }
            }

            metaData sendPop;

            if (minString != currentAddress){
                globalId += 1;
                currentAddress = minString;
                sendPop.forceTransactionCreate = true;
            }

            if (minIndex != 0){
                MPI_Send(&sendPop, 1, metaDataType, minIndex, POP_MESSAGE, MPI_COMM_WORLD);
                MPI_Send(&globalId, 1, MPI_UNSIGNED_LONG_LONG, minIndex, POP_DATA, MPI_COMM_WORLD);

                MPI_Recv(&receiver[minIndex], 1, metaDataType, minIndex, PEEK_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(messageReceiver[minIndex], 200, MPI_CHAR, minIndex, PEEK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                if (receiver[minIndex].stopComms){
                    stopCommsArray[minIndex] = true;
                }

            } else {
                // handle within process 0
                pop(globalId, g);
                if (sendPop.forceTransactionCreate){
                    forceCreateTransactionEntry(globalId, g);
                }

                if (g->localAddressSet.size() == 0){
                    stopCommsArray[0] = true;
                }

                strcpy(messageReceiver[0], peek(g).c_str());
            }

            stopComms = checkIfStopComms(stopCommsArray);
        }
    }
    // TODO : FOLLOWER - p1 ... pn
    //   FIRST TIME : call peek fn, send peek data
    //   while set / stack not empty :
    //      1. wait for pop instruction - call pop
    //      2. peek if not fully empty
    //   send stopComms flag..
    else{
        string add = peek(g);

        metaData sendData;

        if (g->localAddressSet.size() == 0){
            sendData.stopComms = true;
        }

        MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
        MPI_Send(add.c_str(), add.size()+1, MPI_CHAR, 0, PEEK_DATA, MPI_COMM_WORLD);

        while(g->localAddressSet.size() > 0){
            metaData popReceive;
            unsigned long long globalIdReceive;

            MPI_Recv(&popReceive, 1, metaDataType, 0, POP_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&globalIdReceive, 1, MPI_UNSIGNED_LONG_LONG, 0, POP_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            pop(globalIdReceive, g);

            if (popReceive.forceTransactionCreate){
                forceCreateTransactionEntry(globalIdReceive, g);
            }

            add = peek(g);

            metaData sendData;

            if (g->localAddressSet.size() == 0){
                sendData.stopComms = true;
            }

            MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
            MPI_Send(add.c_str(), add.size()+1, MPI_CHAR, 0, PEEK_DATA, MPI_COMM_WORLD);
        }
    }

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
    // TODO : Handle DEFAULT_NULL : -1 case
    for (set<pair<string, string>>::iterator itr = g->localTransactionsSet.begin(); itr != g->localTransactionsSet.end(); itr++)
    {
        g->transactionGlobalIdSet.insert(pair<int, int>(g->addressGlobalIdMapping[itr->first], g->addressGlobalIdMapping[itr->second]));
    }

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

//    printGlobalIdTransactionSet(&g);

    printAddressGlobalIdMapping(&g);

    MPI_Finalize();

    return 0;
}
