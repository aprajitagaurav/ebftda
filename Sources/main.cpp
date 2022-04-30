//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <vector>
#include <list>
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
#define SAVE_MESSAGE 4
#define SAVE_DATA_SID 5
#define SAVE_DATA_SIZE 6
#define SAVE_DATA_LIST 7
#define METADATA 8

struct graph{
    map<unsigned long long, vector<unsigned long long> > adjList;
    map<string, unsigned long long> addressGlobalIdMapping;
};

struct graphData{
    set<pair<string, string>  > localTransactionsSet;
    set<string> localAddressSet;

    map<unsigned long long, list<unsigned long long> > localTransactionsMap;

    //map<unsigned long long, list<unsigned long long> > sortedTransactionsMap;

    set<pair<unsigned long long, unsigned long long> > transactionGlobalIdSet;

    map<string, unsigned long long> addressGlobalIdMapping;
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

void addToAdjList(graph* graphInstance, unsigned long long sourceGlobalId, unsigned long long sizeOfList, unsigned long long destinationArr[]) {
    cout<<"addToAdjList: "<< "Processor Id: " << processorId <<" saving sourceGlobalId:"<<sourceGlobalId<<"\n";
    unsigned long long i;
    cout<<"graphInstance->addToAdjList.count(sourceGlobalId): "<<graphInstance->adjList.count(sourceGlobalId)<<endl;
    //if entry exists, add to list
    if(graphInstance->adjList.count(sourceGlobalId)) {
        cout<<"addToAdjList: "<< "Processor Id: " << processorId <<" transaction already exists for src "<<sourceGlobalId<<" size="<<graphInstance->adjList[sourceGlobalId].size()<<" adding "<<sizeOfList<<" more elemnets \n";
        if(sizeOfList != 0)
            for(i=0 ; i<sizeOfList ; i++)   graphInstance->adjList[sourceGlobalId].push_back(destinationArr[i]);
        cout<<"new graphInstance->addToAdjList.count(sourceGlobalId): "<<graphInstance->adjList.count(sourceGlobalId)<<endl;
    }

    else {
        cout<<"addToAdjList: "<< "Processor Id: " << processorId <<" creating new transaction adding src "<<sourceGlobalId<<" of size "<<sizeOfList<<" elemnets \n";
        vector<unsigned long long> l;

        if(sizeOfList != 0)
            for(i=0 ; i<sizeOfList ; i++)   l.push_back(destinationArr[i]);
        
        graphInstance->adjList[sourceGlobalId] = l;
    
    }

    cout<<"addToAdjList: "<< "Processor Id: " << processorId <<" DONE saving sourceGlobalId:"<<sourceGlobalId<<"\n";
    
    for (map<unsigned long long, vector<unsigned long long> >::iterator it=graphInstance->adjList.begin(); it!=graphInstance->adjList.end(); ++it){
        cout << "Processor Id: " << processorId << " addToAdjList entry: " << it->first <<": [ ";
        
        for (vector<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
            std::cout << *it1 <<" ";
        }
        cout<<"]"<<endl;
    }
    cout<<endl;
}

void popTransaction(unsigned long long sourceGlobalId, graphData * g) {
    // for (map<unsigned long long, list<unsigned long long> >::iterator it=g->localTransactionsMap.begin(); it!=g->localTransactionsMap.end(); ++it){
    //     cout << "Processor Id: " << processorId << " popTransaction: " << it->first <<": [ ";
        
    //     for (list<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
    //         std::cout << *it1 <<" ";
    //     }
    //     cout<<"]"<<endl;
    // }

    g->localTransactionsMap.erase(sourceGlobalId);
    if(g->localTransactionsMap.count(sourceGlobalId)) {
        cout<<"found\n";
    }
    else
        cout<<"nop\n";
    printf("%d popTransaction: popped , global id : %llu\n", processorId, sourceGlobalId);

    // for (map<unsigned long long, list<unsigned long long> >::iterator it=g->localTransactionsMap.begin(); it!=g->localTransactionsMap.end(); ++it){
    //     cout << "Processor Id: " << processorId << " popTransaction: " << it->first <<": [ ";
        
    //     for (list<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
    //         std::cout << *it1 <<" ";
    //     }
    //     cout<<"]"<<endl;
    // }
    // cout<<endl;
}

void transactionsToMap(graphData * g,  graph * graphInstance)  {

    for (set<pair<string, string> >::iterator it=g->localTransactionsSet.begin() ; it!=g->localTransactionsSet.end() ; it++) {
        
        //g->addressGlobalIdMapping[it->first]

        if(g->localTransactionsMap.count(g->addressGlobalIdMapping[it->first])) {
            //cout << "transactionsToMap: Processor Id: " << processorId << " exists  key :"<< it->first<<endl;
            g->localTransactionsMap[g->addressGlobalIdMapping[it->first]].push_back(g->addressGlobalIdMapping[it->second]);
        }
        else {
            cout << "transactionsToMap: Processor Id: " << processorId << " key doesnt exist :"<< it->first<<endl;
            list<unsigned long long> l;
            l.push_back(g->addressGlobalIdMapping[it->second]);
            g->localTransactionsMap[g->addressGlobalIdMapping[it->first]] = l;
        }
    }

    //construct graph->addressGlobalIdMapping with only mappings for addressGlobalIdMapping
    for(map<string, unsigned long long>::iterator it=g->addressGlobalIdMapping.begin() ; it!=g->addressGlobalIdMapping.end() ; it++) {
        if(g->localTransactionsMap.count(it->second))
            graphInstance->addressGlobalIdMapping[it->first] = it->second;
    }

    // for (map<string, unsigned long long >::iterator it=graphInstance->addressGlobalIdMapping.begin(); it!=graphInstance->addressGlobalIdMapping.end(); ++it){
    //     cout << "graphInstance->addressGlobalIdMapping: Processor Id: " << processorId << " key: " << it->first <<"  value:"<<it->second<<endl;
    // }
    // cout<<endl;
    
}

string peek(graphData * g){
    if (g->localAddressSet.size() == 0){
        return "";
    }

    return g->localAddressSet.begin()->c_str();
}

unsigned long long peekTransaction(graphData * g){
    // for (map<unsigned long long, list<unsigned long long> >::iterator it=g->localTransactionsMap.begin(); it!=g->localTransactionsMap.end(); ++it){
    //     cout << "Processor Id: " << processorId << " peekTransaction: " << it->first <<": [ ";
        
    //     for (list<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
    //         std::cout << *it1 <<" ";
    //     }
    //     cout<<"]"<<endl;
    // }
    
    cout<<"peekTransaction: processorId:"<<processorId<<"  sending peek:"<<g->localTransactionsMap.begin()->first<<endl;
    if(g->localTransactionsSet.size() == 0)
        return ULLONG_MAX;
    return g->localTransactionsMap.begin()->first;
}

void pop(unsigned long long globalId, graphData * g){
    set<string>::iterator it = g->localAddressSet.begin();

    string address = it->c_str();

    g->localAddressSet.erase(it);

    g->addressGlobalIdMapping[address] = globalId;

    //printf("%d popped %s, assigned global id : %d\n", processorId, address.c_str(), globalId);
}

void forceCreateTransactionEntry(unsigned long long globalId, graphData * g){
    //printf("%d force transaction created for global id : %d\n", processorId, globalId);
    list<unsigned long long> l;
    g->localTransactionsMap[globalId] = l;

    //g->transactionGlobalIdSet.insert(pair<int, int>(globalId, DEFAULT_NULL));
}

void printAddressSet(graphData * g){
    //printf("%d Size: %lu\n",processorId, g->localAddressSet.size());

    for (std::set<std::string>::iterator it=g->localAddressSet.begin(); it!=g->localAddressSet.end(); ++it){
        //std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
    }
}

void printAddressGlobalIdMapping(graphData * g){
    for (auto const& x : g->addressGlobalIdMapping)
    {
        //printf("Processor %d, address : %s, global id : %d\n",processorId, x.first.c_str(), x.second);
    }
}

void printTransactionSet(graphData * g){
    //printf("%d Size: %lu\n",processorId, g->localTransactionsSet.size());

    for (std::set<pair<string, string> >::iterator it=g->localTransactionsSet.begin(); it!=g->localTransactionsSet.end(); ++it){
        //std::cout << "Processor Id: " << processorId << " Transaction: " << it->first << " " << it->second << "\n";
    }
}

void printGlobalIdTransactionSet(graphData * g){
    ////printf("%d Size: %lu\n",processorId, g->transactionGlobalIdSet.size());

    // for (std::set<pair<int, int> >::iterator it=g->transactionGlobalIdSet.begin(); it!=g->transactionGlobalIdSet.end(); ++it){
    //     std::cout << "Processor Id: " << processorId << " Transaction: " << it->first << " " << it->second << "\n";
    // }
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
        //printf("----------FOLLOWER %d size : %lu\n",processorId, g->localAddressSet.size());

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
                //printf("HELLO %d %d\n", i, stopCommsArray[i]);
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
                    //printf("%d REACHED\n", processorId);
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
        //printf("----------FOLLOWER %d size : %lu\n",processorId, g->localAddressSet.size());

        string add = peek(g);

        metaData sendData;

        if (g->localAddressSet.size() == 0){
            sendData.stopComms = true;
            //printf("%d REACHED\n", processorId);
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
                //printf("%d REACHED\n", processorId);
            }

            MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
            MPI_Send(add.c_str(), add.size()+1, MPI_CHAR, 0, PEEK_DATA, MPI_COMM_WORLD);
        }
    }
}

void sortTransactions(graphData * g, graph * graphInstance) {

    cout<<"sortTransactions printing tns on Processor Id: " << processorId <<" \n";

    for (map<unsigned long long, list<unsigned long long> >::iterator it=g->localTransactionsMap.begin(); it!=g->localTransactionsMap.end(); ++it){
        cout << "Processor Id: " << processorId << " printTn: " << it->first <<": [ ";
        
        for (list<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
            std::cout << *it1 <<" ";
        }
        cout<<"]"<<endl;
    }
    cout<<endl<<endl;

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
    // pre-processing : Generate transactions global id -> global id mapping
    // local sort by source,destination unique transactions, store each transaction as a struct...
    // TODO : Handle DEFAULT_NULL : -1 case
    for (set<pair<string, string> >::iterator itr = g->localTransactionsSet.begin(); itr != g->localTransactionsSet.end(); itr++)
    {
        g->transactionGlobalIdSet.insert(pair<unsigned long long, unsigned long long>(g->addressGlobalIdMapping[itr->first], g->addressGlobalIdMapping[itr->second]));
    }

    int lengths[4] = { 1, 1, 1, 1};
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
    

    int stopCommsCounter = 0;
    bool sendStopCommsToAll = false;

    // TODO : LEADER - P0 -
    if (processorId == 0) {
        cout<<"In p0\n";
        cout<<"sortTransactions: processorId:"<<processorId<<"\n";
        //printf("----------FOLLOWER %d size : %lu\n",processorId, g->localAddressSet.size());

        bool stopComms;
        bool stopCommsArray[numberOfProcessors];

        for (int x=0; x<numberOfProcessors; x++){
            stopCommsArray[x] = false;
        }

        metaData receiver[numberOfProcessors];
        unsigned long long messageReceiverTrn[numberOfProcessors];

        // receiver[0].peek = false;
        // receiver[0].pop = false;
        // receiver[0].stopComms = false;
        // receiver[0].forceTransactionCreate = false;

        unsigned long long peekData = peekTransaction(g);        
        messageReceiverTrn[0] =  peekData;

        //  pop call to all followers  --- NOT NEEDED
        cout<<"sortTransactions: processorId:"<<processorId<<" Receieving peek from all\n";
        //  receive calls from all followers
        for (int i=1 ; i<numberOfProcessors ; i++) {
            // receiver[i].peek = false;
            receiver[i].pop = false;
            receiver[i].stopComms = false;
            receiver[i].forceTransactionCreate = false;

            MPI_Recv(&receiver[i], 1, metaDataType, i, PEEK_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (receiver[i].stopComms){
                messageReceiverTrn[i] = UULONG_MAX;
                stopCommsArray[i] = true;
                cout<<"sortTransactions: processorId:"<<processorId<<" got from: "<<i<<" stopcoms   metadata info: "<<receiver[i].pop<<" "<<receiver[i].forceTransactionCreate<<" "<<receiver[i].stopComms<<endl;
                cout<<"sortTransactions: processorId:"<<processorId<<" Processor:"<<i<<" sent stopcoms\n";
            } 
            else {
                MPI_Recv(&messageReceiverTrn[i], 1, MPI_UNSIGNED_LONG_LONG, i, PEEK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                cout<<"sortTransactions: processorId:"<<processorId<<" ******messageReceiverTrnn : "<<messageReceiverTrn[i]<<endl;

            }          
        }
        for(unsigned long long i=0 ; i<numberOfProcessors ; i++)
            cout<<"sortTransactions: processorId:"<<processorId<<" messageReceiverTrn["<<i<<"]: "<<messageReceiverTrn[i]<<endl;

       cout<<"sortTransactions: processorId:"<<processorId<<" check1\n";
        //  while (not p-1 empty flags):
        unsigned long long minGlobalId = messageReceiverTrn[0];
        int minIndex = 0;
        stopComms = checkIfStopComms(stopCommsArray);

        while (!stopComms) {
            minGlobalId = ULLONG_MAX;
            minIndex = -1;
            cout<<"sortTransactions: processorId:"<<processorId<<" Calculating min\n";
            // maintain received data, find minimum
            for (int i=0; i<numberOfProcessors; i++) {
                //printf("HELLO %d %d\n", i, stopCommsArray[i]);

                if (!stopCommsArray[i] && (messageReceiverTrn[i] < minGlobalId)){
                    minIndex = i;
                    minGlobalId = messageReceiverTrn[i];
                }
            }
            cout<<"sortTransactions: processorId:"<<processorId<<" check2\n";
            unsigned long long sizeOfList;
            int destinationProcessor = minGlobalId % numberOfProcessors;
            
            
            
            cout<<"sortTransactions: processorId:"<<processorId<<" minGlobalId: "<<minGlobalId<<" minIndex: "<<minIndex<<"\n";
            if (minIndex != 0) {
                cout<<"sortTransactions: processorId:"<<processorId<<" sending pop\n";
                metaData sendPop;
                sendPop.pop = true;
                // sendPop.peek = false;
                sendPop.stopComms = false;
                sendPop.forceTransactionCreate = false;
                cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" sourceID: "<<minGlobalId<<" pop metadata info: "<<sendPop.pop<<" "<<sendPop.forceTransactionCreate<<" "<<sendPop.stopComms<<endl;
                //here rn
                MPI_Send(&sendPop, 1, metaDataType, minIndex, POP_MESSAGE, MPI_COMM_WORLD);
                
                cout<<"sortTransactions: processorId:"<<processorId<<" Receiving peek again\n";
                MPI_Recv(&receiver[minIndex], 1, metaDataType, minIndex, PEEK_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                cout<<"\n";
                if (receiver[minIndex].stopComms) {
                    stopCommsArray[minIndex] = true;
                    messageReceiverTrn[minIndex] = UULONG_MAX;
                    cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" Receiving peek again, got stopcoms prev minGlobalId:"<<minGlobalId<<" stopComms  metadata info: "<<receiver[minIndex].pop<<" "<<receiver[minIndex].forceTransactionCreate<<" "<<receiver[minIndex].stopComms<<endl;
                    cout<<"sortTransactions: processorId:"<<processorId<<" Processor:"<<minIndex<<" sent stopcoms\n";
                }
                else {
                    MPI_Recv(&messageReceiverTrn[minIndex], 1, MPI_UNSIGNED_LONG_LONG, minIndex, PEEK_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<"sortTransactions: processorId:"<<processorId<<"*** messageReceiverTrn["<<minIndex<<"]: "<<messageReceiverTrn[minIndex]<<endl;
                }
                
                for(unsigned long long i=0 ; i<numberOfProcessors ; i++)
                    cout<<"sortTransactions: processorId:"<<processorId<<" messageReceiverTrn["<<i<<"]: "<<messageReceiverTrn[i]<<endl;

                destinationProcessor = minGlobalId % numberOfProcessors;
                
                cout<<"checkcheck1 destinationProcessor:"<<destinationProcessor<<"  id: "<<minGlobalId<<endl;
                if(destinationProcessor == 0) {
                    cout<<"sortTransactions: processorId:"<<processorId<<" P0 saving transaction from another Prc\n";
                    metaData actionReceive;
                    // actionReceive.peek = false;
                    // actionReceive.pop = false;
                    // actionReceive.stopComms = false;
                    // actionReceive.forceTransactionCreate = false;

                    MPI_Recv(&actionReceive, 1, metaDataType, MPI_ANY_SOURCE, POP_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if(actionReceive.forceTransactionCreate) {
                        unsigned long long sourceId, sizeOfList;
                        MPI_Recv(&sourceId, 1, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_SID, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        cout<<"sortTransactions: processorId:"<<processorId<<" P0 saving transaction sourceId:"<<sourceId<<" destPrc:"<<destinationProcessor<<"\n";
                        MPI_Recv(&sizeOfList, 1, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_SIZE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        
                        unsigned long long destinationArr[sizeOfList];
                        if(sizeOfList != 0)
                            MPI_Recv(&destinationArr, sizeOfList, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_LIST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                        //saveSortedTransaction(g, sourceId, sizeOfList, destinationArr);
                        addToAdjList(graphInstance, sourceId, sizeOfList, destinationArr);
                    }
                }
            }

            else {
                //global min is present locally
                //get size of destination list construct  array of destination addresses from local
                
                cout<<"sortTransactions: processorId:"<<processorId<<" Local min, Constructing destination Arr for destinationProcessor: "<<destinationProcessor<<"\n";
                sizeOfList = g->localTransactionsMap[minGlobalId].size();
                unsigned long long destinationArr[sizeOfList];

                map<unsigned long long, list<unsigned long long> >::iterator it = g->localTransactionsMap.begin();
                list<unsigned long long>::iterator it1 = it->second.begin();

                if(sizeOfList != 0)
                    for(int i=0 ; i<sizeOfList && it1!=it->second.end() ; i++, it1++) {
                        destinationArr[i] = *it1;
                    }
                
                //Will be sending data to (source%p)th processor, pop it
                popTransaction(peekData, g);

                cout<<"sortTransactions: processorId:"<<processorId<<" Get local min again\n";
                if (g->localTransactionsMap.size() == 0){
                    stopCommsArray[0] = true;
                    //printf("%d REACHED\n", processorId);
                }
                else {
                    peekData = peekTransaction(g);
                    cout<<"sortTransactions: processorId:"<<processorId<<" peek: "<<peekData<<endl;
                    messageReceiverTrn[0] = peekData;
                    for(unsigned long long i=0 ; i<numberOfProcessors ; i++)
                        cout<<"sortTransactions: processorId:"<<processorId<<" messageReceiverTrn["<<i<<"]: "<<messageReceiverTrn[i]<<endl;
                }


                //destinatiomn is p0
                cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" sourceID: "<<minGlobalId<<"\n";
                cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<"\n";
                if(destinationProcessor == processorId) {
                    cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" sourceID: "<<minGlobalId<<" saving in if\n";
                    //save transaction and continue
                    //saveSortedTransaction(g, minGlobalId, sizeOfList, destinationArr);
                    addToAdjList(graphInstance, minGlobalId, sizeOfList, destinationArr);
                    //cout<<"skip\n";
                }
                else {
                    cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" sourceID: "<<minGlobalId<<" sending in else\n";
                    metaData sendSortedData;
                    //sendSortedData.peek = false;
                    sendSortedData.pop = false;
                    sendSortedData.stopComms = false;
                    sendSortedData.forceTransactionCreate = true;
                    cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor: "<<destinationProcessor<<" prev  min: "<<minGlobalId<<" sending in else, force metadata info: "<<sendSortedData.pop<<" "<<sendSortedData.forceTransactionCreate<<" "<<sendSortedData.stopComms<<endl;
                    cout<<"sortTransactions: processorId:"<<processorId<<" Sending data:"<<minGlobalId<<" to destination:"<<destinationProcessor<<" srcGID:"<<minGlobalId<<"\n";
                    MPI_Send(&sendSortedData, 1, metaDataType, destinationProcessor, POP_MESSAGE, MPI_COMM_WORLD);
                    MPI_Send(&minGlobalId, 1, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_SID, MPI_COMM_WORLD);
                    MPI_Send(&sizeOfList, 1, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_SIZE, MPI_COMM_WORLD);
                    if(sizeOfList != 0)
                        MPI_Send(&destinationArr, sizeOfList, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_LIST, MPI_COMM_WORLD);
                    cout<<"sortTransactions: processorId:"<<processorId<<" sent data:"<<minGlobalId<<" to destination:"<<destinationProcessor<<"\n";
                }
            }


            //pop call and wait for receive call on the minimum process id ----- NOT NEEDED
            stopComms = checkIfStopComms(stopCommsArray);
        }
        
        //All processors sent stopComs.
        //send p-1 stop comm messages to all followers to end listening for sorted transactions.
        metaData sendStop;
        sendStop.stopComms = true;
        sendStop.forceTransactionCreate = false;
        sendStop.pop = false;
        // sendStop.forceTransactionCreate = false;
        
        for (int i=1 ; i<numberOfProcessors ; i++) {
            cout<<"sortTransactions: processorId:"<<processorId<<" sending stopAllComms\n";
            MPI_Send(&sendStop, 1, metaDataType, i, POP_MESSAGE, MPI_COMM_WORLD);
        }
    }
    
    // TODO FOLLOWER :
    else {
        
        cout<<"sortTransactions: processorId:"<<processorId<<"\n";
        //printf("----------FOLLOWER %d size : %lu\n",processorId, g->localAddressSet.size());
        //todo - redo peak for transactions
        unsigned long long peekData;
        cout<<"sortTransactions: processorId:"<<processorId<<" curr peek:"<<peekData<<"\n";
        metaData sendData;
        // sendData.peek = false;
        sendData.pop = false;
        sendData.stopComms = false;
        sendData.forceTransactionCreate = false;
        

        if (g->localTransactionsMap.size() == 0){
            sendData.stopComms = true;
            sendStopCommsToAll = true;
            cout<<" stopcoms metadata info: "<<sendData.pop<<" "<<sendData.forceTransactionCreate<<" "<<sendData.stopComms<<endl;
            cout<<"sortTransactions: processorId:"<<processorId<<"  sending stopcoms init\n";
            MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
            //printf("%d REACHED end of localTransactionsMap on \n", processorId);
        }
        
        // if local transactionMap not empty send peek
        else {
            peekData = peekTransaction(g);
            cout<<"sortTransactions: processorId:"<<processorId<<" Sending peek:"<<peekData<<"\n";
            MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
            MPI_Send(&peekData, 1, MPI_UNSIGNED_LONG_LONG, 0, PEEK_DATA, MPI_COMM_WORLD);
        }

        //  while (!stopcomms){
        bool stopAllComms = false;
        
        while(g->localTransactionsMap.size() > 0 || stopCommsCounter < numberOfProcessors) {
            cout<<"sortTransactions: processorId:"<<processorId<<" comms count: "<<stopCommsCounter<<"\n";
            metaData actionReceive;
            // actionReceive.peek = false;
            actionReceive.pop = false;
            actionReceive.forceTransactionCreate = false;
            actionReceive.stopComms = false;

            cout<<"sortTransactions: processorId:"<<processorId<<" receiving actionReceive\n";
            cout<<"sortTransactions: processorId:"<<processorId<<" done here\n";
            //wait for call from P0 and check metadata, whether to POP or to SAVE.
            MPI_Recv(&actionReceive, 1, metaDataType, MPI_ANY_SOURCE, POP_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            cout<<"here\n";
            map<unsigned long long, list<unsigned long long> >::iterator it = g->localTransactionsMap.begin();
            cout<<"sortTransactions: processorId:"<<processorId<<" prev local peek:"<<peekData << ", next up top:"<<(++it++)->first<<" received actionReceive:"<<actionReceive.pop<<" "<<actionReceive.forceTransactionCreate<<" "<<actionReceive.stopComms<<"\n";
            if(actionReceive.stopComms) {
                stopCommsCounter++;
                cout<<"sortTransactions: processorId:"<<processorId<<" got Stop comms: "<<stopCommsCounter<<"\n";
                cout<<"sortTransactions: processorId:"<<processorId<<" got Stop comms: "<<stopCommsCounter<<"\n";
            }
                
            cout<<"sortTransactions: processorId:"<<processorId<<" check2\n";
            
            //if p0 asked to pop
            if(actionReceive.pop) {
                cout<<"sortTransactions: processorId:"<<processorId<<" Received pop for :"<<peekData<<"\n";
                
                //only getting data to send, actual send after peek
                
                int destinationProcessor = peekData % numberOfProcessors;

                unsigned long long sizeOfList = g->localTransactionsMap[peekData].size(), sourceGlobalId = peekData;
                unsigned long long destinationArr[sizeOfList];
                map<unsigned long long, list<unsigned long long> >::iterator it = g->localTransactionsMap.begin();
                list<unsigned long long>::iterator it1 = it->second.begin();

                if(sizeOfList != 0)
                    for(int i=0 ; i<sizeOfList && it1!=it->second.end() ; i++, it1++) {
                        destinationArr[i] = *it1;
                    }                
                
                cout<<"popping gid:"<<sourceGlobalId<<endl;
                
                popTransaction(sourceGlobalId, g);

                if (g->localTransactionsMap.size() == 0) {
                    metaData sendData;
                    sendData.pop = false;
                    sendData.forceTransactionCreate = false;
                    sendData.stopComms = true;
                    sendStopCommsToAll = true;
                    cout<<"prev min:"<<sourceGlobalId<<"localTransactionsMap size = 0\n";
                    cout<<"sortTransactions: processorId:"<<processorId<<"  sending stopcoms\n";
                    cout<<"prev min:"<<sourceGlobalId<<" stopComs metadata info: "<<sendData.pop<<" "<<sendData.forceTransactionCreate<<" "<<sendData.stopComms<<endl;
                    MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
                    //printf("%d REACHED\n", processorId);
                }
                else {
                    cout<<"prev min:"<<sourceGlobalId<<"localTransactionsMap not size = 0\n";
                    cout << "Processor Id: " << processorId << " sortTransactions: peeking and printing"<<endl;
                    for (map<unsigned long long, list<unsigned long long> >::iterator it=g->localTransactionsMap.begin(); it!=g->localTransactionsMap.end(); ++it){
                        cout << "Processor Id: " << processorId << " sortTransactions: " << it->first <<": [ ";
                        
                        for (list<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
                            std::cout << *it1 <<" ";
                        }
                        cout<<"]"<<endl;
                    }
                    cout<<endl;
                    peekData = peekTransaction(g);
                    metaData sendData;

                    // sendData.peek = false;
                    sendData.pop = false;
                    sendData.forceTransactionCreate = false;
                    sendData.stopComms = false;
                    cout<<"prev min:"<<sourceGlobalId<<" just  sending peek:"<<peekData<<" metadata info: "<<sendData.pop<<" "<<sendData.forceTransactionCreate<<" "<<sendData.stopComms<<endl;
                    cout<<"sortTransactions: processorId:"<<processorId<<" done sending peek:"<<peekData<<endl;
                    MPI_Send(&sendData, 1, metaDataType, 0, PEEK_MESSAGE, MPI_COMM_WORLD);
                    MPI_Send(&peekData, 1, MPI_UNSIGNED_LONG_LONG, 0, PEEK_DATA, MPI_COMM_WORLD);
                }

                cout<<"sortTransactions: processorId:"<<processorId<<" destinationProcessor:"<<destinationProcessor<<"\n";
                //if destination is another processor
                
                cout<<"checkcheck2 destinationProcessor:"<<destinationProcessor<<"  sourceGlobalId: "<<sourceGlobalId<<endl;

                if(destinationProcessor != processorId) {

                    metaData sendSortedData;
                    sendSortedData.pop = false;
                    sendSortedData.stopComms = false;
                    sendSortedData.forceTransactionCreate = true;
                    cout<<"prev min:"<<sourceGlobalId<<" force metadata info: "<<sendSortedData.pop<<" "<<sendSortedData.forceTransactionCreate<<" "<<sendSortedData.stopComms<<endl;
                    // sendSortedData.peek = false;
                    // sendSortedData.pop = false;
                    // sendSortedData.stopComms = false;

                    cout<<"sortTransactions: processorId:"<<processorId<<" in P1 sending sourceGlobalId:"<<sourceGlobalId<<" to destinationProcessor:"<<destinationProcessor<<"\n";
                    MPI_Send(&sendSortedData, 1, metaDataType, destinationProcessor, POP_MESSAGE, MPI_COMM_WORLD);
                    MPI_Send(&sourceGlobalId, 1, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_SID, MPI_COMM_WORLD);
                    MPI_Send(&sizeOfList, 1, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_SIZE, MPI_COMM_WORLD);
                    if(sizeOfList != 0)
                        MPI_Send(&destinationArr, sizeOfList, MPI_UNSIGNED_LONG_LONG, destinationProcessor, SAVE_DATA_LIST, MPI_COMM_WORLD);
                }
                else {
                    cout<<"checkcheck destinationProcessor:"<<destinationProcessor<<"  sourceGlobalId: "<<sourceGlobalId<<endl;
                    cout<<"saving follower not\n";
                    //saveSortedTransaction(g, sourceGlobalId, sizeOfList, destinationArr);
                    addToAdjList(graphInstance, sourceGlobalId, sizeOfList, destinationArr);
                }

                if(sendStopCommsToAll) {
                    stopCommsCounter++;
                    metaData sendStop;
                    sendStop.stopComms = true;
                    sendStop.forceTransactionCreate = false;
                    sendStop.pop = false;
                    for (int i=1 ; i<numberOfProcessors ; i++) {
                        cout<<"sortTransactions: processorId:"<<processorId<<" sending stopComms to all processors expect me and 0\n";
                        if(i != processorId)
                            MPI_Send(&sendStop, 1, metaDataType, i, POP_MESSAGE, MPI_COMM_WORLD);
                    }
                }

                cout<<" processorId:"<<processorId<<" done pop, going to wait sourceGlobalId:"<<sourceGlobalId<<"\n";
            }

            else if(actionReceive.forceTransactionCreate) {            
            //if save transaction flag set, keep checking for save transaction till it is unset
                //while (actionReceive.forceTransactionCreate) {
                    //save transaction
                    cout<<"sortTransactions: processorId:"<<processorId<<" actionReceive = Save transaction\n";
                    unsigned long long sourceGlobalId, sizeOfList;
                    char** addr = new char*[200];

                    MPI_Recv(&sourceGlobalId, 1, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_SID, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<"sortTransactions: processorId:"<<processorId<<" Save  transaction GID:"<<sourceGlobalId<<"\n";
                    MPI_Recv(&sizeOfList, 1, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_SIZE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<"sortTransactions: processorId:"<<processorId<<" Received sizeOfList:"<<sizeOfList<<"\n";
                    unsigned long long destinationArr[sizeOfList];
                    if(sizeOfList != 0)
                        MPI_Recv(&destinationArr, sizeOfList, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, SAVE_DATA_LIST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<"sortTransactions: processorId:"<<processorId<<" Received dest arr saving\n";
                    for(unsigned long long i=0 ; i<sizeOfList ; i++) {
                        cout<<"Received dest arr, printing"<<destinationArr[i]<<", ";
                    }
                    cout<<endl;
                    //saveSortedTransaction(g, sourceGlobalId, sizeOfList, destinationArr);
                    addToAdjList(graphInstance, sourceGlobalId, sizeOfList, destinationArr);

                    //MPI_Recv(&actionReceive, 1, metaDataType, 0, POP_MESSAGE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    cout<<"sortTransactions: processorId:"<<processorId<<" Done saving id:"<<sourceGlobalId<<"\n";
                //}
                cout<<" processorId:"<<processorId<<" done force save, going to wait sourceGlobalId:"<<sourceGlobalId<<"\n";
            }
            
            
            // else if(actionReceive.stopComms) {
            //     cout<<"sortTransactions: processorId:"<<processorId<<" got Stop ALL comms\n";
            //     stopAllComms = true;
            //     break;
            // }
        }
    }

}

int main(int argc, char** argv) {
    // TODO : change to read from command line / MPI ...
    //        support multiple files per process

    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    graphData g;
    graph graphInstance;

    readFiles(processorId, &g);

    todo(&g);

    transactionsToMap(&g, &graphInstance);

    printGlobalIdTransactionSet(&g);

    printAddressGlobalIdMapping(&g);

    // createAddrMapping(&g, &graphInstance);

    sortTransactions(&g, &graphInstance);

    

    cout<<"------------DONE --------------\n\n";
    cout << "Processor Id: " << processorId << " *final printing  addToAdjList: "<<graphInstance.adjList.size()<<"\n";
    for (map<unsigned long long, vector<unsigned long long> >::iterator it=graphInstance.adjList.begin(); it!=graphInstance.adjList.end(); ++it){
        cout << "Processor Id: " << processorId << " addToAdjList final entry: " << it->first <<": [ ";
        
        for (vector<unsigned long long>::iterator it1=it->second.begin(); it1!=it->second.end(); ++it1) {
            std::cout << *it1 <<" ";
        }
        cout<<"]"<<endl;
    }
    cout<<"Done printing"<<endl;

    MPI_Finalize();

    return 0;
}

