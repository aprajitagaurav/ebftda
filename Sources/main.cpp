//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <array>
#include <stack>
#include <math.h>
#include <mpi.h>
#include <unistd.h>
#include <map>

using namespace std;

int numberOfProcessors;
int processorId;
int fileCount = 2;

struct graph{

    map<unsigned long long, vector<unsigned long long> > adjList;
    map<string, unsigned long long> addressGlobalIdMapping;
};
struct graphData{
    vector<Transaction> unsortedTransactions;
    vector<string> unsortedAddresses;
    set<string> unsortedAddressesSet;
    vector<string> sortedAddresses;
    vector<pair<string, string> > sortedTransactions;

    map<string, int> addressGlobalIdMapping;
    map<Transaction, pair<int, int> > transactionOldLocalIdMapping;
    struct graph returnGraph;
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
         std::cout << "Processor Id: " << processorId << " Address: " << *it << "\n";
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

void blacklisted_node_forest(int processorId, graph *g, vector<string> blacklisted_nodes){
    // TODO : init forest as a map, init stack Ap (set of addresses to be visited)
    map<unsigned long long, unsigned long long> Fp;
    map<unsigned long long, unsigned long long> Dp;
    stack<unsigned long long> Ap;
    map<unsigned long long, pair<unsigned long long, unsigned long long> > Sp;
    printf("Processor id: %d\n", processorId);
    for(auto &entry: g->adjList){
        printf("%llu: ", entry.first);
        for(auto &value: entry.second){
            printf("%llu ", value);
        }
        printf("\n");
    }

    // TODO : make all blacklisted addresses (nodes) as roots
    //  if a root node is present in the local address set,
    //  make the root node point to itself,  depth (D[]) of this root node is 0
    //  add all root nodes to stack Ap
    for(auto node: blacklisted_nodes){
        if(g->addressGlobalIdMapping.count(node) ){
            unsigned long long nodeId = g->addressGlobalIdMapping[node];
            Fp[nodeId] = nodeId;
            Dp[nodeId] = 0;
            Ap.push(nodeId);
        }
    }

    for(int i = 0; i<Ap.size(); i++){
        printf("Processor Id: %d ----> Ap element\n", processorId);
    }

    // TODO : |A| : cumulative number of elements in the stack A across processors (use MPI Reduce)
    unsigned long long numberOfAp = Ap.size();
    unsigned long long numberOfA;
    MPI_Allreduce(&numberOfAp, &numberOfA, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    // TODO : while loop (while |A| > 0), init Sp map[to] <parent, depth (ideally of the to node from blacklisted root)>
    
    while(numberOfA > 0){
        Sp.clear();
    //  while loop - (while the stack Ap is not empty) pop s and find all its transactions locally such that s,t,
        while(!Ap.empty()){
            unsigned long long s = Ap.top();
            printf("s: %llu\n", s);
            Ap.pop();
            for(auto t: g->adjList[s]){
    //          if t is local - call visit node on t with Fp(t)
                printf("t: %llu\n", t);
                if(g->adjList.count(t)){
                    if(!Fp.count(t) || (Fp.count(t) && (Dp[s]+1 < Dp[t]))){
                        Fp[t] = s;
                        Dp[t] = Dp[s] + 1;
                        Ap.push(t);
                    }
                }
    //          else
    //              if t isnt in Sp map, update Sp[t] = <s, D[s] + 1>
    //              if t is in Sp map, check if D[s] +1 < Sp[t].D, update as above with the lower value
                else{
                    if(!Sp.count(t) || (Sp.count(t) && (Dp[s]+1 < Sp[t].second))){
                        
                        pair<unsigned long long, unsigned long long> sourceDepthPair;
                        sourceDepthPair = make_pair(s, Dp[s]+1);
                        Sp[t] = sourceDepthPair;
                    }
                }
                for(auto &FpEntry: Fp){
                    printf("Processor Id: %d ----> Forest key: %llu, Forest value: %llu\n", processorId, FpEntry.first, FpEntry.second);
                }

                for(auto &DpEntry: Dp){
                    printf("Processor Id: %d ----> Depth key: %llu, Depth value: %llu\n", processorId, DpEntry.first, DpEntry.second);
                }

                for(auto &SpEntry: Sp){
                    printf("Processor Id: %d ----> Sp key: %llu,  src: %llu, depth: %llu\n", processorId, SpEntry.first, SpEntry.second.first, SpEntry.second.second);
                }
            }
        }
    //  C : cumulative number of S keys across all processors (use MPI all reduce)
        unsigned long long numberOfSp = Sp.size();
        unsigned long long C;
        MPI_Allreduce(&numberOfSp, &C, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        printf("C: %llu\n", C);
    //  if C > 0 :
        if(C > 0){
    //            iterate over all ts in Sp and send it to the processor which has t in their local address list. [MPI SEND RECEIVE]    
    //            receive all the t's being sent from the other processors...
    //            iterate over the received t's, visit node on t's pair <s,d>,  call visit node on t with Fp(s) - check on algo for specific parameters
            map<int, vector<array<unsigned long long, 4> > > sendList;
            
            for(int i = 0; i < numberOfProcessors; i++){
                sendList[i];
            }
            
            for(auto &SpEntry: Sp){
                printf("Processor Id: %d ----> Sp key: %llu,  src: %llu, depth: %llu\n", processorId, SpEntry.first, SpEntry.second.first, SpEntry.second.second);
            }

            for(auto &SpEntry: Sp){
                array<unsigned long long, 4> entry;
                entry = {1, SpEntry.first, SpEntry.second.first, SpEntry.second.second};
                int destinationProcessor = (int) (SpEntry.first % (unsigned long long) numberOfProcessors);
                printf("Processor Id: %d ----> Destination processor: %d\n", processorId, destinationProcessor);
                sendList[destinationProcessor].push_back(entry);
                for(auto &arr: sendList[destinationProcessor]){
                    printf("Processor Id: %d ----> [ %llu, %llu, %llu, %llu ] ", processorId, arr[0], arr[1], arr[2], arr[3]);
                }
                printf("\n");
                printf("Processor Id: %d ----> Sp size: %d\n", processorId, Sp.size());
                for(auto &SpEntry: Sp){
                    printf("Processor Id: %d ----> Sp key: %llu,  src: %llu, depth: %llu\n", processorId, SpEntry.first, SpEntry.second.first, SpEntry.second.second);
                }
            }

            Sp.clear();

            for(int i = 0; i < numberOfProcessors; i++){
                if(i != processorId){
                    array<unsigned long long, 4> entry;
                    entry = {0, 0 ,0 ,0};
                    sendList[i].push_back(entry);
                } 
            }

            for(auto &SendListEntry: sendList){
                printf("Processor Id: %d ----> SendList Key: %d, Values: ",  processorId, SendListEntry.first);
                for(auto &message: SendListEntry.second){
                    printf("[ %llu, %llu, %llu, %llu ] ", message[0], message[1], message[2], message[3]);
                }
                printf("\n");
            }
            
            MPI_Request rec_request[numberOfProcessors];
            MPI_Request send_request[numberOfProcessors];
            unsigned long long receive[numberOfProcessors][4];
            bool stopComm[numberOfProcessors];
            bool allReceived = false;
            bool allSent = false;

            for(int i = 0; i<numberOfProcessors; i++){
                
                if(i != processorId)
                    stopComm[i] = false;
                else
                    stopComm[i] = true;
                
                printf("Processor Id: %d --------> hello\n", processorId);
                
                if(i != processorId){
                    MPI_Isend(&sendList[i].front(), 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &send_request[i]);
                    sendList[i].erase(sendList[i].begin());
                
                    MPI_Irecv(&receive[i], 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &rec_request[i]);
                }
            }

            for(auto &SendListEntry: sendList){
                printf("Processor Id: %d ----> SendList Key: %d, Values: ",  processorId, SendListEntry.first);
                for(auto &message: SendListEntry.second){
                    printf("[ %llu, %llu, %llu, %llu ] ", message[0], message[1], message[2], message[3]);
                }
                printf("\n");
            }

            while(!allSent || !allReceived){
                
                if(!allSent){
                    allSent = true;
                    for(int i = 0; i<numberOfProcessors; i++){
                        if(i != processorId){
                            int flag_sent = 0;
                            MPI_Test(&send_request[i], &flag_sent, MPI_STATUS_IGNORE);
                            printf("Processor Id: %d -----------> to Processor: %d flag_sent: %d\n", processorId, i, flag_sent);
                            if(flag_sent == 1){
                                printf("Processor Id: %d -----------> Right before req free\n", processorId);
                                //MPI_Request_free(&send_request[i]);
                                send_request[i] = MPI_REQUEST_NULL;
                                printf("Processor Id: %d -----------> I crossed this shit\n", processorId);
                                if(!sendList[i].empty()){
                                    MPI_Isend(&sendList[i].front(), 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &send_request[i]);
                                    sendList[i].erase(sendList[i].begin());
                                    allSent = false;
                                }
                            }
                        }
                    }
                }
                
                if(!allReceived){
                    allReceived = true;
                    for(int i = 0; i<numberOfProcessors; i++){
                        int flag_rec = 0;
                        if(!stopComm[i]){
                            MPI_Test(&rec_request[i], &flag_rec, MPI_STATUS_IGNORE);
                            if(flag_rec == 1){
                                printf("Processor Id: %d -----------> I'm here motherfuckas!!!!\n", processorId);
                                rec_request[i] = MPI_REQUEST_NULL;
                                if(receive[i][0] != 0){
                                    unsigned long long t = receive[i][1];
                                    unsigned long long s = receive[i][2];
                                    unsigned long long d = receive[i][3];
                                    if(!Fp.count(t) || (Fp.count(t) && (d < Dp[t]))){
                                        Fp[t] = s;
                                        Dp[t] = d;
                                        Ap.push(t);
                                    }
            
                                    MPI_Irecv(&receive[i], 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &rec_request[i]);
                                    allReceived = false;
                                }
                                else
                                    stopComm[i] = true;
                            }
                        }   
                    }
                }                 
            }

        }
    //  update |A| once more [all reduce]
        int numberOfAp1 = Ap.size();
        MPI_Allreduce(&numberOfAp1, &numberOfA, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    }

    for(auto &FpEntry: Fp){
        printf("Processor Id: %d ----> Forest key: %llu, Forest value: %llu\n", processorId, FpEntry.first, FpEntry.second);
    }

    for(auto &DpEntry: Dp){
        printf("Processor Id: %d ----> Depth key: %llu, Depth value: %llu\n", processorId, DpEntry.first, DpEntry.second);
    }
    //return forest
    //return Fp;
}

void visitNode(){
    /**
     * parameters : forest(f), source node(s), destination(t), distances d1 & d2
     * TODO : if (f is null) or (f is not null and d1 < d2)
     *          push t into Ap
     *          Fp (forest) [t] = s
     *          Dp[t] = d1
     */
}

void visitNodeReceive(){
    /**
     * This is specific to the second call on Line 38 in the algo...
     * parameters : forest(f), source node(s), destination(t), distances d1 & d2
     * TODO : if d1 < d2
     *          push t into Ap
     *          Fp (forest) [t] = s
     *          Dp[t] = d1
     */
}


int main(int argc, char** argv) {
    // TODO : change to read from command line / MPI ...
    //        support multiple files per process
    
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);
    
//    graphData g;

//    readFiles(processorId, &g);
//
//    generateGraph(processorId, &g);
    
    struct graph g0;
    struct graph g1;
    
    map<unsigned long long, vector<unsigned long long> > adjList0;
    adjList0[0];
    adjList0[0].push_back(1);
    adjList0[0].push_back(2);

    adjList0[2];
    
    adjList0[4];
    adjList0[4].push_back(0);
    
    map<string, unsigned long long> addressGlobalIdMapping0;
    addressGlobalIdMapping0["0"] = 0;
    addressGlobalIdMapping0["2"] = 2;
    addressGlobalIdMapping0["4"] = 4;

    map<unsigned long long, vector<unsigned long long> > adjList1;
    adjList1[1];
    adjList1[1].push_back(2);
    adjList1[1].push_back(3);
    adjList1[1].push_back(4);

    adjList1[3];

    map<string, unsigned long long> addressGlobalIdMapping1;
    addressGlobalIdMapping1["1"] = 1;
    addressGlobalIdMapping1["3"] = 3;

    g0.adjList = adjList0;
    g0.addressGlobalIdMapping = addressGlobalIdMapping0;

    g1.adjList = adjList1;
    g1.addressGlobalIdMapping = addressGlobalIdMapping1; 

    vector<string> blacklistedNodes;
    blacklistedNodes.push_back("4");
    blacklistedNodes.push_back("1");

    if(processorId == 0){
        blacklisted_node_forest(processorId, &g0, blacklistedNodes);
    }
    else if(processorId == 1){
        blacklisted_node_forest(processorId, &g1, blacklistedNodes);
    }

    MPI_Finalize();

    return 0;
}
