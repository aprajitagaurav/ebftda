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

void blacklisted_node_forest(int processorId, graph *g, vector<string> blacklisted_nodes){
    // TODO : init forest as a map, init stack Ap (set of addresses to be visited)
    map<unsigned long long, unsigned long long> Fp;
    map<unsigned long long, unsigned long long> Dp;
    stack<unsigned long long> Ap;
    map<unsigned long long, pair<unsigned long long, unsigned long long> > Sp;
    // printf("Processor id: %d\n", processorId);
    // for(auto &entry: g->adjList){
    //     printf("%llu: ", entry.first);
    //     for(auto &value: entry.second){
    //         printf("%llu ", value);
    //     }
    //     printf("\n");
    // }

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

    // for(int i = 0; i<Ap.size(); i++){
    //     printf("Processor Id: %d ----> Ap element\n", processorId);
    // }

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
            // printf("Processor Id: %d --------> s: %llu\n", processorId, s);
            Ap.pop();
            for(auto t: g->adjList[s]){
    //          if t is local - call visit node on t with Fp(t)
                // printf("Processor Id: %d --------> t: %llu\n", processorId, t);
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
                // for(auto &FpEntry: Fp){
                //     printf("Processor Id: %d ----> Forest key: %llu, Forest value: %llu\n", processorId, FpEntry.first, FpEntry.second);
                // }

                // for(auto &DpEntry: Dp){
                //     printf("Processor Id: %d ----> Depth key: %llu, Depth value: %llu\n", processorId, DpEntry.first, DpEntry.second);
                // }

                // for(auto &SpEntry: Sp){
                //     printf("Processor Id: %d ----> Sp key: %llu,  src: %llu, depth: %llu\n", processorId, SpEntry.first, SpEntry.second.first, SpEntry.second.second);
                // }
            }
            // printf("ProcessorId: %d, Ap Size: %d\n", processorId, Ap.size());
        }
        // printf("ProcessorId: %d ooolalalalallala\n", processorId);
    //  C : cumulative number of S keys across all processors (use MPI all reduce)
        unsigned long long numberOfSp = Sp.size();
        unsigned long long C;
        MPI_Allreduce(&numberOfSp, &C, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        // printf("Processor Id: %d, C: %llu\n", processorId, C);
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
                array<unsigned long long, 4> entry;
                entry = {1, SpEntry.first, SpEntry.second.first, SpEntry.second.second};
                int destinationProcessor = (int) (SpEntry.first % (unsigned long long) numberOfProcessors);
                // printf("Processor Id: %d ----> Destination processor: %d\n", processorId, destinationProcessor);
                sendList[destinationProcessor].push_back(entry);
            }

            for(int i = 0; i < numberOfProcessors; i++){
                if(i != processorId){
                    array<unsigned long long, 4> entry;
                    entry = {0, 0 ,0 ,0};
                    sendList[i].push_back(entry);
                } 
            }

            // for(auto &SendListEntry: sendList){
            //     printf("Processor Id: %d ----> SendList Key: %d, Values: ",  processorId, SendListEntry.first);
            //     for(auto &message: SendListEntry.second){
            //         printf("[ %llu, %llu, %llu, %llu ] ", message[0], message[1], message[2], message[3]);
            //     }
            //     printf("\n");
            // }
            
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
                
                if(i != processorId){
                    MPI_Isend(&sendList[i].front(), 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &send_request[i]);
                    sendList[i].erase(sendList[i].begin());
                
                    MPI_Irecv(&receive[i], 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &rec_request[i]);
                }
            }

            // for(auto &SendListEntry: sendList){
            //     printf("Processor Id: %d ----> SendList Key: %d, Values: ",  processorId, SendListEntry.first);
            //     for(auto &message: SendListEntry.second){
            //         printf("[ %llu, %llu, %llu, %llu ] ", message[0], message[1], message[2], message[3]);
            //     }
            //     printf("\n");
            // }

            while(!allSent || !allReceived){
                
                if(!allSent){
                    allSent = true;
                    for(int i = 0; i<numberOfProcessors; i++){
                        if(i != processorId){
                            int flag_sent = 0;
                            MPI_Test(&send_request[i], &flag_sent, MPI_STATUS_IGNORE);
                            //printf("Processor Id: %d -----------> to Processor: %d flag_sent: %d\n", processorId, i, flag_sent);
                            if(flag_sent == 1){
                                //MPI_Request_free(&send_request[i]);
                                send_request[i] = MPI_REQUEST_NULL;
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
                            //printf("Processor Id: %d -----------> from Processor: %d flag_rec: %d\n", processorId, i, flag_rec);
                            if(flag_rec == 1){
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
                                    //printf("ProcessorId: %d from Processor: %d ------> I'm here!!!!!\n", processorId, i);
            
                                    MPI_Irecv(&receive[i], 4, MPI_UNSIGNED_LONG_LONG, i, 0, MPI_COMM_WORLD, &rec_request[i]);
                                    allReceived = false;
                                }
                                else
                                    stopComm[i] = true;
                            }
                            else
                                allReceived = false;
                        }   
                    }
                }                 
            }

        }

        // for(auto &FpEntry: Fp){
        //     printf("Processor Id: %d ----> Forest key: %llu, Forest value: %llu\n", processorId, FpEntry.first, FpEntry.second);
        // }

        // for(auto &DpEntry: Dp){
        //     printf("Processor Id: %d ----> Depth key: %llu, Depth value: %llu\n", processorId, DpEntry.first, DpEntry.second);
        // }

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
    struct graph g2;
    struct graph g3;
    
    map<unsigned long long, vector<unsigned long long> > adjList0;
    adjList0[0];
    adjList0[0].push_back(1);
    adjList0[0].push_back(2);

    adjList0[4];
    adjList0[4].push_back(0);
    adjList0[4].push_back(6);
    
    
    
    map<string, unsigned long long> addressGlobalIdMapping0;
    addressGlobalIdMapping0["0"] = 0;
    addressGlobalIdMapping0["4"] = 4;

    map<unsigned long long, vector<unsigned long long> > adjList1;
    adjList1[1];
    adjList1[1].push_back(2);
    adjList1[1].push_back(3);
    adjList1[1].push_back(4);

    adjList1[5];

    map<string, unsigned long long> addressGlobalIdMapping1;
    addressGlobalIdMapping1["1"] = 1;
    addressGlobalIdMapping1["5"] = 5;

    map<unsigned long long, vector<unsigned long long> > adjList2;
    adjList2[2];
    adjList2[2].push_back(5);
    adjList2[2].push_back(6);

    adjList2[6];
    adjList2[6].push_back(1);
    adjList2[6].push_back(3);

    map<string, unsigned long long> addressGlobalIdMapping2;
    addressGlobalIdMapping2["2"] = 2;
    addressGlobalIdMapping2["6"] = 6;

    map<unsigned long long, vector<unsigned long long> > adjList3;
    adjList3[3];
    adjList3[3].push_back(5);

    map<string, unsigned long long> addressGlobalIdMapping3;
    addressGlobalIdMapping3["3"] = 3;

    g0.adjList = adjList0;
    g0.addressGlobalIdMapping = addressGlobalIdMapping0;

    g1.adjList = adjList1;
    g1.addressGlobalIdMapping = addressGlobalIdMapping1; 

    g2.adjList = adjList2;
    g2.addressGlobalIdMapping = addressGlobalIdMapping2; 

    g3.adjList = adjList3;
    g3.addressGlobalIdMapping = addressGlobalIdMapping3;

    vector<string> blacklistedNodes;
    blacklistedNodes.push_back("0");
    blacklistedNodes.push_back("2");
    blacklistedNodes.push_back("6");

    if(processorId == 0){
        blacklisted_node_forest(processorId, &g0, blacklistedNodes);
    }
    else if(processorId == 1){
        blacklisted_node_forest(processorId, &g1, blacklistedNodes);
    }
    else if(processorId == 2){
        blacklisted_node_forest(processorId, &g2, blacklistedNodes);
    }
    else if(processorId == 3){
        blacklisted_node_forest(processorId, &g3, blacklistedNodes);
    }

    MPI_Finalize();

    return 0;
}
