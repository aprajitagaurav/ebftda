//
// Created by Team EBFTDA on 06/04/22.
//

#include "../Headers/Reader.h"
#include "../Headers/Transaction.h"

#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <stack>
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
    map<string, vector> graph;
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

void blacklisted_node_forest(int processorId, graphData * g, string blacklisted_nodes[]){
    // TODO : init forest as a map, init stack Ap (set of addresses to be visited)
    map<string, string> Fp;
    map<string, int> Dp;
    stack<string> Ap;
    map<string, int> Sp;

    // TODO : make all blacklisted addresses (nodes) as roots
    //  if a root node is present in the local address set,
    //  make the root node point to itself,  depth (D[]) of this root node is 0
    //  add all root nodes to stack Ap
    for(auto node: blacklisted_nodes){
        if(g->graph.count(node)){
            Fp[node] = node;
            Dp[node] = 0;
            Ap.push(node);
        }
    }

    // TODO : |A| : cumulative number of elements in the stack A across processors (use MPI Reduce)
    int numberOfAp = Ap.size();
    int numberOfA;
    MPI_Allreduce(&numberOfAp, &numberOfA, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    // TODO : while loop (while |A| > 0), init Sp map[to] <parent, depth (ideally of the to node from blacklisted root)>
    while(numberOfA > 0){
        Sp.clear();
    //  while loop - (while the stack Ap is not empty) pop s and find all its transactions locally such that s,t,
        while(!Ap.empty()){
            string s = Ap.top()
            Ap.pop();
            for(auto t: g->graph[s]){
    //          if t is local - call visit node on t with Fp(t)
                if(g->graph.count(t)){
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
                        pair<string, int> sourceDepthPair (s, Dp[s]+1);
                        Sp[t] = sourceDepthPair;
                    }
                }
            }
        }
    //  C : cumulative number of S keys across all processors (use MPI all reduce)
        int numberOfSp = Sp.size()
        int C;
        MPI_Allreduce(&numberOfSp, &C, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    //  if C > 0 :
        if(C > 0){
    //            iterate over all ts in Sp and send it to the processor which has t in their local address list. [MPI SEND RECEIVE]
    //            receive all the t's being sent from the other processors...
    //            iterate over the received t's, visit node on t's pair <s,d>,  call visit node on t with Fp(s) - check on algo for specific parameters
        }
    //  update |A| once more [all reduce]
        int numberOfAp1 = Ap.size();
        MPI_Allreduce(&numberOfAp1, &numberOfA, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    }
    //return forest
    return Fp;
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
