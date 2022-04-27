/**
 * @author RookieHPC
 * @brief Original source code at https://www.rookiehpc.com/mpi/docs/mpi_type_create_struct.php
 **/

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <mpi.h>
#include <tuple>
#include <list>

using namespace  std;

int numberOfProcessors;
int processorId;

struct metaData{
    bool stopComms;
    int number;
};

int main(int argc, char* argv[])
{
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    int lengths[2] = { 1, 1};
    MPI_Datatype metaDataType;

    MPI_Aint displacements[2];
    struct metaData dummy;
    MPI_Aint base_address;

    MPI_Get_address(&dummy, &base_address);
    MPI_Get_address(&dummy.stopComms, &displacements[0]);
    MPI_Get_address(&dummy.number, &displacements[1]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Datatype types[2] = { MPI_C_BOOL, MPI_INT};
    MPI_Type_create_struct(2, lengths, displacements, types, &metaDataType);
    MPI_Type_commit(&metaDataType);

    metaData receiveBuffer[numberOfProcessors];
    MPI_Request request[numberOfProcessors];
    MPI_Request sendRequest[numberOfProcessors];

    for (int i=0; i<numberOfProcessors; i++){
        if (i != processorId){
            MPI_Irecv(&receiveBuffer[i], 1, metaDataType, 0, 0, MPI_COMM_WORLD, &request[i]);
        }
    }

    bool stopComms[numberOfProcessors];
    bool run = true;

    list<tuple<int, int, int>> ls;

    ls.push_back(tuple<int, int, int>(1,0,1));
    ls.push_back(tuple<int, int, int>(2,1,2));
    ls.push_back(tuple<int, int, int>(3,2,3));
    ls.push_back(tuple<int, int, int>(4,3,0));
    ls.push_back(tuple<int, int, int>(5,1,3));
    ls.push_back(tuple<int, int, int>(6,2,0));
    ls.push_back(tuple<int, int, int>(7,3,2));
    ls.push_back(tuple<int, int, int>(8,2,1));
    ls.push_back(tuple<int, int, int>(9,0,3));
    ls.push_back(tuple<int, int, int>(10,2,0));
    ls.push_back(tuple<int, int, int>(11,1,3));

    list<tuple<int, int, int>>::iterator it;
    it = ls.begin();
    bool sendInProgress[numberOfProcessors] = false;
    int sendData[numberOfProcessors];

    while (it != ls.end()){
        // send
        int number = get<0>(*it);
        int from = get<1>(*it);
        int to = get<2>(*it);

        if (processorId == from){
            if (!sendInProgress[to]){
                metaData send;
                send.number = number;
                MPI_Isend(&send, 1, MPI_INT, to, 0, MPI_COMM_WORLD, &sendRequest[to]);
            } else {
                // check on send status
                MPI_Test(&request ,&flag ,MPI_STATUS_IGNORE);
            }
        } else {
            it++;
            sendInProgress = false
        }
        // receive

        // process

        // check for stop comms
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
