/**
 * @author RookieHPC
 * @brief Original source code at https://www.rookiehpc.com/mpi/docs/mpi_type_create_struct.php
 **/

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <mpi.h>

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

    // These cannot be fixed length in real use case
    int sendBuffer[10];
    int receiver[10];

    for (int i = 0; i<10; i++ ){
        sendBuffer[i] = i + processorId;
    }

    int sendIterator = 0;
    int receiveIterator = 0;
    int processIterator = 0;

    bool run = true;
    while (run){
        // send
        if (sendIterator < 10){
            metaData sender;
            sender.number = sendBuffer[sendIterator];
            if (sendIterator == 10){
                sender.stopComms = true;
            }
            MPI_Send(&sender, 1, metaDataType, 1, 0, MPI_COMM_WORLD, &sendRequest[1]);
            MPI_Test(&request[i], &flag, MPI_STATUS_IGNORE);
        } else {
            for (int i=0; i<numberOfProcessors; i++){
                if (i != processorId){
                    metaData sender;
                    sender.stopComms = true;
                    MPI_Send(&sender, 1, metaDataType, 1, 0, MPI_COMM_WORLD);
                }
            }
        }

        // receive
        for (int i=0; i<numberOfProcessors; i++){
            if (i != processorId && !stopComms[i]){
                int flag = 0;
                MPI_Test(&request[i], &flag, MPI_STATUS_IGNORE);

                if (flag == 1){
                    // received
                    receiver[receiveIterator] = receiveBuffer[i].number;
                    receiveIterator += 1;

                    if (!receiveBuffer[i].stopComms){
                        MPI_Irecv(&receiveBuffer[i], 1, metaDataType, 0, 0, MPI_COMM_WORLD, &request[i]);
                    } else {
                        stopComms[i] = true;
                    }
                }
            }
        }

        // process received elements
        if (receiveIterator > processIterator){
            while (processIterator < receiveIterator){
                printf("Processor ID : %d received element %d\n", processorId, receiver[processIterator]);
                processIterator += 1;
            }
        }

        // check if all stop comms
        for (int i=0; i<numberOfProcessors; i++){

            // TODO : also check for length of send array and
            //        check if any more elements to be processed
            run = false;

            if (i != processorId && !stopComms[i]){
                run = true;
            }
        }
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
