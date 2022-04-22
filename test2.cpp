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
int fileCount = 5;

int main(int argc, char* argv[])
{
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);
    MPI_Request request;

    if (processorId == 0){
        int send = 10;
        for (int i=1; i<numberOfProcessors; i++) {
            send = 10 + i;
            MPI_Isend(&send, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request);
            printf("--------------------------------------------------------SENT\n");
        }
    } else {
        int receive;

        MPI_Irecv(&receive, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &request);

        int flag = 0;
        int ctr = 0;
        while (!flag){
            ctr += 1;
            MPI_Test(&request ,&flag ,MPI_STATUS_IGNORE);
        }
        if (flag == 1){
            printf("--------------------------------------------------------------------%d after %d\n", receive, ctr);
        }
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
