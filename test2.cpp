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

    int receive;
    int send;

    int arr[10];

    for (int x=0; x<10; x++)
    {
        arr[x] = processorId+1;
    }

    if (processorId != 0 ){
        printf("%d Receive\n", processorId);
        MPI_Recv(&receive, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    } else {
        send = 10;
        for (int i=1; i<numberOfProcessors; i++)
        {
            printf("%d send\n", processorId);
            MPI_Send(&send, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        }
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
