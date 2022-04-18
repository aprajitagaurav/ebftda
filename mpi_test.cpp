//
// Created by Sai Neeraj Kanuri on 17/04/22.
//

#include <mpi.h>
#include <iostream>

using namespace std;

int numberOfProcessors;
int processorId;

struct test {
    bool flag;
    bool flag2;
};

int main(int argc, char** argv) {
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfProcessors);
    MPI_Comm_rank(MPI_COMM_WORLD, &processorId);

    int lengths[2] = { 1, 1};

    MPI_Datatype test_type;

    MPI_Aint displacements[2];
    struct test dummy;
    MPI_Aint base_address;

    MPI_Get_address(&dummy, &base_address);
    MPI_Get_address(&dummy.flag, &displacements[0]);
    MPI_Get_address(&dummy.flag2, &displacements[1]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Datatype types[2] = { MPI_C_BOOL, MPI_C_BOOL };
    MPI_Type_create_struct(2, lengths, displacements, types, &test_type);
    MPI_Type_commit(&test_type);

    if (processorId == 0){
//        while (true){
//
//        }
        test send;
        send.flag = false;
        send.flag2 = false;

            for (int i=1; i<numberOfProcessors; i++){
                MPI_Send(&send, 1, test_type, i, 0, MPI_COMM_WORLD);
            }
    } else {
        test receive;
        int source = 0;
        while (receive.flag2){
            printf("Received %d\n",processorId);
            MPI_Recv(&receive, 1, test_type, source, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }

    MPI_Finalize();

    return 0;
}
