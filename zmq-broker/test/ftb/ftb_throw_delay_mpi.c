/***********************************************************************************/
/* This file is part of FTB (Fault Tolerance Backplance) - the core of CIFTS
 * (Co-ordinated Infrastructure for Fault Tolerant Systems)
 *
 * See http://www.mcs.anl.gov/research/cifts for more information.
 * 	
 */

/* FTB_Version: 0.6.2
 * FTB_API_Version: 0.5
 * FTB_Heredity:FOSS_ORIG
 * FTB_License:BSD
 */

/* This software is licensed under BSD. See the file FTB/misc/license.BSD for
 * complete details on your rights to copy, modify, and use this software.
 */
/***********************************************************************************/


/*
 * A sample MPI application.
 * This calculates the amount of time it takes to run the FTB_Publish
 * routine
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include "mpi.h"

#include "libftb.h"


int main(int argc, char *argv[])
{
    FTB_client_handle_t handle;
    FTB_client_t cinfo;
    FTB_event_handle_t ehandle;
    int i, count;
    int rank, size, ret = 0;
    double begin, end, delay;
    double min, max, avg;

    if (argc > 1) {
        if ((strcasecmp(argv[1], "usage") == 0) || (argc > 2)) {
            printf("Usage: ./ftb_throw_delay_mpi <iterations>");
            exit(0);
        }
        else if (argc == 2) {
            count = atoi(argv[1]);
        }

    }
    else {
        count = 2500;
    }


    /* Create namespace and other attributes before calling FTB_Connect */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.MPI.EXAMPLE_MPI");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NONE");

    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect is not successful ret=%d\n", ret);
        exit(-1);
    }

    FTB_event_info_t event_info[1] = { {"MPI_SIMPLE_EVENT", "INFO"}
    };
    ret = FTB_Declare_publishable_events(handle, 0, event_info, 1);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Barrier(MPI_COMM_WORLD);
    begin = MPI_Wtime();

    /*
     * Publish the events and calculate user-level wall time needed for this
     * publish to complete
     */
    for (i = 0; i < count; i++) {
        FTB_Publish(handle, "MPI_SIMPLE_EVENT", NULL, &ehandle);
    }
    end = MPI_Wtime();
    delay = end - begin;
    MPI_Reduce(&delay, &max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&delay, &min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&delay, &avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    avg /= size;

    if (rank == 0) {
        printf("AvgTime-%d-throws AvgTime-1-throw\n", count);
        printf("%0.5f %0.5f\n", avg, avg / count);
/*
	printf("***** AVG delay: %.5f for %d throws and %d for 1 throw *****\n", avg, count);
	printf("***** MAX delay: %.5f for %d throws *****\n", max, count);
	printf("***** MIN delay: %.5f for %d throws *****\n", min, count);
*/
    }

    MPI_Finalize();
    FTB_Disconnect(handle);

    return 0;
}
