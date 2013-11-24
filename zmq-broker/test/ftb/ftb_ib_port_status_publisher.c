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
 * This example demonstrates an InfiniBand network library
 *
 * Description:
 * The current example throws an event when an InfiniBand port becomes
 * active
 *
 * This example requires that the OFED distribution be present on the
 * machine. If OFED is not present, compilation errors may occurs
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include "ftb.h"
#include <infiniband/verbs.h>

typedef struct {
    struct ibv_device *nic;
    struct ibv_context *context;
    struct ibv_pd *ptag;
    struct ibv_qp **qp_hndl;
    struct ibv_cq *cq_hndl;
    pthread_t async_thread;
} ibv_info_t;

ibv_info_t ibv_dev;

/*
 * Open the HCA to get the HCA Context
 */
static void open_hca(void)
{
    struct ibv_device **dev_list;
    int num_adapters;

    /* Get the list of the adapters */
    dev_list = ibv_get_device_list(&num_adapters);

    /* Select the first Adapter for now, more changes can be added later */
    ibv_dev.nic = dev_list[0];

    /* Get the context from the selected Adapter */
    ibv_dev.context = ibv_open_device(dev_list[0]);

    /* If the context is not found, report an error and exit */
    if (!ibv_dev.context) {
        fprintf(stderr, "Error Getting HCA Context, Aborting ..\n");
        exit(1);
    }

    /* Allocate the protection domain */
    ibv_dev.ptag = ibv_alloc_pd(ibv_dev.context);

    if (!ibv_dev.ptag) {
        fprintf(stderr, "Error Getting Protection Domain for HCA, Aborting ..\n");
    }
}



int main(int argc, char *argv[])
{
    FTB_client_t cinfo;
    FTB_client_handle_t handle;
    int i, ret = 0;
    struct ibv_port_attr port_attr;
    FTB_event_info_t event_info[1] = { {"ib_port_active", "info"} };

    printf("Begin\n");

    /* Specify this components information and call FTB_Connect */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.network_libaries.infiniband");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NONE");

    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect did not return a success ret=%d\n", ret);
        exit(-1);
    }

    /* Declare the publishable events */
    ret = FTB_Declare_publishable_events(handle, 0, event_info, 1);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    open_hca();
    for (i = 0; i < 12; i++) {
        if (ibv_query_port(ibv_dev.context, 2, &port_attr)) {
            fprintf(stderr, "Error Querying the Port Status\n");
            exit(1);
        }

        /*
         * If the status of the port is active, throw an FTB event
         * referring to port status as ACTIVE
         */
        if (IBV_PORT_ACTIVE == port_attr.state) {
            FTB_event_handle_t ehandle;

            printf("FTB_Publish_event\n");
            ret = FTB_Publish(handle, "IB_PORT_ACTIVE", NULL, &ehandle);
            if (ret != FTB_SUCCESS) {
                printf("FTB_Publish_event did not return a success return=%d\n", ret);
                exit(-1);
            }
            printf("sleeping for a couple of seconds now ...\n");
            sleep(2);
        }
    }
    printf("FTB_Disconnect\n");
    FTB_Disconnect(handle);

    printf("End\n");
    return 0;
}
