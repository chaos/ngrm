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
 * This simple subscriber component catches events of event name: SIMPLE_EVENT.
 *
 * Usage: ./ftb_simple_subscriber
 *
 * Description:
 * It does not publish any events
 * This examples should be used in conjuction with the
 * simple_event_publisher.c component, which throws SIMPLE_EVENTS events
 */
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>

#include "ftb.h"

int main(int argc, char *argv[])
{
    FTB_client_handle_t handle;
    int i = 0, ret = 0;
    FTB_client_t cinfo;
    FTB_subscribe_handle_t shandle;

    if (argc > 1) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf("./ftb_simple_subscriber");
            exit(0);
        }
    }
    printf("Begin\n");
    /* Specify the client information and call FTB_Connect */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.SIMPLE");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_POLLING");

    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect was not successful\n");
        exit(-1);
    }
    /*
     * Subscribe to all events of name SIMPLE_EVENT
     * This event will be called using polling mechanism
     */
    ret = FTB_Subscribe(&shandle, handle, "event_name=SIMPLE_EVENT", NULL, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed!\n");
        exit(-1);
    }
    /* Get the first 10 events only */
    for (i = 0; i < 10; i++) {
        int ret;
        FTB_receive_event_t event;

        printf("Sleeping for 10 seconds..\n\n");
        sleep(10);
        while (1) {
            printf("Calling FTB_Poll_event\n");
            /* Poll for an event */
            ret = FTB_Poll_event(shandle, &event);
            if (ret == FTB_SUCCESS) {
                printf
                    ("Caught event: event_space: %s, severity: %s, event_name: %s from host: %s and pid: %d\n",
                     event.event_space, event.severity, event.event_name, event.incoming_src.hostname,
                     event.incoming_src.pid);
            }
            else {
                printf("No event\n");
                break;
            }
        }
    }
    printf("FTB_Disconnect\n");
    /* Disconnect from FTB */
    FTB_Disconnect(handle);
    printf("End\n");
    return 0;
}
