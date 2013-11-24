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
 * For certain FTB-routines, the user needs to be extra careful to ensure
 * that the routine remain thread-safe. An example of such an FTB routine is
 * FTB_Unsubscribe.
 *
 * This is an example of how FTB_Unsubscribe can be called from the callback
 * notification function.
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <pthread.h>

#include "ftb.h"


FTB_client_handle_t handle;

/*
 * The  callback_handle_recovery() gets one event, when called - and
 * unsubscribes the handle
 *
 * Any error may  occur if FTB_Disconnect is called before by the main thread
 * before the callback thread calls FTB_Unsubscribe or any other FTB
 * function. It is up to the user to make sure that this does not happen
 */

int callback_handle_recovery(FTB_receive_event_t * evt, void *arg)
{
    FTB_subscribe_handle_t *shandle = (FTB_subscribe_handle_t *) arg;
    int ret = 0;
    static int numtimes = 0;
    numtimes += 1;
    printf("Callback Function : Callback_handle_recovery called %d times\n", numtimes);
    ret = FTB_Unsubscribe(shandle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Unsubscribe failed with code %d\n", ret);
        return ret;
    }
    printf("In callback function: Unsubscribing the subscription string\n");
    return 0;
}


int main(int argc, char *argv[])
{
    FTB_client_t cinfo;
    int ret = 0;
    FTB_subscribe_handle_t *shandle = (FTB_subscribe_handle_t *) malloc(sizeof(FTB_subscribe_handle_t));
    FTB_event_info_t einfo[1] = { {"my_error_event", "warning"}
    };
    FTB_event_handle_t ehandle;
    int k = 0;

    if (argc > 1) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf("./ftb_example_callback_unsubscribe\n");
            exit(0);
        }
    }

    /* Call FTB_Connect with required information */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.MULTITHREAD_EXAMPLE");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NOTIFY");
    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed with return code = %d\n", ret);
        FTB_Disconnect(handle);
    }

    /* Declare the event to publish */
    ret = FTB_Declare_publishable_events(handle, 0, einfo, 1);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_Publishable_event failed with return code=%d \n", ret);
        FTB_Disconnect(handle);

    }

    /* Subscribe to its own event using a callback to handle the event */
    ret =
        FTB_Subscribe(shandle, handle, "event_name=my_error_event", callback_handle_recovery,
                      (void *) shandle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed with return code=%d\n", ret);
        FTB_Disconnect(handle);
    }

    /*
     * Publish some more events.
     * At some time, the callback will unsubscribe the subscription_str and
     * some of these events will not be caught
     */
    for (k = 0; k < 1; k++) {
        printf("Publishing event %d\n", k);
        ret = FTB_Publish(handle, "my_error_event", NULL, &ehandle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Publish_event failed with return code=%d\n", ret);
        }
    }

    /*
     * The sleep is a quick hack to ensure that the callback function
     * completes before FTB_Disconnect is called
     */
    sleep(10);
    printf("Disconnecting from FTB\n");
    FTB_Disconnect(handle);

    return 0;
}
