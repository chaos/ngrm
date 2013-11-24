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
 * Usage: Server: ./ftb_eventhandles_example server
 *        Client: ./ftb_eventhandles_example client
 *
 * Description:
 * In this example, the client sends an "unknown_error" event. The server
 * responds to that particular event with a response event of name
 * "everything_ok". The client then detects this response and exits.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include "ftb.h"

static volatile int done = 0;
static int is_server = 0;
FTB_subscribe_handle_t shandle;

void Sig_handler(int sig)
{
    if (sig == SIGINT || sig == SIGTERM)
        done = 1;
}

/*
 *  When the server receives an "unknown_error" event, it sends an
 *  "everything_ok" event, along with the event_handle of the
 *  "unknown_error" event. This is suppossed to indicate that the
 *  "everything_ok" event is a response to the "unknown_error" event.
 *  The server exits after publishing the "unknown_error" event.
 *  All other events are ignored by the server.
 */
int eventhandle_server(FTB_receive_event_t * evt, void *arg)
{
    FTB_event_handle_t ehandle, ehandle1;
    FTB_client_handle_t *handle = (FTB_client_handle_t *) arg;
    FTB_event_properties_t *eprop = (FTB_event_properties_t *) malloc(sizeof(FTB_event_properties_t));
    int ret = 0, i = 0;

    printf("In eventhandle_server callback handler\n");

    if (strcasecmp(evt->event_name, "unknown_error") == 0) {
        printf("Got an event with memory errors. Send recovery event\n");
    }
    else
        return 0;

    /* Set the event properties for response event */
    eprop->event_type = 2;
    ret = FTB_Get_event_handle(*evt, &ehandle1);
    if (ret != FTB_SUCCESS) {
        fprintf(stderr, "FTB_Get_event_handle failed %d", ret);
        exit(-1);
    }
    memcpy(&eprop->event_payload, (char *) &ehandle1, sizeof(FTB_event_handle_t));

    /* The server publishes the event 3 times every 1 second */
    for (i = 0; i < 3; i++) {
        ret = FTB_Publish(*handle, "everything_ok", eprop, &ehandle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Publish at server side ret=%d\n", ret);
        }
        sleep(1);
    }

    /* Unsubscribe and exit now */
    ret = FTB_Unsubscribe(&shandle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Unsubscribe at server side ret=%d\n", ret);
    }
    done = 1;
    return 0;
}


/*
 * The client checks for event_type=2(indicates response event).
 * If the event name is "everything_ok", it compares the received handle to
 * its original "memory_error" published handle. If there is a match it
 * exits; else it returns
 */
int eventhandle_client(FTB_receive_event_t * evt, void *arg)
{
    FTB_event_handle_t ehandle;
    FTB_event_handle_t *ehandle_orig = (FTB_event_handle_t *) arg;
    int ret = 0;
    printf("In eventhandle_client callback handler\n");

    if (evt->event_type == 2) {
        memcpy(&ehandle, &evt->event_payload, sizeof(FTB_event_handle_t));
    }

    if (strcasecmp(evt->event_name, "everything_ok") == 0) {
        ret = FTB_Compare_event_handles(ehandle, *ehandle_orig);
        if (ret != FTB_SUCCESS) {
            fprintf(stderr, "FTB_Get_event_handle failed %d", ret);
            exit(-1);
        }
        printf("Got \"everything ok\" to my error event\n");
        printf("Exiting\n");
        done = 1;
    }
    return 0;
}


int main(int argc, char *argv[])
{
    FTB_client_handle_t handle;
    FTB_client_t cinfo;
    FTB_event_handle_t ehandle;
    int ret = 0;

    if (argc == 2) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf
                ("For server: ./ftb_eventhandle_example server \nFor client: ./ftb_eventhandle_example client\n");
            exit(0);
        }
        else if (strcasecmp(argv[1], "client") == 0) {
            is_server = 0;
            fprintf(stderr, "Starting client\n");
        }
        else if (strcasecmp(argv[1], "server") == 0) {
            is_server = 1;
            fprintf(stderr, "Starting server\n");
        }
        else {
            fprintf(stderr, "Wrong usage..Exiting\n");
            exit(-1);;
        }
    }
    else {
        fprintf(stderr, "Wrong usage..Exiting\n");
        fprintf(stderr,
                "For server: ./ftb_eventhandle_example server \nFor client: ./ftb_eventhandle_example client\n");
        exit(-1);;
    }

    /*
     * Specify the client properties and call FTB_Connect.
     * Note that the pingpong component will subscribe to events using the
     * notification subscription mechanism
     */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.ftb_eventhandle");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NOTIFY");
    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect is not successful ret=%d\n", ret);
        exit(-1);
    }

    /*
     * This component will publish two events, as defined in the event_info
     * structure below
     */
    FTB_event_info_t event_info[2] = { {"everything_ok", "info"}
    , {"unknown_error", "fatal"}
    };
    ret = FTB_Declare_publishable_events(handle, 0, event_info, 2);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    if (is_server) {
        ret = FTB_Subscribe(&shandle, handle, "severity=fatal", eventhandle_server, (void *) &handle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Subscribe failed!\n");
            exit(-1);
        }
        signal(SIGINT, Sig_handler);
        signal(SIGTERM, Sig_handler);
    }
    else {
        ret = FTB_Publish(handle, "unknown_error", NULL, &ehandle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Publish failed\n");
            exit(-1);
        }

        ret =
            FTB_Subscribe(&shandle, handle, "event_name=everything_ok", eventhandle_client,
                          (void *) &ehandle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Subscribe failed!\n");
            exit(-1);
        }
    }
    while (!done) {
        sleep(1);
    }
    FTB_Disconnect(handle);
    return 0;
}
