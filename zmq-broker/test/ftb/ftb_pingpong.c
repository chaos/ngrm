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
 * The pingpong component is an example of FTB-enabled multi-threading component
 *
 * Usage: Server: ./ftb_pingong server
 *        Client: ./ftb_pingpong client iterations
 *
 * Description:
 * In this example, the server side and the client side exchange events
 * with each other for the specified number of iterations. Both the client
 * and the server subscribe to events using notification callback functions.
 *
 * Pingpong Operation:
 * 1. Client sends an event to the server
 * 2. Server gets the client event and sends its own event as an answer
 * 3. Client gets the server event and sends another client event
 * 4. Steps 2 and 3 are repeated for the specified number of iterations
 *
 * Subscription as well as publishing of events are through callback
 * functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include "ftb.h"

static volatile int done = 0;
static struct timeval begin, end;
static int is_server = 0;
static int count = 0;
static int iter = 0;

void Sig_handler(int sig)
{
    if (sig == SIGINT || sig == SIGTERM)
        done = 1;
}


/* This is the callback handler called by the server side */
int pingpong_server(FTB_receive_event_t * evt, void *arg)
{
    count++;
    FTB_event_handle_t ehandle;
    FTB_client_handle_t *handle = (FTB_client_handle_t *) arg;

    printf("pingpong_server callback handler received event (%d)\n", count);
    /*
     * Server publishes its event in response to the recived client event,
     * that trigerred this callback
     */
    FTB_Publish(*handle, "PINGPONG_EVENT_SRV", NULL, &ehandle);
    return 0;
}


/* This is the callback handler called by the client side */
int pingpong_client(FTB_receive_event_t * evt, void *arg)
{
    FTB_event_handle_t ehandle;

    count++;
    printf("pingpong_client callback handler received event (%d)\n", count);
    if (count >= iter) {
        gettimeofday(&end, NULL);
        done = 1;
        return 0;
    }
    FTB_client_handle_t *handle = (FTB_client_handle_t *) arg;

    /*
     * Client publishes its events in response to the received server-side event,
     * which trigerred this callback
     */
    FTB_Publish(*handle, "PINGPONG_EVENT_CLI", NULL, &ehandle);
    return 0;
}


int main(int argc, char *argv[])
{
    FTB_client_handle_t handle;
    FTB_client_t cinfo;
    FTB_subscribe_handle_t shandle;
    FTB_event_handle_t ehandle;
    double event_latency;
    int ret = 0;

    if (argc >= 2) {
        if (strcasecmp(argv[1], "server") == 0) {
            if (argc > 2) {
                fprintf(stderr, "Starting pingpong server. Ignoring additional arguments.\n");
            }
            else {
                fprintf(stderr, "Starting pingpong server\n");
                is_server = 1;
            }
        }
        else if (strcasecmp(argv[1], "client") == 0) {
            if (argc >= 3) {
                is_server = 0;
                int i = 0;
                for (i = 0; i < strlen(argv[2]); i++) {
                    if ((argv[2][i] >= '0') && (argv[2][i] <= '9'))
                        continue;
                    else {
                        printf("Pingpong iterations not a valid number\n");
                        exit(0);
                    }
                }
                iter = atoi(argv[2]);
                if (iter < 1) {
                    printf("Pingpong iterations cannot be less than 1\n");
                    exit(0);
                }
                if (argc > 3) {
                    fprintf(stderr,
                            "Starting pingpong client with iterations %d. Ignoring additional arguments\n",
                            iter);
                }
                else {
                    fprintf(stderr, "Starting pingpong client with iterations %d\n", iter);
                }
            }
            else if (argc < 3) {
                fprintf(stderr, "Number of iterations missing from the client pingpong program\n");
                exit(0);
            }
        }
        else {
            printf
                ("For pingpong server: ./ftb_pingpong server\nFor pingpong client: ./ftb_pingpong client <number of iterations>\n");
            exit(0);
        }
    }
    else {
        printf
            ("For pingpong server: ./ftb_pingpong server\nFor pingpong client: ./ftb_pingpong client <number of iterations>\n");
        exit(0);
    }

    /*
     * Specify the client properties and call FTB_Connect.
     * Note that the pingpong component will subscribe to events using the
     * notification subscription mechanism
     */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.Pingpong");
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
    FTB_event_info_t event_info[2] = { {"PINGPONG_EVENT_SRV", "INFO"}
    , {"PINGPONG_EVENT_CLI", "INFO"}
    };
    ret = FTB_Declare_publishable_events(handle, 0, event_info, 2);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    if (is_server) {
        /*
         * The pingong server subscribes for all event of name
         * PINGPONG_EVENT_CLI and calls the pingpong_server function to
         * handle these events
         */
        ret =
            FTB_Subscribe(&shandle, handle, "event_name=PINGPONG_EVENT_CLI", pingpong_server,
                          (void *) &handle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Subscribe failed!\n");
            exit(-1);
        }
        signal(SIGINT, Sig_handler);
        signal(SIGTERM, Sig_handler);
    }
    else {
        /*
         * The pingpong client subscribes to all events of event name
         * PINGPONG_EVENT_SRV and calls a function pingpong_client callback
         * function to handle these events
         */
        ret =
            FTB_Subscribe(&shandle, handle, "event_name=PINGPONG_EVENT_SRV", pingpong_client,
                          (void *) &handle);
        if (ret != FTB_SUCCESS) {
            printf("FTB_Subscribe failed!\n");
            exit(-1);
        }
        gettimeofday(&begin, NULL);
        /*
         * After subscription, the client throws the first PINGPONG_EVENT_CLI
         * event. Subsequent events will be thrown by the client and server
         * callback function
         */
        FTB_Publish(handle, "PINGPONG_EVENT_CLI", NULL, &ehandle);
    }

    while (!done) {
        sleep(1);
    }

    /* Calculate the latency at the client side */
    if (!is_server) {
        event_latency = (end.tv_sec - begin.tv_sec) * 1000000 + (end.tv_usec - begin.tv_usec);
        event_latency /= iter;
        printf("Latency: %.3f microseconds\n", event_latency);
    }

    FTB_Disconnect(handle);

    return 0;
}
