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
 * This example component demonstrates a logger using the notification
 * subscription style
 *
 * Usage: ./ftb_notify_logger [filename or -]
 * Run ./ftb_notify_logger usage for usage information
 *
 * Description:
 * This loggers subscribes to all events, but does not publish any event in
 * its lifetime
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <string.h>

#include "ftb.h"

#define LOG_FILE "/tmp/ftb_log"

static volatile int done = 0;

void Int_handler(int sig)
{
    if (sig == SIGINT)
        done = 1;
}


int event_logger(FTB_receive_event_t * evt, void *arg)
{
    FILE *log_fp = (FILE *) arg;
    time_t current = time(NULL);
    char buffer[26];

    fprintf(log_fp,
            "Current Time: %sEvent Caught with eventspace: %s, severity: %s, event_name: %s, client_name: %s, from host: %s, client_jobid: %s, seqnum: %d\n\n",
            ctime_r(&current, buffer), evt->event_space, evt->severity, evt->event_name,
            evt->client_name, evt->incoming_src.hostname, evt->client_jobid, evt->seqnum);
    fflush(log_fp);

    return 0;
}


int main(int argc, char *argv[])
{
    FILE *log_fp = NULL;
    FTB_client_t cinfo;
    FTB_client_handle_t handle;
    FTB_subscribe_handle_t *shandle = (FTB_subscribe_handle_t *) malloc(sizeof(FTB_subscribe_handle_t));
    int ret = 0;

    if (argc >= 2) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf
                ("Usage: ./ftb_notify_logger [option]\nOptions:\n\t\t- : Display on screen\n\t filename : Log into the file\n\t If above options are not specified, data is logged in default file specified in the ftb_notify_logger.c code\n");
            exit(0);
        }
        else if (!strcmp("-", argv[1])) {
            fprintf(stderr, "Using stdout for output\n");
            log_fp = stdout;
        }
        else {
            fprintf(stderr, "Using %s as log file\n", argv[1]);
            log_fp = fopen(argv[1], "w");
        }
    }
    else {
        fprintf(stderr, "Using %s as log file\n", LOG_FILE);
        log_fp = fopen(LOG_FILE, "w");
    }

    if (log_fp == NULL) {
        fprintf(stderr, "Failed to open file %s\n", argv[1]);
        return -1;
    }

    /* Specify the client information */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.NOTIFY_LOGGER");
    strcpy(cinfo.client_name, "notify");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NOTIFY");

    /* Connect to FTB */
    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed \n");
        exit(-1);
    }

    /*
     * Subscribe to all event using empty subscription string.
     * event_logger function is executed when event arrives.
     * event_logger function uses the file specified by log_fp to log events
     */
    ret = FTB_Subscribe(shandle, handle, "", event_logger, (void *) log_fp);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed\n");
        exit(-1);
    }
    signal(SIGINT, Int_handler);
    /*
     * Continue this till the done variable is set by the event_logger
     * function
     */
    while (!done) {
        sleep(5);
    }

    /* Disconnect from FTB */
    FTB_Disconnect(handle);
    fclose(log_fp);
    return 0;
}
