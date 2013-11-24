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
 * This example component demonstrates a polling logger.
 *
 * Usage: ./ftb_polling_logger [filename or -]
 * Run ./ftb_polling_logger usage for more information
 *
 * Description:
 * The polling logger subscribes to "all" events using the "Polling"
 * subscription style. It does not publish any events during its lifetime.
 *
 * The default log file, where the polled data is stored, is
 * specified by LOG_FILE argument. The user can specified his/her own
 * polling file
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


int main(int argc, char *argv[])
{
    FILE *log_fp = NULL;
    FTB_client_handle_t handle;
    FTB_subscribe_handle_t shandle;
    FTB_client_t cinfo;
    int ret;
    char buffer[26];

    if (argc >= 2) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf
                ("Usage: ./ftb_polling_logger [option]\nOptions:\n\t\t- : Display on screen\n\t filename : Log into the file\n\t If above options are not specified, data is logged in default file specified in the ftb_polling_logger.c code\n");
            exit(0);
        }
        else if ((strcmp("-", argv[1]) == 0)) {
            fprintf(stderr, "Using stdout as log file\n");
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
        fprintf(stderr, "failed to open file %s\n", argv[1]);
        return -1;
    }
    /* Specify the event space and other details of the client */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.polling_LOGGER");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_POLLING");

    /* Connect to FTB */
    ret = FTB_Connect(&cinfo, &handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed \n");
        exit(-1);
    }

    /* Subscribe to all events by specifying an empty subscription string */
    ret = FTB_Subscribe(&shandle, handle, "", NULL, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed\n");
        exit(-1);
    }

    signal(SIGINT, Int_handler);
    while (1) {
        FTB_receive_event_t event;
        int ret = 0;

        /* Poll for an event matching "" subscription string */
        ret = FTB_Poll_event(shandle, &event);
        if (ret == FTB_GOT_NO_EVENT) {
            time_t current = time(NULL);
            fprintf(log_fp, "Current Time: %sNo Event Caught\n\n", ctime_r(&current, buffer));
            fflush(log_fp);
            sleep(5);
        }
        else {
            time_t current = time(NULL);
            fprintf(log_fp,
                    "Current Time: %sEvent Caught with eventspace: %s, severity: %s, event_name: %s, client_name: %s, from host: %s, client_jobid: %s, seqnum: %d\n\n",
                    ctime_r(&current, buffer), event.event_space, event.severity, event.event_name,
                    event.client_name, event.incoming_src.hostname, event.client_jobid, event.seqnum);
            fflush(log_fp);
        }
        if (done)
            break;
    }

    /* Disconnect from FTB */
    FTB_Disconnect(handle);
    fclose(log_fp);

    return 0;
}
