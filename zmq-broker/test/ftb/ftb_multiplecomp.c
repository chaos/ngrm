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
 * This examples demonstrates how different components in a software stack
 * can all be FTB-enabled simulateously.
 *
 * Usage: ./ftb_multicomp
 * Description:
 * Client1, Client and Client3 act as different client in a software stack
 * with Client1 being the topmost and Client3 being the bottom-most.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftb.h"

/*************  Client3  ******************/

/*
 * Client3 subscribes to all events of severity info.
 * It does not publish any events
 */

FTB_client_handle_t Client3_ftb_handle;
FTB_subscribe_handle_t shandle3;

int Client3_Finalize()
{
    printf("Client3: FTB_Disconnect\n");
    FTB_Disconnect(Client3_ftb_handle);

    return 0;
}


int Client3_evt_handler(FTB_receive_event_t * evt, void *arg)
{
    printf("Client3 caught event: event_space: %s, severity: %s, event_name %s, from host %s, pid %d\n",
           evt->event_space, evt->severity, evt->event_name,
           evt->incoming_src.hostname, evt->incoming_src.pid);
    return 0;
}


int Client3_Func()
{
    sleep(1);
    return 0;
}


int Client3_Init()
{
    int ret = 0;
    FTB_client_t cinfo;

    /* Client3 calls FTB_Connect */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.MULTICOMP_COMP3");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NOTIFY");

    printf("Client3: FTB_Connect\n");
    ret = FTB_Connect(&cinfo, &Client3_ftb_handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed\n");
        exit(-1);
    }

    /* Client3 subscribes to events of severity info */
    printf("Client3: FTB_Subscribe \n");
    ret = FTB_Subscribe(&shandle3, Client3_ftb_handle, "severity=info", Client3_evt_handler, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed!\n");
        exit(-1);
    }

    return 0;
}

/*************  Client3 End ******************/

/*
 * Client2 forms the second component in this example software stack.
 * Client2 subscribes to events of name TEST_EVENT_1 (from client1) and gets them
 * using polling mechanism. It can throw only one info severity event named
 * TEST_EVENT_2
 *
 * Internally Client2_Init calls Client3_Init, Client2_Finalize calls
 * Client3_Finalize.
 *
 * Client2_PollandPublish polls for event and publishes its own event when
 * the poll succeeds
 *
 */

FTB_client_handle_t Client2_ftb_handle;
FTB_subscribe_handle_t shandle2;

int Client2_Finalize()
{
    Client3_Finalize();
    printf("Client2: FTB_Disconnect\n");
    FTB_Disconnect(Client2_ftb_handle);
    return 0;
}

int Client2_PollandPublish()
{
    FTB_receive_event_t evt;
    FTB_event_handle_t ehandle;
    int ret = 0;

    Client3_Func();
    while (1) {
        ret = FTB_Poll_event(shandle2, &evt);
        if (ret == FTB_GOT_NO_EVENT) {
            break;
        }
        printf
            ("Client2 caught event: event_space: %s, severity: %s, event_name %s, from host %s, pid %d\n",
             evt.event_space, evt.severity, evt.event_name, evt.incoming_src.hostname,
             evt.incoming_src.pid);
    }
    printf("Client2: FTB_Publish\n");
    FTB_Publish(Client2_ftb_handle, "TEST_EVENT_2", NULL, &ehandle);
    return 0;
}

int Client2_Init()
{
    FTB_client_t cinfo;
    FTB_event_info_t event_info[1] = { {"TEST_EVENT_2", "INFO"} };
    int ret = 0;

    /* Client 2 specifies its information and calls FTB_Connect */
    printf("Client2: FTB_Connect\n");
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.MULTICOMP_COMP2");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_POLLING");

    ret = FTB_Connect(&cinfo, &Client2_ftb_handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed\n");
        exit(-1);
    }

    /* Client2 declares its publishable event */
    ret = FTB_Declare_publishable_events(Client2_ftb_handle, 0, event_info, 1);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    /* Client2 subscribes to event */
    printf("Client2: FTB_Subscribe via polling\n");
    ret = FTB_Subscribe(&shandle2, Client2_ftb_handle, "event_name=TEST_EVENT_1", NULL, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed!\n");
        exit(-1);
    }
    Client3_Init();
    return 0;
}

/*************  Client2 End ***************/


/*************  Client1 Start  ******************/

/*
 * Client1 can be considered a topmost client(or component) in this example
 * software stack.
 *
 * Client1 can publish only one event called TEST_EVENT_1 (sev:info). It can catch all
 * events of severity fatal, which it manages using the Client1_evt_handler.
 * Client1_Init() internally calls Client2_Init(), Client1_Finalize()
 * internally calls Client2_Finalize() and Client1_Publish internally calls
 * Client2_Publish
 *
 */

FTB_client_handle_t Client1_ftb_handle;
FTB_subscribe_handle_t shandle1;

int Client1_Finalize()
{
    Client2_Finalize();
    printf("Client1: Called Client2_Finalize and my own FTB_Disconnect\n");
    FTB_Disconnect(Client1_ftb_handle);
    return 0;
}


int Client1_evt_handler(FTB_receive_event_t * evt, void *arg)
{
    printf("Client1 caught event: event_space: %s, severity: %s, event_name %s, from host %s, pid %d\n",
           evt->event_space, evt->severity, evt->event_name, evt->incoming_src.hostname,
           evt->incoming_src.pid);
    return 0;
}


int Client1_Publish()
{
    static int i = 0;
    FTB_event_handle_t ehandle;

    i++;
    if (i % 5 == 0) {
        printf("Client1: FTB_Publish\n");
        FTB_Publish(Client1_ftb_handle, "TEST_EVENT_1", NULL, &ehandle);
    }
    Client2_PollandPublish();
    return 0;
}

int Client1_Init()
{
    FTB_client_t cinfo;
    int ret = 0;
    FTB_event_info_t event_info[1] = { {"TEST_EVENT_1", "INFO"} };

    /* Specify Client1 properties and call FTB_Connect */
    memset(&cinfo, 0, sizeof(cinfo));
    strcpy(cinfo.event_space, "FTB.FTB_EXAMPLES.MULTICOMP_COMP1");
    strcpy(cinfo.client_schema_ver, "0.5");
    strcpy(cinfo.client_subscription_style, "FTB_SUBSCRIPTION_NOTIFY");
    printf("Client1: FTB_Connect\n");
    ret = FTB_Connect(&cinfo, &Client1_ftb_handle);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Connect failed\n");
        exit(-1);
    }

    /* Declare the event to be thrown by Client1 */
    ret = FTB_Declare_publishable_events(Client1_ftb_handle, 0, event_info, 1);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Declare_publishable_events failed ret=%d!\n", ret);
        exit(-1);
    }

    /*
     * Client1 catches all events of severity fatal and uses a callback
     * handler to handle received events
     */
    printf("Client1: FTB_Subscribe \n");
    ret = FTB_Subscribe(&shandle1, Client1_ftb_handle, "severity=fatal", Client1_evt_handler, NULL);
    if (ret != FTB_SUCCESS) {
        printf("FTB_Subscribe failed!\n");
        exit(-1);
    }

    Client2_Init();
    return 0;
}

/*************  Client1 End ***************/


int main(int argc, char *argv[])
{
    int i;

    if (argc > 1) {
        if (strcasecmp(argv[1], "usage") == 0) {
            printf("./ftb_multiplecomp\n");
            exit(0);
        }
    }
    Client1_Init();
    for (i = 0; i < 40; i++) {
        /* Ask client1 to publish its events */
        Client1_Publish();
    }
    Client1_Finalize();
    return 0;
}
