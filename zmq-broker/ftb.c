/* ftb.c - implement CiFTS Fault Tolerance Backplane v0.5 */

#define _GNU_SOURCE
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdarg.h>
#include <json/json.h>

#include "flux.h"
#include "log.h"
#include "ftb/libftb.h"
#include "cmb.h"

int FTB_Connect (const FTB_client_t *client_info,
                FTB_client_handle_t *client_handle)
{
    int rc = -1;
    return rc;
}

int FTB_Publish (FTB_client_handle_t client_handle, const char *event_name,
                 const FTB_event_properties_t *event_properties,
                 FTB_event_handle_t * event_handle)
{
    int rc = -1;
    return rc;
}

int FTB_Subscribe (FTB_subscribe_handle_t *subscribe_handle,
                   FTB_client_handle_t client_handle,
                   const char *subscription_str,
                   int (*callback) (FTB_receive_event_t *, void *), void *arg)
{
    int rc = -1;
    return rc;
}

int FTB_Unsubscribe (FTB_subscribe_handle_t *subscribe_handle)
{
    int rc = -1;
    return rc;
}

int FTB_Declare_publishable_events (FTB_client_handle_t client_handle,
                                    const char *schema_file,
                                    const FTB_event_info_t *event_info,
                                    int num_events)
{
    int rc = -1;
    return rc;
}

int FTB_Poll_event (FTB_subscribe_handle_t shandle,
                    FTB_receive_event_t *receive_event)
{
    int rc = -1;
    return rc;
}

int FTB_Disconnect (FTB_client_handle_t client_handle)
{
    int rc = -1;
    return rc;
}

int FTB_Get_event_handle (const FTB_receive_event_t receive_event,
                          FTB_event_handle_t * event_handle)
{
    int rc = -1;
    return rc;
}

int FTB_Compare_event_handles (const FTB_event_handle_t event_handle1,
                               const FTB_event_handle_t event_handle2)
{
    int rc = -1;
    return rc;
}


/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
