/* ftb.c - implement CiFTS Fault Tolerance Backplane v0.5 API */

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
#include "ftb.h"
#include "cmb.h"

#define CLIENT_HANDLE_MAGIC 0x4345eeee
struct FTB_client_handle {
    int magic;
    FTB_client_t cinfo;
    flux_t f;
    pthread_t cb_t; /* thread for callbacks */
};

static FTB_client_handle_t create_handle (const FTB_client_t *cinfo)
{
    FTB_client_handle_t h;

    if (!(h = malloc (sizeof (*h)))) {
        errno = ENOMEM;
        return NULL;
    }
    h->magic = CLIENT_HANDLE_MAGIC;
    memcpy (&h->cinfo, cinfo, sizeof (h->cinfo));
    if (!(h->f = cmb_init ())) {
        free (h);
        return NULL;
    }
    return h;
}

static void destroy_handle (FTB_client_handle_t h)
{
    assert (h->magic == CLIENT_HANDLE_MAGIC);
    flux_handle_destroy (&h->f);
    free (h);
}

int FTB_Connect (const FTB_client_t *client_info, /* IN */
                FTB_client_handle_t *client_handle) /* OUT */
{
    FTB_client_handle_t h;
    int rc = FTB_ERR_GENERAL;

    if (!(h = create_handle (client_info)))
        goto done;

    *client_handle = h;
    rc = FTB_SUCCESS;
done:
    return rc;
}

int FTB_Disconnect (FTB_client_handle_t h)
{
    destroy_handle (h);

    return FTB_SUCCESS;
}


int FTB_Publish (FTB_client_handle_t h, const char *event_name,
                 const FTB_event_properties_t *event_properties,
                 FTB_event_handle_t * event_handle)
{
    int rc = FTB_SUCCESS;
    assert (h->magic == CLIENT_HANDLE_MAGIC);
    return rc;
}

int FTB_Subscribe (FTB_subscribe_handle_t *subscribe_handle,
                   FTB_client_handle_t h,
                   const char *subscription_str,
                   int (*callback) (FTB_receive_event_t *, void *), void *arg)
{
    int rc = FTB_SUCCESS;
    assert (h->magic == CLIENT_HANDLE_MAGIC);
    return rc;
}

int FTB_Unsubscribe (FTB_subscribe_handle_t *subscribe_handle)
{
    int rc = FTB_SUCCESS;
    return rc;
}

int FTB_Declare_publishable_events (FTB_client_handle_t h,
                                    const char *schema_file,
                                    const FTB_event_info_t *event_info,
                                    int num_events)
{
    int rc = FTB_SUCCESS;
    assert (h->magic == CLIENT_HANDLE_MAGIC);
    return rc;
}

int FTB_Poll_event (FTB_subscribe_handle_t shandle,
                    FTB_receive_event_t *receive_event)
{
    int rc = FTB_SUCCESS;
    return rc;
}

int FTB_Get_event_handle (const FTB_receive_event_t receive_event,
                          FTB_event_handle_t * event_handle)
{
    int rc = FTB_SUCCESS;
    return rc;
}

int FTB_Compare_event_handles (const FTB_event_handle_t event_handle1,
                               const FTB_event_handle_t event_handle2)
{
    int rc = FTB_SUCCESS;
    return rc;
}


/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
