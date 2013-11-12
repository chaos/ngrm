/* apicli.c - flux_t implementation for UNIX domain socket */

#define _GNU_SOURCE
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <libgen.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/param.h>
#include <stdbool.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <ctype.h>
#include <stdarg.h>
#include <json/json.h>
#include <czmq.h>

#include "log.h"
#include "zmsg.h"
#include "cmb.h"
#include "util.h"
#include "flux.h"
#include "flux_handle.h"

typedef struct {
    int fd;
    int rank;
    int size;
    zlist_t *resp;
} cmb_t;

static int cmb_request_sendmsg (cmb_t *c, zmsg_t **zmsg)
{
    return zmsg_send_fd (c->fd, zmsg);
}

static int cmb_response_recvmsg (cmb_t *c, zmsg_t **zmsg, bool nonblock)
{
    zmsg_t *z;

    if (!(z = zlist_pop (c->resp)) && !(z = zmsg_recv_fd (c->fd, nonblock)))
        return -1;
    *zmsg = z;
    return 0;
}

static int cmb_response_putmsg (cmb_t *c, zmsg_t **zmsg)
{
    if (zlist_append (c->resp, *zmsg) < 0)
        return -1;
    *zmsg = NULL;
    return 0;
}

/* If 'o' is NULL, there will be no json part,
 *   unlike flux_request_send() which will fill in an empty JSON part.
 */
static int cmb_request_send (cmb_t *c, json_object *o, const char *fmt, ...)
{
    zmsg_t *zmsg;
    char *tag;
    int rc;
    va_list ap;

    va_start (ap, fmt);
    if (vasprintf (&tag, fmt, ap) < 0)
        oom ();
    va_end (ap);
    zmsg = cmb_msg_encode (tag, o);
    free (tag);
    if ((rc = cmb_request_sendmsg (c, &zmsg)) < 0)
        zmsg_destroy (&zmsg);

    return rc;
}

static int cmb_snoop_subscribe (cmb_t *c, const char *s)
{
    return cmb_request_send (c, NULL, "api.snoop.subscribe.%s", s ? s: "");
}

static int cmb_snoop_unsubscribe (cmb_t *c, const char *s)
{
    return cmb_request_send (c, NULL, "api.snoop.unsubscribe.%s", s ? s: "");
}

static int cmb_event_subscribe (cmb_t *c, const char *s)
{
    return cmb_request_send (c, NULL, "api.event.subscribe.%s", s ? s: "");
}

static int cmb_event_unsubscribe (cmb_t *c, const char *s)
{
    return cmb_request_send (c, NULL, "api.event.unsubscribe.%s", s ? s: "");
}

static int cmb_event_sendmsg (cmb_t *c, zmsg_t **zmsg)
{
    int rc;
    json_object *o = NULL;
    char *tag = NULL;

    if (cmb_msg_decode (*zmsg, &tag, &o) < 0)
        return -1;
    rc = cmb_request_send (c, o, "api.event.send.%s", tag ? tag : "");
    if (rc == 0)
        zmsg_destroy (zmsg);
    if (tag)
        free (tag);
    if (o)
        json_object_put (o);
    return rc;
}

static int cmb_rank (cmb_t *c)
{
    return c->rank;
}

static int cmb_size (cmb_t *c)
{
    return c->size;
}

static int cmb_session_info_query (cmb_t *c)
{
    json_object *request = util_json_object_new_object ();
    json_object *response = NULL;
    zmsg_t *zmsg = NULL;
    int errnum;
    int rc = -1;

    if (cmb_request_send (c, request, "api.session.info.query") < 0)
        goto done;
    if (cmb_response_recvmsg (c, &zmsg, false) < 0)
        goto done;
    if (cmb_msg_decode (zmsg, NULL, &response) < 0 || !response) {
        errno = EPROTO;
        goto done;
    }
    if (util_json_object_get_int (response, "errnum", &errnum) == 0) {
        errno = errnum;
        goto done;
    }
    if (util_json_object_get_int (response, "rank", &c->rank) < 0
            || util_json_object_get_int (response, "size", &c->size) < 0) {
        errno = EPROTO;
        goto done;
    }
    rc = 0;
done:
    if (request)
        json_object_put (request);
    if (response)
        json_object_put (response);
    return rc;
}

static void cmb_fini (cmb_t **cp)
{
    if (cp && *cp) {
        if ((*cp)->fd >= 0)
            (void)close ((*cp)->fd);
        free (*cp);
        *cp = NULL;
    }
}

flux_t cmb_init_full (const char *path, int flags)
{
    flux_t h;
    cmb_t *c = NULL;
    struct sockaddr_un addr;

    c = xzmalloc (sizeof (*c));
    if (!(c->resp = zlist_new ()))
        oom ();
    c->fd = socket (AF_UNIX, SOCK_STREAM, 0);
    if (c->fd < 0)
        goto error;
    memset (&addr, 0, sizeof (struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    strncpy (addr.sun_path, path, sizeof (addr.sun_path) - 1);

    if (connect (c->fd, (struct sockaddr *)&addr,
                         sizeof (struct sockaddr_un)) < 0)
        goto error;
    if (cmb_session_info_query (c) < 0)
        goto error;

    h = flux_handle_create (c, (FluxFreeFn *)cmb_fini, flags);
    h->request_sendmsg = (FluxRequestSendMsg *)cmb_request_sendmsg;
    h->response_recvmsg = (FluxResponseRecvMsg *)cmb_response_recvmsg;
    h->response_putmsg = (FluxResponsePutMsg *)cmb_response_putmsg;
    h->event_sendmsg = (FluxEventSendMsg *)cmb_event_sendmsg;
    h->event_recvmsg = (FluxEventRecvMsg *)cmb_response_recvmsg; /* FIXME */
    h->event_subscribe = (FluxEventSub *)cmb_event_subscribe;
    h->event_unsubscribe = (FluxEventUnsub *)cmb_event_unsubscribe;
    h->snoop_recvmsg = (FluxSnoopRecvMsg *)cmb_response_recvmsg; /* FIXME */
    h->snoop_subscribe = (FluxSnoopSub *)cmb_snoop_subscribe;
    h->snoop_unsubscribe = (FluxSnoopUnsub *)cmb_snoop_unsubscribe;
    h->rank = (FluxRank *)cmb_rank;
    h->size = (FluxSize *)cmb_size;
    return h;
error:
    cmb_fini (&c);
    return NULL;
}

flux_t cmb_init (void)
{
    const char *val;
    char path[PATH_MAX + 1];

    if ((val = getenv ("CMB_API_PATH"))) {
        if (strlen (val) > PATH_MAX) {
            err ("Crazy value for CMB_API_PATH!");
            return (NULL);
        }
        strcpy(path, val);
    }
    else
        snprintf (path, sizeof (path), CMB_API_PATH_TMPL, getuid ());
    return cmb_init_full (path, 0);
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
