/* hbsrv.c - generate session heartbeat */

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
#include <sys/time.h>
#include <ctype.h>
#include <zmq.h>
#include <czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "util.h"
#include "log.h"
#include "plugin.h"

#define MAX_HEARTRATE   30*60.0

static int epoch = 0;
static int timer_id = -1;
static double heartrate = 1.0; /* seconds */

static int timeout_cb (flux_t h, void *arg)
{
    json_object *o = util_json_object_new_object ();
    int rc = -1;
    util_json_object_add_int (o, "epoch", ++epoch);
    if (flux_event_send (h, o, "hb") < 0) {
        err ("flux_event_send");
        goto done;
    }
    rc = 0;
done:
    json_object_put (o);
    return rc;
}

static void set_heartrate (const char *key, double val, void *arg, int errnum)
{
    flux_t h = arg;

    if (errnum != 0)
        return;
    if (val == NAN || val <= 0 || val > MAX_HEARTRATE) {
        flux_log (h, LOG_ERR, "%s: %.1f out of range (0 < sec < %.1f", key,
                  val, MAX_HEARTRATE);
        return;
    }
    if (val != heartrate) {
        heartrate = val;
        flux_tmouthandler_remove (h, timer_id);
        timer_id = flux_tmouthandler_add (h, (int)(heartrate * 1000),
                                          false, timeout_cb, NULL);
        if (timer_id < 0) {
            flux_log (h, LOG_ERR, "flux_tmouthandler_add: %s", strerror (errno));
            return;
        }
        flux_log (h, LOG_INFO, "heartrate set to %.1fs", heartrate);
    }
}

int mod_main (flux_t h, zhash_t *args)
{
    if (kvs_watch_double (h, "conf.hb.heartrate", set_heartrate, h) < 0) {
        flux_log (h, LOG_ERR, "kvs_watch_dir conf.hb: %s", strerror (errno));
        return -1;
    }
    timer_id = flux_tmouthandler_add (h, (int)(heartrate * 1000),
                                      false, timeout_cb, NULL);
    if (timer_id < 0) {
        flux_log (h, LOG_ERR, "flux_tmouthandler_add: %s", strerror (errno));
        return -1;
    }
    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
        return -1;
    }
    return 0;
}

MOD_NAME ("hb");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
