/*
 *--------------------------------------------------------------------------------
 * Copyright and authorship blurb here
 *--------------------------------------------------------------------------------
 * schedsrv.h - common scheduler services
 *
 * Update Log:
 *       May 24 2012 DHA: File created.
 */ 

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "log.h"
#include "util.h"
#include "plugin.h"
#include "scheduler.h"

#define LS_RESERVED "reserved"
#define LS_PENDING  "pending"
#define LS_RUNREQ   "runrequest"
#define LS_STARTING "starting"
#define LS_RUNNING  "running"
#define LS_COMPLETE "complete"
#define LS_REAPED   "reaped"
#define MAX_STR_LEN 128

/****************************************************************
 *
 *                 INTERNAL DATA STRUCTURE
 *
 ****************************************************************/

/**
 *  Enumerate different queue types
 */
typedef enum {
    p_queue,
    c_queue,
    ev_queue
} queue_e;


/**
 *  Define a queue
 */
typedef struct {
    zlist_t *queue;
    int rewind;
} queue_t;


/****************************************************************
 *
 *                 STATIC DATA
 *
 ****************************************************************/
static queue_t *lwj_p = NULL;
static queue_t *lwj_c = NULL;
static queue_t *event = NULL;
static flux_t h = NULL;


/****************************************************************
 *
 *              Queue Abstraction on Top of zlist
 *
 ****************************************************************/
static int 
init_internal_queues () 
{
    int rc = 0;

    if (lwj_p || lwj_c || event) { 
        rc = -1;
        goto ret;
    }

    lwj_p->queue = zlist_new ();
    lwj_c->queue = zlist_new ();
    event->queue = zlist_new ();

    if (!lwj_p->queue || !lwj_c->queue || !event->queue) { 
        rc = -1;
        goto ret;
    }
    lwj_p->rewind = 0; 
    lwj_c->rewind = 0; 
    event->rewind = 0; 

ret:
    return rc;
}


#if 0 /* comment this in when this function is called */
static int 
destroy_internal_queues () 
{
    if (lwj_p->queue) {
        zlist_destroy (&lwj_p->queue);
        lwj_p->queue = NULL;
    }
    if (lwj_c->queue) { 
        zlist_destroy (&lwj_c->queue);
        lwj_c->queue = NULL;
    }
    if (event->queue) {
        zlist_destroy (&event->queue);
        event->queue = NULL;
    }
    return 0;
}
#endif


static queue_t *
return_queue (queue_e t)
{
    queue_t *rq = NULL;

    switch (t) {
    case p_queue:
        rq = lwj_p;
        break;

    case c_queue:
        rq = lwj_c;       
        break;

    case ev_queue:
        rq = event;       
        break;

    default:
        flux_log (h, LOG_ERR, "unknown queue!");

        break;
    }

    return rq;
}


static int
enqueue (queue_e t, void * item)
{
    int rc = 0;
    queue_t *q = return_queue (t);

    if (!q) {
        flux_log (h, LOG_ERR, "null queue!");
        rc = -1;
        goto ret;
    }
    if (zlist_append (q->queue, item) == -1) {
        flux_log (h, LOG_ERR, "failed to enqueu!");
        rc = -1;
    }

ret:
    return rc;
}


static void *
dequeue (queue_e t)
{
    void *item = NULL;
    queue_t *q = return_queue (t);

    if (!q) {
        flux_log (h, LOG_ERR, "null queue!");
        goto ret;
    }

    item = zlist_pop (q->queue);

ret:
    return item;
}


static int 
queue_iterator_reset (queue_e t)
{
    int rc = 0;
    queue_t *q = return_queue (t);

    if (!q) {
        flux_log (h, LOG_ERR, "null queue!");
        rc = -1;
        goto ret;
    }

    q->rewind = 0;

ret:
    return rc;
    
}


static void *
queue_next (queue_e t)
{
    void *item = NULL;
    queue_t *q = return_queue (t);

    if (!q) {
        flux_log (h, LOG_ERR, "null queue!");
        goto ret;
    }

    if (q->rewind == 0) {
       item = zlist_first (q->queue); 
       q->rewind = 1;
    }
    else {
       item = zlist_next (q->queue);
    }

ret:
    return item;
}


static void 
queue_remove (queue_e t, void *item)
{
    queue_t *q = return_queue (t);

    if (!q) {
        flux_log (h, LOG_ERR, "null queue!");
        goto ret;
    }

    zlist_remove (q->queue, item);

ret:
    return;
}


static int
signal_event ( )
{
    int rc = 0;
    if (kvs_put_int64 (h, "event-counter", 0) < 0 ) {
        flux_log (h, LOG_ERR,
            "error kvs_put_int64 event-counter: %s",
            strerror (errno));
        rc = -1;
        goto ret;
    }
    if (kvs_commit (h) < 0) {
        flux_log (h, LOG_ERR, "kvs_commit error!");
        rc = -1;
        goto ret;
    }

ret:
    return rc;
}


static flux_lwj_t *
find_lwj (uint64_t id)
{
    flux_lwj_t *j = NULL;
    while ( (j = queue_next (p_queue)) != NULL) {
        if (j->lwj_id == id) 
            break;
    }
    queue_iterator_reset (p_queue);
    return j;
}


/****************************************************************
 * 
 *              Utility Functions
 *
 ****************************************************************/

static inline void
set_event (flux_event_t *e, 
           event_class_e c, int ei, flux_lwj_t *j)
{
    e->t = c;
    e->lwj = j;
    switch (c) {
        case lwj_event:
            e->ev.je = (lwj_event_e) ei;
            break;
        case res_event:
            e->ev.re = (res_event_e) ei;
            break;
        default:
            flux_log (h, LOG_ERR, "unknown ev class");
            break;
    }
    return;
}


static int 
extract_lwjid (const char *k, uint64_t *i)
{
    int rc = 0;
    char *kcopy = NULL;
    char *lwj = NULL;
    char *id = NULL;

    if (!k) {
        rc = -1;
        goto ret;
    }

    kcopy = strdup (k);
    lwj = strtok (kcopy, ".");
    if (strncmp(lwj, "lwj", 3) != 0) {
        rc = -1;
        goto ret;
    }
    id = strtok (NULL, ".");
    *i = strtol(id, (char **) NULL, 10);
     
ret:
    return rc;
}


static lwj_state_e 
translate_state (const char *s)
{
    lwj_state_e re = j_for_rent;

    if (strcmp (s, LS_RESERVED) == 0) {
        re = j_submitted;
    }
    else if (strcmp (s, LS_PENDING) == 0) {
        re = j_unsched;
    }
    else if (strcmp (s, LS_RUNREQ) == 0) {
        re = j_runrequest;
    }
    else if (strcmp (s, LS_STARTING) == 0) {
        re = j_starting;
    }
    else if (strcmp (s, LS_RUNNING) == 0) {
        re = j_running;
    }
    else if (strcmp (s, LS_COMPLETE) == 0) {
        re = j_complete;
    }
    else if (strcmp (s, LS_REAPED) == 0) {
        re = j_reaped;
    }
    else {
        flux_log (h, LOG_ERR, "Unknown state "); 
    }

    return re;
}


static int 
extract_lwjinfo (uint64_t val, flux_lwj_t *j)
{
    //char *state;

    /* TODO: extract from "lwj.val" lwj info */
    //j->state = translate_state (state);
    return 0;
}


static void
genev_kvs_st_chng (lwj_event_e e, flux_lwj_t *j)
{
    flux_event_t *ev 
        = (flux_event_t *) xzmalloc (sizeof (flux_event_t));
    ev->t = lwj_event;
    ev->ev.je = e;
    ev->lwj = j;
    if (enqueue (ev_queue, (void *) ev) == -1) {
        flux_log (h, LOG_ERR, 
                  "enqueuing an event failed");
        goto ret;
    }
    if (signal_event () == -1) {
        flux_log (h, LOG_ERR, 
                  "signaling an event failed");
        goto ret;
    }

ret:
    return;
}


/****************************************************************
 *
 *         Actions Led by Current State + an Event
 *
 ****************************************************************/

static int 
request_run (flux_lwj_t *lwj)
{
    int rc = 0;
    char kv[MAX_STR_LEN];
    snprintf (kv, MAX_STR_LEN, "lwj.%ld.state", lwj->lwj_id);    
    if (kvs_put_string (h, (const char *) kv, "request_run") < 0) {
        flux_log (h, LOG_ERR, "kvs_put error!");
        rc = -1;
        goto ret;
    }
    if (kvs_commit (h) < 0) {
        flux_log (h, LOG_ERR, "kvs_commit error!");
        rc = -1;
        goto ret;
    }
    lwj->state = j_runrequest;

ret:
   return rc;
}


static int 
release_res (flux_lwj_t *lwj)
{
    int rc = 0;
    flux_event_t *newev 
        = (flux_event_t *) xzmalloc (sizeof (flux_event_t));

    // TODO: how to update the status of each entry as "free"
    // then destroy zlist_t without having to destroy
    // the elements
    // release lwj->resource

    newev->t = res_event;
    newev->ev.re = r_released; 
    newev->lwj = NULL;

    if (enqueue (ev_queue, (void *) newev) == -1) {
        flux_log (h, LOG_ERR,
                  "enqueuing an event failed");
        rc = -1;
        goto ret;
    }
    if (signal_event () == -1) {
        flux_log (h, LOG_ERR,
                  "signal the event-enqueued event ");
        rc = -1;
        goto ret;
    }

ret:
    return rc;
}


static int
move_to_c_queue (flux_lwj_t *lwj)
{
    queue_remove (p_queue, (void *) lwj);
    return enqueue (c_queue, (void *) lwj);
}


static int
action_j_event (flux_event_t *e)
{
    switch (e->lwj->state) {
    case j_submitted:
        if (e->ev.je != j_unsched) {
           goto bad_transition;
        }
        // how can schedule_jobs generates j_pending or j_allocated??
        // TODO: schedule_jobs (/* pending queue, resource, e->lwj */);
        // for now, it should enqueue (ev_queue, e) and signal_event()
        break;

    case j_unsched:
        if (e->ev.je == j_pending) {
            e->lwj->state = j_pending;
        }
        else if (e->ev.je == j_allocated) {
            e->lwj->state = j_allocated;
        }
        else {
            goto bad_transition;
        }
        break;

    case j_allocated:
        if (e->ev.je != j_runrequest) {
           goto bad_transition;
        }
        request_run(e->lwj);
        break;

    case j_runrequest:
        if (e->ev.je != j_starting) {
           goto bad_transition;
        }
        e->lwj->state = j_starting;
        break;

    case j_starting:
        if (e->ev.je != j_running) {
           goto bad_transition;
        }
        e->lwj->state = j_running;
        break;

    case j_running:
        if (e->ev.je != j_complete) {
           goto bad_transition;
        }
        release_res (e->lwj);
        break;

    case j_complete:
        if (e->ev.je != j_reaped) {
           goto bad_transition;
        }
        move_to_c_queue (e->lwj);
        break;

    case j_pending:
        goto bad_transition;
        break;

    default:
        flux_log (h, LOG_ERR, "unknown lwj state");
        break;
    }

    return 0;

bad_transition:
    flux_log (h, LOG_ERR, "bad transition");
    return -1;
}


static int
action_r_event (flux_event_t *e)
{
    int rc = -1;

    if ((e->ev.je == r_released) 
        || (e->ev.re == r_attempt)) {
        //TODO: schedule_jobs ()
        rc = 0;
    }

    return rc;
}


static int
action (flux_event_t *e)
{
    int rc = 0;

    switch (e->t) {
    case lwj_event:
        rc = action_j_event (e);
        break;

    case res_event:
        rc = action_r_event (e);
        break;
 
    default:
        flux_log (h, LOG_ERR, "unknown event type");
        break;
    }

    return rc;
}


/****************************************************************
 *
 *         Abstractions for KVS Callback Registeration 
 *
 ****************************************************************/
static int 
wait_for_lwj_init ()
{
    int rc = 0;
    kvsdir_t dir = NULL;

    if (kvs_watch_once_dir (h, &dir, "lwj") < 0) {
        flux_log (h, LOG_ERR, "wait_for_lwj_init: %s", 
                  strerror (errno));
        rc = -1;
        goto ret;
    } 
     
    flux_log (h, LOG_INFO, "wait_for_lwj_init %s", 
              kvsdir_key(dir));

ret:
    if (dir)
        kvsdir_destroy (dir);
    return rc;
}


static int
reg_event_hdlr (KVSSetInt64F *func)
{
    int rc = 0;
    if (kvs_put_int64 (h, "event-counter", 0) < 0 ) {
        flux_log (h, LOG_ERR, 
            "error kvs_put_int64 event-counter: %s",
            strerror (errno));
        rc = -1;
        goto ret;
    }
    if (kvs_commit (h) < 0) {
        flux_log (h, LOG_ERR, "kvs_commit error!");
        rc = -1;
        goto ret;
    }
    if (kvs_watch_int64 (h, "event-counter", func, (void *) h) < 0) {
        flux_log (h, LOG_ERR, "watch event-counte: %s", 
		          strerror (errno));
	    rc = -1;
        goto ret;
    } 

ret:
    return rc;
}


static int 
reg_newlwj_hdlr (KVSSetInt64F *func)
{    
    if (kvs_watch_int64 (h,"lwj.next-id", func, (void *) h) < 0) {
        flux_log (h, LOG_ERR, "watch lwj.next-id: %s", 
		          strerror (errno));
	    return -1;
    } 

    flux_log (h, LOG_INFO, 
	          "lwj creation handler registered.");

    return 0;
}


static int 
reg_lwj_state_hdlr (const char *path, KVSSetStringF *func)
{
    int rc = 0;
    char *k = NULL;
   
    asprintf (&k, "%s.state", path);
    if (kvs_watch_string (h, k, func, (void *)h) < 0) {
	    flux_log (h, LOG_ERR, 
		    "watch a lwj state in %s: %s.", 
		    k, strerror (errno));
	    rc = -1;
	    goto ret;
    }

    flux_log (h, LOG_INFO, 
	          "lwj state state change handler registered.");

ret:
    free (k);
    return rc;
}


/****************************************************************
 *                KVS Watch Callback Functions
 ****************************************************************/
static void 
lwjstate_cb (const char *key, const char *val, void *arg, int errnum)
{
    uint64_t lwj_id;
    flux_lwj_t *j = NULL;
    lwj_event_e e;

    if (errnum > 0) {
        flux_log (h, LOG_ERR, 
                  "in newlwj_cb key(%s), val(%s): %s", 
                  key, val, strerror (errnum));
        goto ret;
    }
    
    if (extract_lwjid (key, &lwj_id) == -1) {
        flux_log (h, LOG_ERR, "ill-formed key");
        goto ret;
    }

    j = find_lwj (lwj_id);
    if (j) {
       e = translate_state (val);
       genev_kvs_st_chng (e, j);
    }

ret:
    return;    
}


static void 
newlwj_cb (const char *key, int64_t val, void *arg, int errnum)
{
    char path[MAX_STR_LEN];
    flux_lwj_t *j = NULL;
    flux_event_t *e = NULL;

    if (errnum > 0 || val < 0) {
        flux_log (h, LOG_ERR, 
                  "in newlwj_cb key(%s), val(%ld): %s", 
                  key, val, strerror (errnum));
        goto error;
    }
    if ( !(j = (flux_lwj_t *) xzmalloc (sizeof (flux_lwj_t))) ) {
        flux_log (h, LOG_ERR, "oom");
        goto error;
    }
    if ( !(e = (flux_event_t *) xzmalloc (sizeof (flux_event_t))) ) {
        flux_log (h, LOG_ERR, "oom");
        goto error;
    }
    if (extract_lwjinfo (val, j) == -1) {
        flux_log (h, LOG_ERR, 
                  "extracting lwj info failed");
        goto error;
    }
    snprintf (path, MAX_STR_LEN, "lwj.%ld", val);
    if (reg_lwj_state_hdlr (path, (KVSSetStringF *) lwjstate_cb) == -1) {
        flux_log (h, LOG_ERR, 
                  "register lwj state change "
                  "handling callback: %s",    
                  strerror (errno));
        goto error;
    }
    if (enqueue (p_queue, (void *) j) == -1) {
        flux_log (h, LOG_ERR, 
                  "appending a job to pending queue failed");
        goto error;
    }
    set_event (e, lwj_event, j_submitted, j);
    if (enqueue (ev_queue, (void *) e) == -1) {
        flux_log (h, LOG_ERR, 
                  "appending a job to event queue failed");
        goto error;
    }
    if (signal_event () == -1) {
        flux_log (h, LOG_ERR, 
                  "signal the event enqueue event ");
        goto error;
    }

    return; 

error:
    if (j)
        free (j);
    if (e)
        free (e);

    return;    
}


static void 
event_cb (const char *key, const char *val, void *arg, int errnum)
{
    flux_event_t *e = NULL;

    while ( (e = dequeue (ev_queue)) != NULL) {
        action (e);
        free (e);
    }

    return;
}


/****************************************************************
 *
 *        High Level Job and Resource Event Handlers 
 *
 ****************************************************************/
static int
schedsvr_main (flux_t p, zhash_t *args)
{
    /* TODO: MAKE SURE THIS IS ONLY ACTIVATED IN ONE CMB RANK */
    int rc = 0;

    h = p;
    flux_log_set_facility (h, "schedsvr");
    flux_log (h, LOG_INFO, "sched plugin starting"); 

    if (wait_for_lwj_init () == -1) {
        flux_log (h, LOG_ERR, "wait for lwj failed: %s",    
                  strerror (errno));
        rc = -1;
        goto ret; 
    }
    if (init_internal_queues () == -1) {
        flux_log (h, LOG_ERR, 
                  "init for queues failed: %s",    
                  strerror (errno));
        rc = -1;
        goto ret;
    }
    if (reg_newlwj_hdlr ((KVSSetInt64F*) newlwj_cb) == -1) {
        flux_log (h, LOG_ERR, 
                  "register new lwj handling "
                  "callback: %s",    
                  strerror (errno));
        rc = -1;
        goto ret;
    }
    if (reg_event_hdlr ((KVSSetInt64F *) event_cb) == -1) {
        flux_log (h, LOG_ERR, 
                  "register event handling callback: %s",    
                  strerror (errno));
        rc = -1;
        goto ret;
    }
    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, 
                  "flux_reactor_start: %s", 
                  strerror (errno));
        rc =  -1;
        goto ret;
    }

ret:
    return rc;
}


/****************************************************************
 *
 *                 EXTERNAL FUNCTIONS 
 *
 ****************************************************************/

const struct plugin_ops ops = {
    .main = schedsvr_main,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */

