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
#include <libgen.h>
#include <czmq/czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "shortjson.h"
#include "log.h"
#include "util.h"
#include "plugin.h"
#include "rdl.h"
#include "scheduler.h"

#define LS_RESERVED  "reserved"
#define LS_SUBMITTED "submitted"
#define LS_UNSCHED   "unsched"
#define LS_PENDING   "pending"
#define LS_RUNREQ    "runrequest"
#define LS_ALLOCATED "allocated"
#define LS_STARTING  "starting"
#define LS_RUNNING   "running"
#define LS_CANCELLED "cancelled"
#define LS_COMPLETE  "complete"
#define LS_REAPED    "reaped"
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
static long event_count;
static struct rdllib *l = NULL;
static struct rdl *rdl = NULL;;


/****************************************************************
 *
 *         Resource Description Library Setup
 *
 ****************************************************************/
static void f_err (flux_t h, const char *msg, ...)
{
    va_list ap;
    va_start (ap, msg);
    flux_vlog (h, LOG_ERR, msg, ap);
    va_end (ap);
}

/* XXX: Borrowed from flux.c and subject to change... */
static void setup_rdl_lua (void)
{
    char *s;
    char  exe_path [MAXPATHLEN];
    char *exe_dir;

    memset (exe_path, 0, MAXPATHLEN);
    if (readlink ("/proc/self/exe", exe_path, MAXPATHLEN - 1) < 0)
        err_exit ("readlink (/proc/self/exe)");
    exe_dir = dirname (exe_path);

    s = getenv ("LUA_CPATH");
    setenvf ("LUA_CPATH", 1, "%s/dlua/?.so;%s", exe_dir, s ? s : ";;");
    s = getenv ("LUA_PATH");
    setenvf ("LUA_PATH", 1, "%s/dlua/?.lua;%s", exe_dir, s ? s : ";;");

    flux_log (h, LOG_DEBUG, "LUA_PATH %s", getenv ("LUA_PATH"));
    flux_log (h, LOG_DEBUG, "LUA_CPATH %s", getenv ("LUA_CPATH"));

    rdllib_set_default_errf (h, (rdl_err_f)(&f_err));
}

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

    lwj_p = (queue_t *) xzmalloc (sizeof (queue_t));
    lwj_c = (queue_t *) xzmalloc (sizeof (queue_t));
    event = (queue_t *) xzmalloc (sizeof (queue_t));

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
    if (kvs_put_int64 (h, "event-counter", ++event_count) < 0 ) {
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
        re = j_reserved;
    }
    else if (strcmp (s, LS_SUBMITTED) == 0) {
        re = j_submitted;
    }
    if (strcmp (s, LS_UNSCHED) == 0) {
        re = j_unsched;
    }
    else if (strcmp (s, LS_PENDING) == 0) {
        re = j_pending;
    }
    else if (strcmp (s, LS_RUNREQ) == 0) {
        re = j_runrequest;
    }
    else if (strcmp (s, LS_ALLOCATED) == 0) {
        re = j_allocated;
    }
    else if (strcmp (s, LS_STARTING) == 0) {
        re = j_starting;
    }
    else if (strcmp (s, LS_RUNNING) == 0) {
        re = j_running;
    }
    else if (strcmp (s, LS_CANCELLED) == 0) {
        re = j_cancelled;
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
 *         Scheduler Activities
 *
 ****************************************************************/
static int
load_resources()
{
    int rc = -1;

    setup_rdl_lua ();

    if ((l = rdllib_open ())) {
        if ((rdl = rdl_loadfile (l, "conf/hype.lua"))) {
            rc = 0;
        } else {
            flux_log (h, LOG_ERR, "failed to load rdl file");
        }
    } else {
        flux_log (h, LOG_ERR, "failed to open rdl lib");
    }

    return rc;
}

/*
 * Assumes that the required resources are available, so walk the tree
 * find the required resources and change their state to "allocated".
 */
static int
allocate_resources (struct resource *r, const char *uri, flux_lwj_t *job)
{
    const char *type;
    int rc = -1;
    json_object *o;
    struct resource *c;

    rdl_resource_iterator_reset (r);
    while ((c = rdl_resource_next_child (r))) {
        o = rdl_resource_json (r);
        Jget_str (o, "type", &type);
        if  (strcmp (type, "node") == 0) {
            flux_log (h, LOG_INFO, "found a node: %s",
                      json_object_to_json_string (o));
            rc = 0;
        } else {
            flux_log (h, LOG_INFO, "this is not a node: %s",
                      json_object_to_json_string (o));
        }
        json_object_put (o);
        rdl_resource_destroy (c);
    }
    return rc;
}

static int
update_job(flux_lwj_t *job)
{
    int rc = -1;
/* Add the allocated resources to the job and change its state to
   "allocated" ( or perhaps "runrequest" ) */
    return rc;
}

int schedule_job (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
    int nodes;
    int rc = -1;
    json_object *o;
    uint64_t reqnodes = job->req.nnodes;
    struct resource *r = rdl_resource_get (rdl, uri);
    if (r == NULL) {
        flux_log (h, LOG_ERR, "Failed to get resource `%s'\n", uri);
        return rc;
    }

    o = rdl_resource_aggregate_json (r);
    if (!util_json_object_get_int (0, "nodes", &nodes)) {
        if (nodes >= reqnodes) {
            if (!allocate_resources(r, uri, job))
                rc = update_job(job);
        }
    }

    return rc;
}

int schedule_jobs (struct rdl *rdl, const char *uri, flux_lwj_t *jobs)
{
    flux_lwj_t *job;
    int rc = -1;

    while ((job = jobs)) {
        schedule_job(rdl, uri, job);
        jobs++;
    }
    return rc;
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
    case j_reserved:
        /* ignore this state transition for now ... */
        break;

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
        flux_log (h, LOG_ERR, "unknown lwj state %d", e->lwj->state);
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

    event_count = 0;
    if (kvs_put_int64 (h, "event-counter", event_count) < 0 ) {
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
        flux_log (h, LOG_ERR, "watch event-counter: %s", 
		          strerror (errno));
	    rc = -1;
        goto ret;
    } 
    flux_log (h, LOG_DEBUG, "registered event callback");

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
    flux_log (h, LOG_DEBUG, "registered lwj creation callback");

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
    flux_log (h, LOG_DEBUG, "registered lwj %s.state change callback", path);

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
        /* Ignore ENOENT.  It is expected when this cb is called right
         * after registration.
         */
        if (errnum != ENOENT) {
            flux_log (h, LOG_ERR, 
                      "in lwjstate_cb key(%s), val(%s): %s", 
                      key, val, strerror (errnum));
        }
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

/* The val argument is for the *next* job id.  Hence, the job id of
 * the new job will be (val - 1).
 */
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
    } else {
        flux_log (h, LOG_DEBUG, "newlwj_cb key(%s), val(%ld)", key, val);
    }
    if ( !(j = (flux_lwj_t *) xzmalloc (sizeof (flux_lwj_t))) ) {
        flux_log (h, LOG_ERR, "oom");
        goto error;
    }
    if ( !(e = (flux_event_t *) xzmalloc (sizeof (flux_event_t))) ) {
        flux_log (h, LOG_ERR, "oom");
        goto error;
    }
    if (extract_lwjinfo (val - 1, j) == -1) {
        flux_log (h, LOG_ERR, 
                  "extracting lwj info failed");
        goto error;
    }
    snprintf (path, MAX_STR_LEN, "lwj.%ld", val - 1);
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
event_cb (const char *key, int64_t *val, void *arg, int errnum)
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
    if (load_resources () == -1) {
        flux_log (h, LOG_ERR, "failed to load resources: %s",
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

