/*
 *-------------------------------------------------------------------------------
 * Copyright and authorship blurb here
 *-------------------------------------------------------------------------------
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
#include <dlfcn.h>

#include "zmsg.h"
#include "shortjson.h"
#include "log.h"
#include "util.h"
#include "plugin.h"
#include "rdl.h"
#include "scheduler.h"

#define LS_NULL      "null"
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
    char *rdllib;

    memset (exe_path, 0, MAXPATHLEN);
    if (readlink ("/proc/self/exe", exe_path, MAXPATHLEN - 1) < 0)
        err_exit ("readlink (/proc/self/exe)");
    exe_dir = dirname (exe_path);

    s = getenv ("LUA_CPATH");
    setenvf ("LUA_CPATH", 1, "%s/dlua/?.so;%s", exe_dir, s ? s : ";");
    s = getenv ("LUA_PATH");
    setenvf ("LUA_PATH", 1, "%s/dlua/?.lua;%s", exe_dir, s ? s : ";");

    flux_log (h, LOG_DEBUG, "LUA_PATH %s", getenv ("LUA_PATH"));
    flux_log (h, LOG_DEBUG, "LUA_CPATH %s", getenv ("LUA_CPATH"));

    asprintf (&rdllib, "%s/lib/librdl.so", exe_dir);
    if (!dlopen (rdllib, RTLD_NOW | RTLD_GLOBAL)) {
        flux_log (h, LOG_ERR, "dlopen %s failed", rdllib);
        return;
    }
    free(rdllib);

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
find_lwj (int64_t id)
{
    flux_lwj_t *j = NULL;

    queue_iterator_reset (p_queue);
    while ( (j = queue_next (p_queue)) != NULL) {
        if (j->lwj_id == id)
            break;
    }

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
extract_lwjid (const char *k, int64_t *i)
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
    *i = strtoul(id, (char **) NULL, 10);

ret:
    return rc;
}


static lwj_state_e
translate_state (const char *s)
{
    lwj_state_e re = j_for_rent;

    if (strcmp (s, LS_NULL) == 0) {
        re = j_null;
    }
    else if (strcmp (s, LS_RESERVED) == 0) {
        re = j_reserved;
    }
    else if (strcmp (s, LS_SUBMITTED) == 0) {
        re = j_submitted;
    }
    else if (strcmp (s, LS_UNSCHED) == 0) {
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
        flux_log (h, LOG_ERR, "Unknown state %s", s);
    }

    return re;
}


static int
extract_lwjinfo (flux_lwj_t *j)
{
    char *key;
    char *state;
    int64_t reqnodes = 0;
    int64_t reqtasks = 0;

    if (asprintf (&key, "lwj.%ld.state", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo state key create failed");
    } else if (kvs_get_string (h, key, &state) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo %s: %s", key, strerror (errno));
    } else {
        j->state = translate_state(state);
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %s", key, state);
        free(key);
    }

    if (asprintf (&key, "lwj.%ld.nnodes", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo nnodes key create failed");
    } else if (kvs_get_int64 (h, key, &reqnodes) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
    } else {
        j->req.nnodes = reqnodes;
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %ld", key, reqnodes);
        free(key);
    }

    if (asprintf (&key, "lwj.%ld.ntasks", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo ntasks key create failed");
    } else if (kvs_get_int64 (h, key, &reqtasks) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
    } else {
        /* Assuming a 1:1 relationship right now between cores and tasks */
        j->req.ncores = reqtasks;
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %ld", key, reqtasks);
        free(key);
    }

    j->alloc.nnodes = 0;
    j->alloc.ncores = 0;

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
 * Walk the tree, find the required resources and tag with the lwj_id
 * to which it is allocated.
 */
static bool
allocate_resources (struct resource *r, flux_lwj_t *job)
{
    const char *type;
    json_object *o;
    struct resource *c;
    bool found = false;

    o = rdl_resource_json (r);
    flux_log (h, LOG_DEBUG, "considering resource: %s",
              json_object_to_json_string (o));

    Jget_str (o, "type", &type);
    if  (job->req.nnodes && (strcmp (type, "node") == 0)) {
        job->req.nnodes--;
        job->alloc.nnodes++;
        util_json_object_add_int64(o, "lwj", job->lwj_id);
        flux_log (h, LOG_DEBUG, "allocated node: %s",
                  json_object_to_json_string (o));
    } else if  (job->req.ncores && (strcmp (type, "core")) == 0) {
        job->req.ncores--;
        job->alloc.ncores++;
        util_json_object_add_int64(o, "lwj", job->lwj_id);
        flux_log (h, LOG_DEBUG, "allocated core: %s",
                  json_object_to_json_string (o));
    }
    json_object_put (o);

    found = !(job->req.nnodes || job->req.ncores);

    while (!found && (c = rdl_resource_next_child (r))) {
        found = allocate_resources (c, job);
        rdl_resource_destroy (c);
    }

    return (found);
}

/*
 * Add the allocated resources to the job and change its state to
 * "allocated"
 */
static int
update_job(flux_lwj_t *job)
{
    int rc = -1;

/* LEFT OFF HERE */

    return rc;
}

int schedule_job (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
    int64_t nodes;
    int rc = -1;
    json_object *o;
    uint64_t reqnodes = 0;
    struct resource *r = rdl_resource_get (rdl, uri);

    if (r == NULL) {
        flux_log (h, LOG_ERR, "Failed to get resource `%s'\n", uri);
        return rc;
    }

    o = rdl_resource_aggregate_json (r);
    if (o) {
        rc = util_json_object_get_int64 (o, "node", &nodes);
        if (rc) {
            flux_log (h, LOG_ERR, "schedule_job failed to get nodes: %d", rc);
        } else {
            flux_log (h, LOG_DEBUG, "schedule_job found %ld nodes", nodes);
        }
        json_object_put (o);
    }

    if (job) {
        reqnodes = job->req.nnodes;
        if (nodes >= reqnodes) {
            if (allocate_resources(r, job))
                rc = update_job(job);
        }
    } else {
        flux_log (h, LOG_ERR, "schedule_job passed a null job");
    }

    return rc;
}

int schedule_jobs (struct rdl *rdl, const char *uri, queue_t *jobs)
{
    flux_lwj_t *job = NULL;
    int rc = -1;

    job = (flux_lwj_t *)zlist_first (jobs->queue);
    jobs->rewind = 1;

    if (job)
        schedule_job(rdl, uri, job);

    while ( (job = zlist_next (jobs->queue)) != NULL) {
        schedule_job(rdl, uri, job);
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
    /* e->lwj->state is the current state
     * e->ev.je      is the new state
     */
    flux_log (h, LOG_DEBUG, "attempting job %ld state change from %d to %d",
              e->lwj->lwj_id, e->lwj->state, e->ev.je);
    switch (e->lwj->state) {
    case j_null:
        if (e->ev.je == j_submitted) {
            extract_lwjinfo (e->lwj);
            if (e->lwj->state != e->ev.je) {
                flux_log (h, LOG_ERR,
                          "job %ld read state mismatch ", e->lwj->lwj_id);
                goto bad_transition;
            }
            e->lwj->state = j_submitted;
            flux_log (h, LOG_DEBUG, "setting %ld to submitted state",
                      e->lwj->lwj_id);
            schedule_jobs (rdl, "default", return_queue (p_queue));
        }
        break;

    case j_reserved:
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
        flux_log (h, LOG_ERR, "job %ld unknown state %d",
                  e->lwj->lwj_id, e->lwj->state);
        break;
    }

    return 0;

bad_transition:
    flux_log (h, LOG_ERR, "job %ld bad state transition from %u to %u",
              e->lwj->lwj_id, e->lwj->state, e->ev.je);
    return -1;
}


static int
action_r_event (flux_event_t *e)
{
    int rc = -1;

    if ((e->ev.je == r_released) || (e->ev.re == r_attempt)) {
        schedule_jobs (rdl, "default", return_queue (p_queue));
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
    int64_t lwj_id;
    flux_lwj_t *j = NULL;
    lwj_event_e e;

    if (errnum > 0) {
        /* Ignore ENOENT.  It is expected when this cb is called right
         * after registration.
         */
        if (errnum != ENOENT) {
            flux_log (h, LOG_ERR, "lwjstate_cb key(%s), val(%s): %s",
                      key, val, strerror (errnum));
        }
        goto ret;
    }

    if (extract_lwjid (key, &lwj_id) == -1) {
        flux_log (h, LOG_ERR, "ill-formed key");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "lwjstate_cb: %ld, %s", lwj_id, val);

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
    j->lwj_id = val - 1;
    j->state = j_null;
    snprintf (path, MAX_STR_LEN, "lwj.%ld", j->lwj_id);
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

    return;

error:
    if (j)
        free (j);

    return;
}


static void
event_cb (const char *key, int64_t val, void *arg, int errnum)
{
    flux_event_t *e = NULL;

    if (errnum > 0) {
        /* Ignore ENOENT.  It is expected when this cb is called right
         * after registration.
         */
        if (errnum != ENOENT) {
            flux_log (h, LOG_ERR, "event_cb key(%s), val(%ld): %s",
                      key, val, strerror (errnum));
        }
        goto ret;
    }

    while ( (e = dequeue (ev_queue)) != NULL) {
        action (e);
        free (e);
    }
ret:
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
    flux_log (h, LOG_INFO, "sched comms module starting");

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
    if (reg_event_hdlr ((KVSSetInt64F*) event_cb) == -1) {
        flux_log (h, LOG_ERR,
                  "register event handling callback: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }
    if (wait_for_lwj_init () == -1) {
        flux_log (h, LOG_ERR, "wait for lwj failed: %s",
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

