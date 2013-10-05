
#define _GNU_SOURCE
#include <stdio.h>
#include <zmq.h>
#include <czmq.h>
#include <inttypes.h>
#include <json/json.h>

#include "zmsg.h"
#include "route.h"
#include "cmbd.h"
#include "log.h"
#include "util.h"
#include "plugin.h"
#include "kvns.h"

#if 0
zlist_t * kvs_jobid_list (plugin_ctx_t *p)
{
    zlist_t *zl = NULL;
    json_object_iter i;
    json_object *val = NULL;
    json_object *reply = NULL;
    json_object *request = util_json_object_new_object ();

    json_object_object_add (request, "lwj.id-list", NULL);

    reply = plugin_request (p, request, "kvs.get.dir");
    if (!reply) {
        err ("kvs_job_list: plugin request failed!");
        goto done;
    }
    if (!(val = json_object_object_get (reply, "lwj.ids"))) {
        errno = ENOENT;
        err ("kvs_job_list: %s", strerror (errno));
        goto done;
    }
    if (!(zl = zlist_new ())) {
        errno = ENOMEM;
        goto done;
    }

    json_object_object_foreachC (val, i) {
        json_object *co;
        if ((co = json_object_object_get (i.val, "FILEVAL"))) {
            zlist_append (zl,
                strdup (json_object_to_json_string_ext (co,
                            JSON_C_TO_STRING_PLAIN)));
        }
    }

done:
    if (request)
        json_object_put (request);
    if (reply)
        json_object_put (reply);

    return (zl);
}

void jobid_list_free (zlist_t *list)
{
    void *s;
    while ((s = zlist_pop (list)))
        free (s);
    zlist_destroy (&list);
}

#endif


static int kvs_job_new (plugin_ctx_t *p, unsigned long jobid)
{
    struct kvns *ns;
    int rc;
    json_object *val = json_object_new_string ("reserved");

    ns = kvns_create_lwj_id (p, jobid);
    rc = kvns_put (ns, "state", val);
    json_object_put (val);
    plugin_kvs_commit(p);
    kvns_destroy (ns);
    return rc;
}

/*
 *  Set a new value for lwj.next-id and commit
 */
static int set_next_jobid (plugin_ctx_t *p, unsigned long jobid)
{
    int rc;
    json_object *o = json_object_new_int64 ((int64_t) jobid);

    if ((rc = plugin_kvs_put (p, "lwj.next-id", o)) < 0) {
        err ("plugin_kvs_put: %s", strerror (errno));
        return -1;
    }

    json_object_put (o);

    return plugin_kvs_commit (p);
}

/*
 *  Get and increment lwj.next-id (called from treeroot only)
 */
static unsigned long increment_jobid (plugin_ctx_t *p)
{
    unsigned long ret;
    json_object *v = NULL;
    int rc = plugin_kvs_get (p, "lwj.next-id", &v);
    if (rc < 0 && (errno == ENOENT))
        ret = 1;
    else
        ret = json_object_get_int64 (v);
    json_object_put (v);
    set_next_jobid (p, ret+1);
    return ret;
}

/*
 *  Tree-wide call for lwj_next_id. If not treeroot forward request
 *   up the tree. Otherwise increment jobid and return result.
 */
static unsigned long lwj_next_id (plugin_ctx_t *p)
{
    unsigned long ret;
    if (plugin_treeroot (p))
        ret = increment_jobid (p);
    else {
        json_object *na = json_object_new_object ();
        json_object *o = plugin_request (p, na, "job.next-id");
        if (util_json_object_get_int64 (o, "id", (int64_t *) &ret) < 0) {
            err ("lwj_next_id: Bad object!");
            ret = 0;
        }
        json_object_put (o);
        json_object_put (na);
    }
    return (ret);
}

/*
 *  create json object : { id: <int64>id }
 */
static json_object *json_id (int id)
{
    json_object *o = json_object_new_object ();
    util_json_object_add_int64 (o, "id", id);
    return (o);
}

static char * ctime_iso8601_now (char *buf, size_t sz)
{
    struct tm tm;
    time_t now = time (NULL);

    memset (buf, 0, sz);

    if (!localtime_r (&now, &tm))
        err_exit ("localtime");
    strftime (buf, sz, "%FT%T", &tm);

    return (buf);
}

static void add_jobinfo (plugin_ctx_t *p, int64_t id, json_object *req)
{
    char buf [64];
    json_object_iter i;
    json_object *o;
    kvns_t ns = kvns_create_lwj_id (p, id);

    json_object_object_foreachC (req, i)
        kvns_put (ns, i.key, i.val);

    o = json_object_new_string (ctime_iso8601_now (buf, sizeof (buf)));
    kvns_put (ns, "create-time", o);
    json_object_put (o);

    kvns_destroy (ns);
}

static void handle_recv (plugin_ctx_t *p, zmsg_t **zmsg, zmsg_type_t type)
{
    json_object *o = NULL;
    char *tag;

    if (cmb_msg_decode (*zmsg, &tag, &o) >= 0) {
        if (strcmp (tag, "job.next-id") == 0) {
            if (plugin_treeroot (p)) {
                unsigned long id = lwj_next_id (p);
                json_object *ox = json_id (id);
                plugin_send_response (p, zmsg, ox);
                json_object_put (o);
            }
            else {
                fprintf (stderr, "%s: forwarding request\n", tag);
                plugin_send_request (p, o, tag);
            }
        }
        if (strcmp (tag, "job.create") == 0) {
            json_object *jobinfo = NULL;
            unsigned long id = lwj_next_id (p);
            int rc = kvs_job_new (p, id);
            if (rc < 0) {
                plugin_send_response_errnum (p, zmsg, errno);
                goto out;
            }
            add_jobinfo (p, id, o);

            plugin_kvs_flush (p);
            plugin_kvs_commit (p);

            /* Generate reply with new jobid */
            jobinfo = util_json_object_new_object ();
            util_json_object_add_int64 (jobinfo, "jobid", id);
            plugin_send_response (p, zmsg, jobinfo);
            json_object_put (jobinfo);
        }
    }

out:
    if (o)
        json_object_put (o);
    zmsg_destroy (zmsg);
}

struct plugin_struct jobsrv = {
    .name = "job",
    .recvFn = handle_recv,
};

/*
 * vi: ts=4 sw=4 expandtab
 */
