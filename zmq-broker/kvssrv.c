/* kvssrv.c - distributed key-value store based on hash tree */

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
#include <sys/time.h>
#include <zmq.h>
#include <czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "route.h"
#include "cmbd.h"
#include "plugin.h"
#include "util.h"
#include "log.h"

typedef struct {
    json_object *o;
    zlist_t *reqs;
} hobj_t;

typedef enum { OP_NAME, OP_STORE, OP_FLUSH } optype_t;
typedef struct {
    optype_t type;
    char *key;
    char *ref;
    zmsg_t *zmsg;
} op_t;

typedef struct {
    bool done;
    int rootseq;
    href_t rootdir;
    zlist_t *reqs;
} commit_t;

typedef struct {
    zhash_t *store;
    href_t rootdir;
    int rootseq;
    zlist_t *writeback;
    enum { WB_CLEAN, WB_FLUSHING, WB_DIRTY } writeback_state;
    zlist_t *commit_ops;
    zhash_t *commits;
} ctx_t;

static void event_kvs_setroot_send (plugin_ctx_t *p);
static void kvs_load (plugin_ctx_t *p, zmsg_t **zmsg);
static void kvs_get (plugin_ctx_t *p, zmsg_t **zmsg);

static commit_t *commit_create (void)
{
    commit_t *cp = xzmalloc (sizeof (*cp));
    if (!(cp->reqs = zlist_new ()))
        oom ();
    return cp;
}

static void commit_destroy (commit_t *cp)
{
    assert (zlist_size (cp->reqs) == 0);
    zlist_destroy (&cp->reqs);
    free (cp);
}

static commit_t *commit_new (plugin_ctx_t *p, const char *name)
{
    ctx_t *ctx = p->ctx;
    commit_t *cp = commit_create ();

    zhash_insert (ctx->commits, name, cp);
    zhash_freefn (ctx->commits, name, (zhash_free_fn *)commit_destroy);
    return cp;
}

static commit_t *commit_find (plugin_ctx_t *p, const char *name)
{
    ctx_t *ctx = p->ctx;
    return zhash_lookup (ctx->commits, name);
}

static void commit_done (plugin_ctx_t *p, commit_t *cp)
{
    ctx_t *ctx = p->ctx;

    memcpy (cp->rootdir, ctx->rootdir, sizeof (href_t));
    cp->rootseq = ctx->rootseq;
    cp->done = true;
}

static op_t *op_create (optype_t type, char *key, char *ref, zmsg_t *zmsg)
{
    op_t *op = xzmalloc (sizeof (*op));

    op->type = type;
    op->key = key;
    op->ref = ref;
    op->zmsg = zmsg;

    return op;
}

static void op_destroy (op_t *op)
{
    if (op->key)
        free (op->key);
    if (op->ref)
        free (op->ref);
    if (op->zmsg)
        zmsg_destroy (&op->zmsg);
    free (op);
}

static bool op_match (op_t *op1, op_t *op2)
{
    bool match = false;

    if (op1->type == op2->type) {
        switch (op1->type) {
            case OP_STORE:
                if (!strcmp (op1->ref, op2->ref))
                    match = true;
                break;
            case OP_NAME:
                if (!strcmp (op1->key, op2->key))
                    match = true;
                break;
            case OP_FLUSH:
                break;
        }
    }
    return match;
}

static void commit_add_name (plugin_ctx_t *p, const char *key, const char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t *op;

    assert (plugin_treeroot (p));

    op = op_create (OP_NAME, xstrdup (key), ref ? xstrdup (ref) : NULL, NULL);
    if (zlist_append (ctx->commit_ops, op) < 0)
        oom ();
}

static void writeback_add_name (plugin_ctx_t *p,
                                const char *key, const char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t *op;

    assert (!plugin_treeroot (p));

    op = op_create (OP_NAME, xstrdup (key), ref ? xstrdup (ref) : NULL, NULL);
    if (zlist_append (ctx->writeback, op) < 0)
        oom ();
    ctx->writeback_state = WB_DIRTY;
}

static void writeback_add_store (plugin_ctx_t *p, const char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t *op;

    assert (!plugin_treeroot (p));

    op = op_create (OP_STORE, NULL, xstrdup (ref), NULL);
    if (zlist_append (ctx->writeback, op) < 0)
        oom ();
    ctx->writeback_state = WB_DIRTY;
}

static void writeback_add_flush (plugin_ctx_t *p, zmsg_t *zmsg)
{
    ctx_t *ctx = p->ctx;
    op_t *op;

    assert (!plugin_treeroot (p));

    op = op_create (OP_FLUSH, NULL, NULL, zmsg);
    if (zlist_append (ctx->writeback, op) < 0)
        oom ();
}

static void writeback_del (plugin_ctx_t *p, optype_t type,
                           char *key, char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t mop = { .type = type, .key = key, .ref = ref, .zmsg = NULL };
    op_t *op = zlist_first (ctx->writeback);

    while (op) {
        if (op_match (op, &mop))
            break;
        op = zlist_next (ctx->writeback);
    }
    if (op) {
        zlist_remove (ctx->writeback, op);
        op_destroy (op);
        /* handle flush(es) now at head of queue */
        while ((op = zlist_head (ctx->writeback)) && op->type == OP_FLUSH) {
            ctx->writeback_state = WB_FLUSHING;
            plugin_send_request_raw (p, &op->zmsg); /* fwd upstream */
            zlist_remove (ctx->writeback, op);
            op_destroy (op);
        }
    }
}

static hobj_t *hobj_create (json_object *o)
{
    hobj_t *hp = xzmalloc (sizeof (*hp));

    hp->o = o;
    if (!(hp->reqs = zlist_new ()))
        oom ();

    return hp;
}

static void hobj_destroy (hobj_t *hp)
{
    if (hp->o)
        json_object_put (hp->o);
    assert (zlist_size (hp->reqs) == 0);
    zlist_destroy (&hp->reqs);
    free (hp);
}

static void load_request_send (plugin_ctx_t *p, const href_t ref)
{
    json_object *o = util_json_object_new_object ();

    json_object_object_add (o, ref, NULL);
    plugin_send_request (p, o, "kvs.load");
    json_object_put (o);
}

static bool load (plugin_ctx_t *p, const href_t ref, zmsg_t **zmsg,
                  json_object **op)
{
    ctx_t *ctx = p->ctx;
    hobj_t *hp = zhash_lookup (ctx->store, ref);
    bool done = true;

    if (plugin_treeroot (p)) {
        if (!hp)
            msg_exit ("dangling ref %s", ref);
    } else {
        if (!hp) {
            hp = hobj_create (NULL);
            zhash_insert (ctx->store, ref, hp);
            zhash_freefn (ctx->store, ref, (zhash_free_fn *)hobj_destroy);
            load_request_send (p, ref);
            /* leave hp->o == NULL to be filled in on response */
        }
        if (!hp->o) {
            assert (zmsg != NULL);
            if (zlist_append (hp->reqs, *zmsg) < 0)
                oom ();
            *zmsg = NULL;
            done = false; /* stall */
        }
    }
    if (done) {
        assert (hp != NULL);
        assert (hp->o != NULL);
        *op = hp->o;
    }
    return done;
}

static void store_request_send (plugin_ctx_t *p, const href_t ref,
                                json_object *val)
{
    json_object *o = util_json_object_new_object ();

    json_object_get (val);
    json_object_object_add (o, ref, val);
    plugin_send_request (p, o, "kvs.store");
    json_object_put (o);
}

static void store (plugin_ctx_t *p, json_object *o, bool writeback, href_t ref)
{
    ctx_t *ctx = p->ctx;
    hobj_t *hp; 
    zmsg_t *zmsg;

    compute_json_href (o, ref);

    if ((hp = zhash_lookup (ctx->store, ref))) {
        if (hp->o) {
            json_object_put (o);
        } else {
            hp->o = o;
            /* restart any stalled requests */
            while ((zmsg = zlist_pop (hp->reqs))) {
                if (cmb_msg_match (zmsg, "kvs.load"))
                    kvs_load (p, &zmsg);
                else if (cmb_msg_match (zmsg, "kvs.get"))
                    kvs_get (p, &zmsg);
                if (zmsg)
                    zmsg_destroy (&zmsg);
            }
        }
    } else {
        hp = hobj_create (o);
        zhash_insert (ctx->store, ref, hp);
        zhash_freefn (ctx->store, ref, (zhash_free_fn *)hobj_destroy);
        if (writeback) {
            assert (!plugin_treeroot (p));
            writeback_add_store (p, ref);
            store_request_send (p, ref, o);
        }
    }
}

static void name_request_send (plugin_ctx_t *p, char *key, const href_t ref)
{
    json_object *o = util_json_object_new_object ();

    util_json_object_add_string (o, key, ref);
    plugin_send_request (p, o, "kvs.name");
    json_object_put (o);
}

static void name (plugin_ctx_t *p, char *key, const href_t ref, bool writeback)
{
    if (writeback) {
        writeback_add_name (p, key, ref);
        name_request_send (p, key, ref);
    } else {
        commit_add_name (p, key, ref);
    }
}

static bool decode_rootref (const char *rootref, int *seqp, href_t ref)
{
    char *p;
    int seq = strtoul (rootref, &p, 10);

    if (*p++ != '.' || strlen (p) + 1 != sizeof (href_t))
        return false;
    *seqp = seq;
    memcpy (ref, p, sizeof (href_t));
    return true;
}

static char *encode_rootref (int seq, href_t ref)
{
    char *rootref;

    if (asprintf (&rootref, "%d.%s", seq, ref) < 0)
        oom ();
    return rootref;
}

static void setroot (plugin_ctx_t *p, int seq, href_t ref)
{
    ctx_t *ctx = p->ctx;

    if (seq == 0 || seq > ctx->rootseq) {
        memcpy (ctx->rootdir, ref, sizeof (href_t));
        ctx->rootseq = seq;
    }
}

static void create_snapshot_dirent (plugin_ctx_t *p, json_object *dir)
{
    ctx_t *ctx = p->ctx;
    char *snapshot_name;

    if (asprintf (&snapshot_name, "snapshot.%04d", ctx->rootseq) < 0)
        oom ();
    util_json_object_add_string (dir, snapshot_name, ctx->rootdir);
    free (snapshot_name);
}

static void commit (plugin_ctx_t *p)
{
    ctx_t *ctx = p->ctx;
    op_t *op;
    json_object *dir, *cpy;

    assert (plugin_treeroot (p));

    (void)load (p, ctx->rootdir, NULL, &dir);
    assert (dir != NULL);
    cpy = util_json_object_dup (dir);
    while ((op = zlist_pop (ctx->commit_ops))) {
        assert (op->type == OP_NAME);
        if (op->ref)
            util_json_object_add_string (cpy, op->key, op->ref);
        else
            json_object_object_del (cpy, op->key);
        op_destroy (op);
    }
    create_snapshot_dirent (p, cpy);
    store (p, cpy, false, ctx->rootdir);
    ctx->rootseq++;
}

static void kvs_load (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *val, *cpy = NULL, *o = NULL;
    json_object_iter iter;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        if (!load (p, iter.key, zmsg, &val))
            goto done; /* stall */
        json_object_get (val);
        json_object_object_add (cpy, iter.key, val);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_load_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    href_t href;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        json_object_get (iter.val);
        store (p, iter.val, false, href);
        if (strcmp (href, iter.key) != 0)
            plugin_log (p, LOG_ERR, "%s: bad href %s", __FUNCTION__, iter.key);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_store (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *cpy = NULL, *o = NULL;
    json_object_iter iter;
    bool writeback = !plugin_treeroot (p);
    href_t href;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        json_object_get (iter.val);
        store (p, iter.val, writeback, href);
        if (strcmp (href, iter.key) != 0)
            plugin_log (p, LOG_ERR, "%s: bad href %s", __FUNCTION__, iter.key);
        json_object_object_add (cpy, iter.key, NULL);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_store_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        writeback_del (p, OP_STORE, NULL, iter.key);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_dropcache (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    int rc = 0;

    if (!plugin_treeroot (p)) {
        if (zlist_size (ctx->writeback) > 0)
            rc = EAGAIN;
        else {
            plugin_log (p, LOG_ALERT, "dropped %d cache entries",
                                      zhash_size (ctx->store));
            zhash_destroy (&ctx->store);
            if (!(ctx->store = zhash_new ()))
                oom ();
        }
    }
    plugin_send_response_errnum (p, zmsg, rc);
}

static void kvs_name (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *cpy = NULL, *o = NULL;
    json_object_iter iter;
    bool writeback = !plugin_treeroot (p);

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        name (p, iter.key, json_object_get_string (iter.val), writeback);
        json_object_object_add (cpy, iter.key, NULL);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_name_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        writeback_del (p, OP_NAME, iter.key, NULL);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_flush (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;

    if (plugin_treeroot (p) || ctx->writeback_state == WB_CLEAN)
        plugin_send_response_errnum (p, zmsg, 0);
    else if (zlist_size (ctx->writeback) == 0) {
        plugin_send_request_raw (p, zmsg); /* fwd upstream */
        ctx->writeback_state = WB_FLUSHING;
    } else {
        writeback_add_flush (p, *zmsg); /* enqueue */
        *zmsg = NULL;
    }
}

static void kvs_flush_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;

    plugin_send_response_raw (p, zmsg); /* fwd downstream */
    if (ctx->writeback_state == WB_FLUSHING)
        ctx->writeback_state = WB_CLEAN;
}

static void kvs_get (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    json_object *dir, *val, *cpy = NULL, *o = NULL;
    json_object_iter iter;
    const char *ref;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    if (!load (p, ctx->rootdir, zmsg, &dir))
        goto done; /* stall */
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        if (util_json_object_get_string (dir, iter.key, &ref) < 0)
            continue;
        if (!load (p, ref, zmsg, &val))
            goto done; /* stall */
        json_object_get (val);
        json_object_object_add (cpy, iter.key, val); 
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_put (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    href_t ref;
    bool writeback = !plugin_treeroot (p);

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        if (json_object_get_type (iter.val) == json_type_null) {
            name (p, iter.key, NULL, writeback);
        } else { 
            json_object_get (iter.val);
            store (p, iter.val, writeback, ref);
            name (p, iter.key, ref, writeback);
        }
    }
    plugin_send_response_errnum (p, zmsg, 0); /* success */
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void commit_request_send (plugin_ctx_t *p, const char *name)
{
    json_object *o = util_json_object_new_object ();

    util_json_object_add_string (o, "name", name);
    plugin_send_request (p, o, "kvs.commit");
    json_object_put (o);
}

static void commit_response_send (plugin_ctx_t *p, commit_t *cp, 
                                  json_object *o, zmsg_t **zmsg)
{
    char *rootref = encode_rootref (cp->rootseq, cp->rootdir);

    util_json_object_add_string (o, "rootref", rootref);
    plugin_send_response (p, zmsg, o);
    free (rootref);
}

static void kvs_commit (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    const char *name;
    commit_t *cp;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL
            || util_json_object_get_string (o, "name", &name) < 0) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    if (plugin_treeroot (p)) {
        if (!(cp = commit_find (p, name))) {
            commit (p);
            cp = commit_new (p, name);
            commit_done (p, cp);
            event_kvs_setroot_send (p);
        }
        commit_response_send (p, cp, o, zmsg);
    } else {
        if (!(cp = commit_find (p, name))) {
            commit_request_send (p, name);
            cp = commit_new (p, name);
        }
        if (!cp->done) {
            zlist_append (cp->reqs, *zmsg);
            *zmsg = NULL;
        } else
            commit_response_send (p, cp, o, zmsg);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_commit_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    const char *arg, *name;
    int seq;
    href_t href;
    zmsg_t *zmsg2;
    commit_t *cp;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL
            || util_json_object_get_string (o, "name", &name) < 0
            || util_json_object_get_string (o, "rootref", &arg) < 0
            || !decode_rootref (arg, &seq, href)) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    setroot (p, seq, href); /* may be redundant - racing with multicast */
    cp = commit_find (p, name);
    assert (cp != NULL);
    commit_done (p, cp);
    while ((zmsg2 = zlist_pop (cp->reqs))) {
        json_object *o2 = NULL;
        if (cmb_msg_decode (zmsg2, NULL, &o2) == 0 && o2 != NULL)
            commit_response_send (p, cp, o2, &zmsg2);
        if (zmsg2)
            zmsg_destroy (&zmsg2);
        if (o2)
            json_object_put (o2);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_getroot (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    json_object *o;
    char *rootref = encode_rootref (ctx->rootseq, ctx->rootdir);

    if (!(o = json_object_new_string (rootref)))
        oom ();
    plugin_send_response (p, zmsg, o);
    free (rootref);
    json_object_put (o);
}

static void event_kvs_setroot (plugin_ctx_t *p, char *arg, zmsg_t **zmsg)
{
    href_t href;
    int seq;

    assert (!plugin_treeroot (p));

    if (!decode_rootref (arg, &seq, href))
        plugin_log (p, LOG_ERR, "%s: malformed rootref %s", __FUNCTION__, arg);
    else
        setroot (p, seq, href);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void event_kvs_setroot_send (plugin_ctx_t *p)
{
    ctx_t *ctx = p->ctx;
    char *rootref = encode_rootref (ctx->rootseq, ctx->rootdir);
    plugin_send_event (p, "event.kvs.setroot.%s", rootref);
    free (rootref);
}

static int log_store_entry (const char *key, void *item, void *arg)
{
    plugin_ctx_t *p = arg;
    hobj_t *hp = item; 

    plugin_log (p, LOG_DEBUG, "%s\t%s [%d reqs]", key,
                hp->o ? json_object_to_json_string (hp->o) : "<unset>",
                zlist_size (hp->reqs));
    return 0;
}

static void event_kvs_debug_store (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;

    zhash_foreach (ctx->store, log_store_entry, p);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void event_kvs_debug_stats (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    zlist_t *keys;
    commit_t *cp;
    int done = 0;

    plugin_log (p, LOG_DEBUG, "writeback size %d", zlist_size (ctx->writeback));
    plugin_log (p, LOG_DEBUG, "store size %d", zhash_size (ctx->store));
    plugin_log (p, LOG_DEBUG, "root ref %s (seq=%d)",
                ctx->rootdir, ctx->rootseq);

    keys = zhash_keys (ctx->commits);
    cp = zlist_first (keys);
    while (cp) {
        if (cp->done)
            done++;
        cp = zlist_next (keys);
    }
    zlist_destroy (&keys);
    plugin_log (p, LOG_DEBUG, "commits %d/%d complete",
                done, zhash_size (ctx->commits));
    
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_recv (plugin_ctx_t *p, zmsg_t **zmsg, zmsg_type_t type)
{
    char *arg = NULL;

    if (cmb_msg_match (*zmsg, "kvs.getroot")) {
        kvs_getroot (p, zmsg);
    } else if (cmb_msg_match_substr (*zmsg, "event.kvs.setroot.", &arg)) {
        event_kvs_setroot (p, arg, zmsg);
    } else if (cmb_msg_match (*zmsg, "event.kvs.debug.store")) {
        event_kvs_debug_store (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "event.kvs.debug.stats")) {
        event_kvs_debug_stats (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.dropcache")) {
        kvs_dropcache (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.get")) {
        kvs_get (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.put")) {
        kvs_put (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.load")) {
        if (type == ZMSG_REQUEST)
            kvs_load (p, zmsg);
        else
            kvs_load_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.store")) {
        if (type == ZMSG_REQUEST)
            kvs_store (p, zmsg);
        else
            kvs_store_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.name")) {
        if (type == ZMSG_REQUEST)
            kvs_name (p, zmsg);
        else
            kvs_name_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.flush")) {
        if (type == ZMSG_REQUEST)
            kvs_flush (p, zmsg);
        else
            kvs_flush_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.commit")) {
        if (type == ZMSG_REQUEST)
            kvs_commit (p, zmsg);
        else
            kvs_commit_response (p, zmsg);
    }
    if (arg)
        free (arg);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_init (plugin_ctx_t *p)
{
    ctx_t *ctx;

    ctx = p->ctx = xzmalloc (sizeof (ctx_t));
    if (!plugin_treeroot (p))
        zsocket_set_subscribe (p->zs_evin, "event.kvs.setroot.");
    zsocket_set_subscribe (p->zs_evin, "event.kvs.debug.");
    if (!(ctx->store = zhash_new ()))
        oom ();
    if (!(ctx->writeback = zlist_new ()))
        oom ();
    ctx->writeback_state = WB_CLEAN;
    if (!(ctx->commit_ops = zlist_new ()))
        oom ();
    if (!(ctx->commits = zhash_new ()))
        oom ();

    if (plugin_treeroot (p)) {
        json_object *rootdir = util_json_object_new_object ();
        href_t href;

        store (p, rootdir, false, href);
        setroot (p, 0, href);
    } else {
        json_object *rep = plugin_request (p, NULL, "kvs.getroot");
        const char *rootref = json_object_get_string (rep);
        int seq;
        href_t href;

        if (!decode_rootref (rootref, &seq, href))
            msg_exit ("malformed kvs.getroot reply: %s", rootref);
        setroot (p, seq, href);
    }
}

static void kvs_fini (plugin_ctx_t *p)
{
    ctx_t *ctx = p->ctx;

    zhash_destroy (&ctx->store);
    zlist_destroy (&ctx->writeback);
    zlist_destroy (&ctx->commit_ops);
    zhash_destroy (&ctx->commits);
    free (ctx);
}

struct plugin_struct kvssrv = {
    .name      = "kvs",
    .initFn    = kvs_init,
    .finiFn    = kvs_fini,
    .recvFn    = kvs_recv,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
