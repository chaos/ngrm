
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>

#include <json/json.h>
#include <zmq.h>
#include <czmq.h>

#include "route.h"
#include "cmbd.h"
#include "util.h"
#include "plugin.h"

/*
 *  Handle for kvs namespace
 */
struct kvns {
    plugin_ctx_t *p;
    char *dir;
};

static struct kvns *kvns_alloc (plugin_ctx_t *p, const char *dir)
{
    struct kvns *ns = xzmalloc (sizeof (*ns));
    ns->p = p;
    ns->dir = xstrdup (dir);
    return (ns);
}

struct kvns * kvns_create (plugin_ctx_t *p, const char *fmt, ...)
{
    va_list ap;
    struct kvns *ns;
    char *dir;
    int n;

    va_start (ap, fmt);
    n = vasprintf (&dir, fmt, ap);
    va_end (ap);
    if (n < 0)
        return (NULL);
    ns = kvns_alloc (p, dir);
    free (dir);
    return (ns);
}

struct kvns *kvns_create_lwj_id (plugin_ctx_t *p, unsigned long id)
{
    return (kvns_create (p, "lwj.%lu", id));
}

void kvns_destroy (struct kvns *ns)
{
    ns->p = NULL;
    free (ns->dir);
    free (ns);
}

char * kvns_key_create (struct kvns *ns, const char *rkey)
{
    char *key = xzmalloc (strlen (ns->dir) + 1 + strlen (rkey) + 1);
    sprintf (key, "%s.%s", ns->dir, rkey);
    return (key);
}

int kvns_put (struct kvns *ns, const char *rkey, json_object *val)
{
    int rc;
    char *key = kvns_key_create (ns, rkey);

    rc = plugin_kvs_put (ns->p, key, val);
    free (key);
    return (rc);
}

int kvns_get (struct kvns *ns, const char *rkey, json_object **valp)
{
    char *key = kvns_key_create (ns, rkey);
    int rc = plugin_kvs_get (ns->p, key, valp);
    free (key);
    return (rc);
}

