
#ifndef KVNS_H
#define KVNS_H

#include "plugin.h"


/* kvns - convenience handle for kvs 'namespace' -- i.e. operate
 *  relative to subdirectory in the kvs hierarchy
 */

typedef struct kvns * kvns_t;

kvns_t kvns_create (plugin_ctx_t *p, const char *fmt, ...);
kvns_t kvns_create_lwj_id (plugin_ctx_t *p, int64_t id);

void   kvns_destroy (kvns_t k);

char * kvns_key_create (kvns_t k, const char *rkey);

int    kvns_put (kvns_t k, const char *rkey, json_object *val);
int    kvns_get (kvns_t k, const char *rkey, json_object **valp);

#endif
