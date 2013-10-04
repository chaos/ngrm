/* cmbutil.c - exercise public interfaces */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <stdbool.h>
#include <json/json.h>
#include <sys/param.h>

#include "cmb.h"
#include "log.h"
#include "util.h"

static int _parse_logstr (char *s, int *lp, char **fp);
static void list_kvs (const char *name, json_object *o);

#define OPTIONS "p:s:b:B:k:SK:Ct:P:d:n:x:e:TL:W:D:r:R:qz:Zyl:j:Y:"
static const struct option longopts[] = {
    {"ping",       required_argument,  0, 'p'},
    {"stats",      required_argument,  0, 'x'},
    {"ping-padding", required_argument,0, 'P'},
    {"ping-delay", required_argument,  0, 'd'},
    {"subscribe",  required_argument,  0, 's'},
    {"event",      required_argument,  0, 'e'},
    {"barrier",    required_argument,  0, 'b'},
    {"barrier-torture", required_argument,  0, 'B'},
    {"nprocs",     required_argument,  0, 'n'},
    {"kvs-put",    required_argument,  0, 'k'},
    {"kvs-get",    required_argument,  0, 'K'},
    {"kvs-get-fromcache", required_argument,  0, 'j'},
    {"kvs-list",   required_argument,  0, 'l'},
    {"kvs-watch",  required_argument,  0, 'Y'},
    {"kvs-commit", no_argument,        0, 'C'},
    {"kvs-dropcache", no_argument,     0, 'y'},
    {"kvs-torture",required_argument,  0, 't'},
    {"sync",       no_argument,        0, 'S'},
    {"snoop",      no_argument,        0, 'T'},
    {"log",        required_argument,  0, 'L'},
    {"log-watch",  required_argument,  0, 'W'},
    {"log-dump",   required_argument,  0, 'D'},
    {"route-add",  required_argument,  0, 'r'},
    {"route-del",  required_argument,  0, 'R'},
    {"route-query",no_argument,        0, 'q'},
    {"socket-path",required_argument,  0, 'z'},
    {"trace-apisock",no_argument,      0, 'Z'},
    {0, 0, 0, 0},
};

static void usage (void)
{
    fprintf (stderr, "Usage: cmbutil OPTIONS\n"
"  -p,--ping name         route a message through a plugin\n"
"  -P,--ping-padding N    pad ping packets with N bytes (adds a JSON string)\n"
"  -d,--ping-delay N      set delay between ping packets (in msec)\n"
"  -x,--stats name        get plugin statistics\n"
"  -T,--snoop             display messages to/from router socket\n"
"  -b,--barrier name      execute barrier across slurm job\n"
"  -B,--barrier-torture N execute N barriers across slurm job\n"
"  -n,--nprocs N          override nprocs (default $SLURM_NPROCS or 1)\n"
"  -k,--kvs-put key=val   set a key\n"
"  -K,--kvs-get key       get a key\n"
"  -Y,--kvs-watch key     watch a key\n"
"  -j,--kvs-get-fromcache key get a key (using cache method)\n"
"  -l,--kvs-list name     list keys in a particular \"directory\"\n"
"  -C,--kvs-commit        commit pending kvs puts\n"
"  -y,--kvs-clean         drop cached and unreferenced kvs data\n"
"  -t,--kvs-torture N     set N keys, then commit\n"
"  -s,--subscribe sub     subscribe to events matching substring\n"
"  -e,--event name        publish event\n"
"  -S,--sync              block until event.sched.triger\n"
"  -L,--log fac:lev MSG   log MSG to facility at specified level\n"
"  -W,--log-watch fac:lev watch logs for messages matching tag\n"
"  -D,--log-dump fac:lev  dump circular log buffer\n"
"  -r,--route-add dst:gw  add local route to dst via gw\n"
"  -R,--route-del dst     delete local route to dst\n"
"  -q,--route-query       list routes in JSON format\n"
"  -z,--socket-path PATH  use non-default API socket path\n"
"  -Z,--trace-apisock     trace api socket messages\n"
);
    exit (1);
}

int main (int argc, char *argv[])
{
    int ch;
    cmb_t c;
    int nprocs;
    int padding = 0;
    int pingdelay_ms = 1000;
    static char socket_path[PATH_MAX + 1];
    char *val;
    bool Lopt = false;
    char *Lopt_facility;
    int Lopt_level;
    int flags = 0;

    log_init (basename (argv[0]));

    nprocs = env_getint ("SLURM_NPROCS", 1);

    if ((val = getenv ("CMB_API_PATH"))) {
        if (strlen (val) > PATH_MAX)
            err_exit ("What a long CMB_API_PATH you have!");
        strcpy (socket_path, val);
    }
    else {
        snprintf (socket_path, sizeof (socket_path),
                  CMB_API_PATH_TMPL, getuid ());
    }

    while ((ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'P': /* --ping-padding N */
                padding = strtoul (optarg, NULL, 10);
                break;
            case 'd': /* --ping-delay N */
                pingdelay_ms = strtoul (optarg, NULL, 10);
                break;
            case 'n': /* --nprocs N */
                nprocs = strtoul (optarg, NULL, 10);
                break;
            case 'z': /* --socket-path PATH */
                snprintf (socket_path, sizeof (socket_path), "%s", optarg);
                break;
            case 'Z': /* --trace-apisock */
                flags |= CMB_FLAGS_TRACE;
                break;
        }
    }
    if (!(c = cmb_init_full (socket_path, flags)))
        err_exit ("cmb_init");
    optind = 0;
    while ((ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'P':
            case 'd':
            case 'n':
            case 'z':
            case 'Z':
                break; /* handled in first getopt */
            case 'p': { /* --ping name */
                int i;
                struct timeval t, t1, t2;
                char *tag, *route;
                for (i = 0; ; i++) {
                    xgettimeofday (&t1, NULL);
                    if (cmb_ping (c, optarg, i, padding, &tag, &route) < 0)
                        err_exit ("cmb_ping");
                    xgettimeofday (&t2, NULL);
                    timersub (&t2, &t1, &t);
                    msg ("%s pad=%d seq=%d time=%0.3f ms (%s)", tag,
                         padding,i,
                         (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000,
                         route);
                    usleep (pingdelay_ms * 1000);
                    free (tag);
                    free (route);
                }
                break;
            }
            case 'x': { /* --stats name */
                json_object *o;
                char *s;

                if (!(s = cmb_stats (c, optarg)))
                    err_exit ("cmb_stats");
                if (!(o = json_tokener_parse (s)))
                    err_exit ("json_tokener_parse");
                printf ("%s\n", json_object_to_json_string_ext (o,
                                    JSON_C_TO_STRING_PRETTY));
                json_object_put (o);
                free (s);
                break;
            }
            case 'b': { /* --barrier NAME */
                struct timeval t, t1, t2;
                xgettimeofday (&t1, NULL);
                if (cmb_barrier (c, optarg, nprocs) < 0)
                    err_exit ("cmb_barrier");
                xgettimeofday (&t2, NULL);
                timersub (&t2, &t1, &t);
                msg ("barrier time=%0.3f ms",
                     (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000);
                break;
            }
            case 'B': { /* --barrier-torture N */
                int i, n = strtoul (optarg, NULL, 10);
                char name[16];
                for (i = 0; i < n; i++) {
                    snprintf (name, sizeof (name), "%d", i);
                    if (cmb_barrier (c, name, nprocs) < 0)
                        err_exit ("cmb_barrier %s", name);
                }
                break;
            }
            case 's': { /* --subscribe substr */
                char *event;
                if (cmb_event_subscribe (c, optarg) < 0)
                    err_exit ("cmb_event_subscribe");
                while ((event = cmb_event_recv (c))) {
                    msg ("%s", event);
                    free (event);
                }
                break;
            }
            case 'T': { /* --snoop */
                if (cmb_snoop (c, true) < 0)
                    err_exit ("cmb_snoop");
                while (cmb_snoop_one (c) == 0)
                    ;
                /* NOTREACHED */
                break;
            }
            case 'S': { /* --sync */
                char *event;
                if (cmb_event_subscribe (c, "event.sched.trigger.") < 0)
                    err_exit ("cmb_event_subscribe");
                if (!(event = cmb_event_recv (c)))
                    err_exit ("cmb_event_recv");
                free (event);
                break;
            }
            case 'e': { /* --event name */
                if (cmb_event_send (c, optarg) < 0)
                    err_exit ("cmb_event_send");
                break;
            }
            case 'k': { /* --kvs-put key=val */
                char *key = optarg;
                char *val = strchr (optarg, '=');
                json_object *vo = NULL;

                if (!val)
                    msg_exit ("malformed key=[val] argument");
                *val++ = '\0';
                if (strlen (val) > 0)
                    if (!(vo = json_tokener_parse (val)))
                        vo = json_object_new_string (val);
                if (cmb_kvs_put (c, key, vo) < 0)
                    err_exit ("cmb_kvs_put");
                if (vo)
                    json_object_put (vo);
                break;

            }
            case 'K': { /* --kvs-get key */
                json_object *o;

                if (cmb_kvs_get (c, optarg, &o, KVS_GET_VAL) < 0)
                    err_exit ("cmb_kvs_get");
                if (json_object_get_type (o) == json_type_string)
                    printf ("%s = \"%s\"\n", optarg,
                            json_object_get_string (o));
                else
                    printf ("%s = %s\n", optarg,
                            json_object_to_json_string_ext (o,
                                                    JSON_C_TO_STRING_PLAIN));
                json_object_put (o);
                break;
            }
            case 'Y': { /* --kvs-watch key */
                json_object *o;
                if (cmb_kvs_get (c, optarg, &o, KVS_GET_WATCH) < 0)
                    err_exit ("cmb_kvs_get");
                do {
                    if (json_object_get_type (o) == json_type_string)
                        printf ("%s = \"%s\"\n", optarg,
                                json_object_get_string (o));
                    else
                        printf ("%s = %s\n", optarg,
                                json_object_to_json_string_ext (o,
                                                   JSON_C_TO_STRING_PLAIN));
                    json_object_put (o);
                } while (cmb_kvs_get (c, optarg, &o, KVS_GET_NEXT) == 0);
                break;
            }
            case 'j': { /* --kvs-get-cached key */
                json_object *dir = NULL, *o = NULL;

                if (cmb_kvs_get (c, ".", &dir, KVS_GET_DIR) < 0)
                    err_exit ("cmb_kvs_get");
                if (cmb_kvs_get_cache (dir, optarg, &o) < 0)
                    err_exit ("cmb_kvs_get_cache: %s", optarg);
                if (json_object_get_type (o) == json_type_string)
                    printf ("%s = \"%s\"\n", optarg,
                            json_object_get_string (o));
                else
                    printf ("%s = %s\n", optarg,
                            json_object_to_json_string_ext (o,
                                                    JSON_C_TO_STRING_PLAIN));
                json_object_put (dir);
                json_object_put (o);
                break;
            }
            case 'l': { /* --kvs-list name */
                json_object *o;

                if (cmb_kvs_get (c, optarg, &o, KVS_GET_DIR) < 0)
                    err_exit ("cmb_conf_get");
                list_kvs (optarg, o);
                json_object_put (o);
                break;
            }
            case 'C': { /* --kvs-commit */
                if (cmb_kvs_flush (c) < 0)
                    err_exit ("cmb_kvs_flush");
                if (cmb_kvs_commit (c, NULL) < 0)
                    err_exit ("cmb_kvs_commit");
                break;
            }
            case 'y': { /* --kvs-clean */
                if (cmb_kvs_clean (c) < 0)
                    err_exit ("cmb_kvs_clean");
                break;
            }
            case 't': { /* --kvs-torture N */
                int i, n = strtoul (optarg, NULL, 10);
                char key[16], val[16];
                struct timeval t1, t2, t;
                json_object *vo = NULL;
                char *uuid = uuid_generate_str ();

                xgettimeofday (&t1, NULL);
                for (i = 0; i < n; i++) {
                    snprintf (key, sizeof (key), "key%d", i);
                    snprintf (val, sizeof (key), "val%d", i);
                    vo = json_object_new_string (val);
                    if (cmb_kvs_put (c, key, vo) < 0)
                        err_exit ("cmb_kvs_put");
                    if (vo)
                        json_object_put (vo);
                }
                xgettimeofday (&t2, NULL);
                timersub(&t2, &t1, &t);
                msg ("kvs_put:    time=%0.3f ms",
                     (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000);

                xgettimeofday (&t1, NULL);
                if (cmb_kvs_flush (c) < 0)
                    err_exit ("cmb_kvs_flush");
                xgettimeofday (&t2, NULL);
                timersub(&t2, &t1, &t);
                msg ("kvs_flush:  time=%0.3f ms",
                     (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000);

                xgettimeofday (&t1, NULL);
                if (cmb_kvs_commit (c, uuid) < 0)
                    err_exit ("cmb_kvs_commit");
                xgettimeofday (&t2, NULL);
                timersub (&t2, &t1, &t);
                msg ("kvs_commit: time=%0.3f ms",
                     (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000);

                xgettimeofday (&t1, NULL);
                for (i = 0; i < n; i++) {
                    snprintf (key, sizeof (key), "key%d", i);
                    snprintf (val, sizeof (key), "val%d", i);
                    if (cmb_kvs_get (c, key, &vo, 0) < 0)
                        err_exit ("cmb_kvs_get");
                    if (strcmp (json_object_get_string (vo), val) != 0)
                        msg_exit ("cmb_kvs_get: key '%s' wrong value '%s'",
                                  key, json_object_get_string (vo));
                    if (vo)
                        json_object_put (vo);
                }
                xgettimeofday (&t2, NULL);
                timersub(&t2, &t1, &t);
                msg ("kvs_get:    time=%0.3f ms",
                     (double)t.tv_sec * 1000 + (double)t.tv_usec / 1000);

                free (uuid);
                break;
            }
            case 'L': { /* --log */
                if (_parse_logstr (optarg, &Lopt_level, &Lopt_facility) < 0)
                    msg_exit ("bad log level string");
                Lopt = true; /* see code after getopt */
                break;
            }
            case 'W': {
                char *src, *fac, *s;
                struct timeval tv, start = { .tv_sec = 0 }, rel;
                int count, lev;
                const char *levstr;

                if (_parse_logstr (optarg, &lev, &fac) < 0)
                    msg_exit ("bad log level string");
                if (cmb_log_subscribe (c, lev, fac) < 0)
                    err_exit ("cmb_log_subscribe");
                free (fac);
                while ((s = cmb_log_recv (c, &lev, &fac, &count, &tv, &src))) {
                    if (start.tv_sec == 0)
                        start = tv;
                    timersub (&tv, &start, &rel);
                    levstr = log_leveltostr (lev);
                    //printf ("XXX lev=%d (%s)\n", lev, levstr);
                    fprintf (stderr, "[%-.6lu.%-.6lu] %dx %s.%s[%s]: %s\n",
                             rel.tv_sec, rel.tv_usec, count,
                             fac, levstr ? levstr : "unknown", src, s);
                    free (fac);
                    free (src);
                    free (s);
                }
                if (errno != ENOENT)
                    err ("cmbv_log_recv");
                break;
            }
            case 'D': {
                char *src, *fac, *s;
                struct timeval tv, start = { .tv_sec = 0 }, rel;
                int lev, count;
                const char *levstr;

                if (_parse_logstr (optarg, &lev, &fac) < 0)
                    msg_exit ("bad log level string");
                if (cmb_log_dump (c, lev, fac) < 0)
                    err_exit ("cmb_log_dump");
                free (fac);
                while ((s = cmb_log_recv (c, &lev, &fac, &count, &tv, &src))) {
                    if (start.tv_sec == 0)
                        start = tv;
                    timersub (&tv, &start, &rel);
                    levstr = log_leveltostr (lev);
                    fprintf (stderr, "[%-.6lu.%-.6lu] %dx %s.%s[%s]: %s\n",
                             rel.tv_sec, rel.tv_usec, count,
                             fac, levstr ? levstr : "unknown", src, s);
                    free (fac);
                    free (src);
                    free (s);
                }
                if (errno != ENOENT)
                    err ("cmbv_log_recv");
                break;
            }
            case 'r': { /* --route-add dst:gw */
                char *gw, *dst = xstrdup (optarg);
                if (!(gw = strchr (dst, ':')))
                    usage ();
                *gw++ = '\0';
                if (cmb_route_add (c, dst, gw) < 0)
                    err ("cmb_route_add %s via %s", dst, gw);
                break;
            }
            case 'R': { /* --route-del dst */
                char *gw, *dst = xstrdup (optarg);
                if (!(gw = strchr (dst, ':')))
                    usage ();
                *gw++ = '\0';
                if (cmb_route_del (c, dst, gw) < 0)
                    err ("cmb_route_del %s via %s", dst, gw);
                break;
            }
            case 'q': { /* --route-query */
                json_object *o;
                char *s;

                if (!(s = cmb_route_query (c)))
                    err_exit ("cmb_route_query");
                if (!(o = json_tokener_parse (s)))
                    err_exit ("json_tokener_parse");
                printf ("%s\n", json_object_to_json_string_ext (o,
                                    JSON_C_TO_STRING_PRETTY));
                json_object_put (o);
                free (s);
                msg ("rank=%d size=%d", cmb_rank (c), cmb_size (c));
                break;
            }
            default:
                usage ();
        }
    }

    if (Lopt) {
        char *argstr = argv_concat (argc - optind, argv + optind);

        cmb_log_set_facility (c, Lopt_facility);
        if (cmb_log (c, Lopt_level, "%s", argstr) < 0)
            err_exit ("cmb_log");
        free (argstr);
    } else {
        if (optind < argc)
            usage ();
    }

    cmb_fini (c);
    exit (0);
}

static int _parse_logstr (char *s, int *lp, char **fp)
{
    char *p, *fac = xstrdup (s);
    int lev = LOG_INFO;

    if ((p = strchr (fac, ':'))) {
        *p++ = '\0';
        lev = log_strtolevel (p);
        if (lev < 0)
            return -1;
    }
    *lp = lev;
    *fp = fac;
    return 0;
}

static void list_kvs (const char *name, json_object *o)
{
    json_object *co;
    json_object_iter iter;
    char *path;

    json_object_object_foreachC (o, iter) {
        if (!strcmp (name, "."))
            path = xstrdup (iter.key);
        else if (asprintf (&path, "%s.%s", name, iter.key) < 0)
            oom ();
        if ((co = json_object_object_get (iter.val, "DIRVAL"))) {
            list_kvs (path, co); 
        } else if ((co = json_object_object_get (iter.val, "FILEVAL"))) {
            if (json_object_get_type (co) == json_type_string)
                printf ("%s = \"%s\"\n", path, json_object_get_string (co));
            else
                printf ("%s = %s\n", path, json_object_to_json_string_ext (co,
                                                JSON_C_TO_STRING_PLAIN));
        }
        free (path);
    }
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
