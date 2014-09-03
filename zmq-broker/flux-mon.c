/* flux-mon.c - flux mon subcommand */

#define _GNU_SOURCE
#include <getopt.h>
#include <json/json.h>
#include <assert.h>
#include <libgen.h>

#include "cmb.h"
#include "util.h"
#include "log.h"
#include "shortjson.h"

#define OPTIONS "h"
static const struct option longopts[] = {
    {"help",       no_argument,        0, 'h'},
    { 0, 0, 0, 0 },
};
/*
static void mon_list (flux_t h, int argc, char *argv[]);
static void mon_add (flux_t h, int argc, char *argv[]);
static void mon_del (flux_t h, int argc, char *argv[]);
*/
void usage (void)
{
    fprintf (stderr, 
"Usage: flux-mon list\n"
"       flux-mon add <name> <tag>\n"
"       flux-mon del <name>\n"
);
    exit (1);
}

int main (int argc, char *argv[])
{
    flux_t h;
    int ch;
    char *cmd = NULL;

    log_init ("flux-mon");

    while ((ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'h': /* --help */
                usage ();
                break;
            default:
                usage ();
                break;
        }
    }
    if (optind == argc)
        usage ();
    cmd = argv[optind++];

    if (!(h = cmb_init ()))
        err_exit ("cmb_init");
/*
    if (!strcmp (cmd, "list"))
        mon_list (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "add"))
        mon_add (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "del"))
        mon_del (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "ins"))
        mon_ins (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "echo"))
        printf("Echo\n");
    else*/
        usage ();

    flux_handle_destroy (&h);
    log_fini ();
    return 0;
}
/*
static char *plgname (const char *path)
{
    void *dso;
    char *s = NULL;
    const char **np;

    if (!(dso = dlopen (path, RTLD_NOW | RTLD_LOCAL)))
        goto done;
    if (!(np = dlsym (dso, "plg_name")) || !*np)
        goto done;
    s = xstrdup (*np);
done:
    if (dso)
        dlclose (dso);
    return s;
}

static void mon_del (flux_t h, int argc, char *argv[])
{
    char *key;

    if (argc < 1)
        usage ();
    if (asprintf (&key, "conf.mon.source.%s", argv[0]) < 0)
        oom ();
    if (kvs_get (h, key, NULL) < 0 && errno == ENOENT)
        err_exit ("%s", key);
    if (kvs_unlink (h, key) < 0)
        err_exit ("%s", key);
    if (kvs_commit (h) < 0)
        err_exit ("kvs_commit");
    free (key);
}

static void mon_add (flux_t h, int argc, char *argv[])
{
    char *name, *key, *tag;
    JSON o = Jnew ();

    if (argc != 2)
        usage ();
    name = argv[0];
    tag = argv[1];

    Jadd_str (o, "name", name);
    Jadd_str (o, "tag", tag);

    if (asprintf (&key, "conf.mon.source.%s", name) < 0)
        oom ();
    if (kvs_put (h, key, o) < 0)
        err_exit ("kvs_put %s", key);
    if (kvs_commit (h) < 0)
        err_exit ("kvs_commit");
    free (key);
    Jput (o);
}

static void mon_list (flux_t h, int argc, char *argv[])
{
    JSON o;
    const char *name;
    kvsdir_t dir;
    kvsitr_t itr;

    if (argc != 0)
        usage ();

    if (kvs_get_dir (h, &dir, "conf.mon.source") < 0) {
        if (errno == ENOENT)
            return;
        err_exit ("conf.mon.source");
    }
    itr = kvsitr_create (dir);
    while ((name = kvsitr_next (itr))) {
        if ((kvsdir_get (dir, name, &o) == 0)) {
            printf ("%s:  %s\n", name, Jtostr (o));
            Jput (o);
        }
    }
    kvsitr_destroy (itr);
    kvsdir_destroy (dir);
}

static JSON parse_plgargs (int argc, char **argv)
{
    JSON args = Jnew ();
    int i;

    for (i = 0; i < argc; i++) {
        char *val, *cpy = xstrdup (argv[i]);
        if ((val == strchr (cpy, '=')))
            *val++ = '\0';
        if (!val)
            msg_exit ("malformed argument: %s", cpy);
        Jadd_str (args, cpy, val);
        free (cpy);
    }

    return args;
}

static char *plgfind (const char *plgpath, const char *name)
{
    char *cpy = xstrdup (plgpath);
    char *path = NULL, *dir, *saveptr, *a1 = cpy;
    char *ret = NULL;

    while (!ret && (dir = strtok_r (a1, ":", &saveptr))) {
        if (asprintf (&path, "%s/%s.so", dir, name) < 0)
            oom ();
        if (access (path, R_OK|X_OK) < 0)
            free (path);
        else
            ret = path;
        a1 = NULL;
    }
    free (cpy);
    if (!ret)
        errno = ENOENT;
    return ret;
}

// Copy mod to KVS (without commit).
static void copyplg (flux_t h, const char *name, const char *path, JSON args)
{
    JSON plg = Jnew ();
    char *key;
    int fd, len;
    uint8_t *buf;

    if (asprintf (&key, "conf.mon.plgctl.plugins.%s", name) < 0)
        oom ();
    if (kvs_get (h, key, &plg) == 0)
        errn_exit (EEXIST, "%s", key);
    Jadd_obj (plg, "args", args);
    if ((fd = open (path, O_RDONLY)) < 0)
        err_exit ("%s", path);
    if ((len = read_all (fd, &buf)) < 0)
        err_exit ("%s", path);
    (void)close (fd);
    Jadd_data (plg, "data", buf, len);
    if (kvs_put (h, key, okg) < 0)
        err_exit ("kvs_put %s", key);
    free (key);
    free (buf);
    Jput (plg);
}

static void plg_ins (flux_t h, int argc, char *argv[])
{
    char *name, *path, *tag, plugin_path;
    JSON o = Jnew ();

    if (argc != 1)
        usage ();
    path = argv[0];
    if (asprintf (&plugin_path, "%s/mon", PLUGIN_PATH) < 0)
        oom ();
    if (access (path, R_OK|X_OK) < 0) {
        if (!(trypath = plgfind (plugin_path, path)))
            errn_exit (ENOENT, "%s", path);
        path = trypath;
    }
    if (!(name = plgname (path)))
        msg_exit ("%s: plg_name undefined", path);
        
    args = parse_plgargs (argc - 1, argv + 1);
    copyplg (h, name, path, args);
    if (kvs_commit (h) < 0)
        err_exit ("kvs_commit");
   // if (flux_plgctl_ins (h, name) < 0)
   //     err_exit ("flux_mon_plgctl_ins %s", name);
    msg ("plugin loaded");
    
    free (plugin_path);
    free (name);
    Jput (args);
    if (trypath)
        free (trypath);
}
*/
/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
