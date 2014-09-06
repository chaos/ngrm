/*****************************************************************************\
 *  Copyright (c) 2014 Lawrence Livermore National Security, LLC.  Produced at
 *  the Lawrence Livermore National Laboratory (cf, AUTHORS, DISCLAIMER.LLNS).
 *  LLNL-CODE-658032 All rights reserved.
 *
 *  This file is part of the Flux resource manager framework.
 *  For details, see https://github.com/flux-framework.
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the license, or (at your option)
 *  any later version.
 *
 *  Flux is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *  See also:  http://www.gnu.org/licenses/
\*****************************************************************************/

/* flux-mon.c - flux mon subcommand */

#if HAVE_CONFIG_H
#include "config.h"
#endif
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

static void mon_list (flux_t h, int argc, char *argv[]);
static void mon_add (flux_t h, int argc, char *argv[]);
static void mon_del (flux_t h, int argc, char *argv[]);

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

    if (!strcmp (cmd, "list"))
        mon_list (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "add"))
        mon_add (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "del"))
        mon_del (h, argc - optind, argv + optind);
    else
        usage ();

    flux_handle_destroy (&h);
    log_fini ();
    return 0;
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

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
