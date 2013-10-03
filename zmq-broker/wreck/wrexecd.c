#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/signalfd.h>
#include <json/json.h>
#include <czmq.h>

#include "util/optparse.h"
#include "util/util.h"
#include "util/zmsg.h"
#include "util/log.h"
#include "cmb.h"

struct prog_ctx {
    cmb_t cmb;
    int64_t id;             /* id of this execution */
    int nodeid;
    int nprocs;             /* number of copies of command to execute */

    int argc;
    char **argv;

    zctx_t *zctx;
    zloop_t *zl;            /* zmq event loop       */
    void *zs_req;
    void *zs_rep;
    int signalfd;
    int *pids;
    int exited;
};

void *lsd_nomem_error (const char *file, int line, char *msg)
{
    return (NULL);
}

int globalid (struct prog_ctx *ctx, int localid)
{
    return ((ctx->nodeid * ctx->nprocs) + localid);
}

int signalfd_setup (struct prog_ctx *ctx)
{
    sigset_t mask;

    sigemptyset (&mask);
    sigaddset (&mask, SIGCHLD);
    sigaddset (&mask, SIGTERM);
    sigaddset (&mask, SIGINT);

    if (sigprocmask (SIG_BLOCK, &mask, NULL) < 0)
        err ("Failed to block signals in parent");

    ctx->signalfd = signalfd (-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (ctx->signalfd < 0)
        err_exit ("signalfd");
    return (0);
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

/*
 *  Send a message to rexec plugin
 */
int rexec_send_msg (struct prog_ctx *ctx, char *tag, json_object *o)
{
    zmsg_t *zmsg = cmb_msg_encode (tag, o);
    if (!zmsg)
        return (-1);
    fprintf (stderr, "rexec_send_msg:\n");
    zmsg_dump (zmsg);
    return zmsg_send (&zmsg, ctx->zs_req);
}

void prog_ctx_destroy (struct prog_ctx *ctx)
{
    zloop_destroy (&ctx->zl);
    free (ctx->pids);
    close (ctx->signalfd);

    zmq_close (ctx->zs_req);
    zmq_close (ctx->zs_rep);

    zmq_term (ctx->zctx);

    free (ctx);
}

struct prog_ctx * prog_ctx_create (void)
{
    struct prog_ctx *ctx = malloc (sizeof (*ctx));
    zsys_handler_set (NULL); /* Disable czmq SIGINT/SIGTERM handlers */
    if (!ctx)
        err_exit ("malloc");
    ctx->zctx = zctx_new ();
    ctx->zl = zloop_new ();
    if (!ctx->zl)
        err_exit ("zloop_new");

    ctx->id = -1;
    ctx->nodeid = -1;
    ctx->exited = 0;

    return (ctx);
}

static int prog_ctx_zmq_socket_setup (struct prog_ctx *ctx)
{
    char uri [1024];
    unsigned long uid = geteuid();

    snprintf (uri, sizeof (uri), "ipc:///tmp/cmb-%d-%lu-rexec-req-%lu",
                ctx->nodeid, uid, ctx->id);
    zbind (ctx->zctx, &ctx->zs_rep, ZMQ_ROUTER, uri, -1);

    snprintf (uri, sizeof (uri), "ipc:///tmp/cmb-%d-%lu-rexec-rep-%lu",
                ctx->nodeid, uid, ctx->id);
    zconnect (ctx->zctx, &ctx->zs_req, ZMQ_DEALER, uri, -1, NULL);

    return (0);
}

int json_array_to_argv (json_object *o, char ***argvp, int *argcp)
{
    int i;
    if (json_object_get_type (o) != json_type_array) {
        err ("json_array_to_argv: not an array");
        errno = EINVAL;
        return (-1);
    }

    *argcp = json_object_array_length (o);
    if (*argcp <= 0) {
        err ("json_array_to_argv: array length = %d", *argcp);
        return (-1);
    }

    *argvp = xzmalloc ((*argcp + 1) * sizeof (char **));

    for (i = 0; i < *argcp; i++) {
        json_object *ox = json_object_array_get_idx (o, i);
        if (json_object_get_type (ox) != json_type_string) {
            err ("malformed cmdline");
            free (*argvp);
            return (-1);
        }
        (*argvp) [i] = strdup (json_object_get_string (ox));
        json_object_put (ox);
    }
    return (0);
}

json_object *kvs_dir_get_file (json_object *dir, char *key)
{
    json_object *f;
    if ((f = json_object_object_get (dir, key)))
        return json_object_object_get (f, "FILEVAL");
    return (NULL);
}

int prog_ctx_load_lwj_info (struct prog_ctx *ctx, int64_t id)
{
    char *key;
    json_object *dir;
    json_object *v;

    if (asprintf (&key, "lwj.%lu", id) < 0)
        err_exit ("asprintf");

    if (cmb_kvs_get (ctx->cmb, key, &dir, KVS_GET_DIR) < 0)
        err_exit ("cmb_kvs_get (%s)", key);

    if (!(v = kvs_dir_get_file (dir, "cmdline")))
        err_exit ("object_get: cmdline");

    msg ("got cmdline object : '%s'", json_object_to_json_string (v));

    if (json_array_to_argv (v, &ctx->argv, &ctx->argc) < 0)
        err_exit ("Failed to get cmdline from kvs");

    if ((v = kvs_dir_get_file (dir, "nprocs"))) {
        if ((ctx->nprocs = json_object_get_int (v)) <= 0)
            err_exit ("Failed to get nprocs from kvs: %d", ctx->nprocs);
    }
    else
        ctx->nprocs = 1;

    ctx->pids = xzmalloc (ctx->nprocs * sizeof (*ctx->pids));

    json_object_put (dir);
    return (0);
}

int prog_ctx_signal_parent (int fd)
{
    int rc;
    char c = '\0';
    /*
     * Signal parent we are ready
     */
    rc = write (fd, &c, 1);
    close (fd);
    return (rc);
}

int prog_ctx_init_from_cmb (struct prog_ctx *ctx)
{
    /*
     * Connect to CMB over api socket
     */
    if (!(ctx->cmb = cmb_init ()))
        err_exit ("cmb_init");

    ctx->nodeid = cmb_rank (ctx->cmb);
    msg ("initializing from CMB: rank=%d", ctx->nodeid);
    if (prog_ctx_load_lwj_info (ctx, ctx->id) < 0)
        err_exit ("Failed to load lwj info");


    return (0);
}

void closeall (int fd)
{
    int fdlimit = sysconf (_SC_OPEN_MAX);

    while (fd < fdlimit)
        close (fd++);
    return;
}

void child_io_devnull (struct prog_ctx *ctx)
{
    int devnull = open ("/dev/null", O_RDWR);
    /*
     *  Dup appropriate fds onto child STDIN/STDOUT/STDERR
     */
    if (  (dup2 (devnull, STDIN_FILENO) < 0)
       || (dup2 (devnull, STDOUT_FILENO) < 0)
       || (dup2 (devnull, STDERR_FILENO) < 0))
            err_exit ("dup2: %s", strerror (errno));

    closeall (3);
}

int update_job_state (struct prog_ctx *ctx, const char *state)
{
    char buf [64];
    char *key;
    json_object *o = json_object_new_string (state);
    json_object *to =
        json_object_new_string (ctime_iso8601_now (buf, sizeof (buf)));

    assert (cmb_rank (ctx->cmb) == 0);

    msg ("updating job state to %s", state);

    if (asprintf (&key, "lwj.%lu.state", ctx->id) < 0)
        return (-1);

    if (cmb_kvs_put (ctx->cmb, key, o) < 0)
        return (-1);

    free (key);
    if (asprintf (&key, "lwj.%lu.%s-time", ctx->id, state) < 0)
        return (-1);

    if (cmb_kvs_put (ctx->cmb, key, to) < 0)
        return (-1);
    free (key);

    if (cmb_kvs_flush (ctx->cmb) < 0)
        return (-1);

    if (cmb_kvs_commit (ctx->cmb, NULL) < 0)
        return (-1);

    json_object_put (o);
    return (0);
}

int rexec_state_change (struct prog_ctx *ctx, const char *state)
{
    char *name;

    if (strcmp (state, "running") == 0)
        asprintf (&name, "lwj.%lu.startup", ctx->id);
    else
        asprintf (&name, "lwj.%lu.shutdown", ctx->id);

    /* Wait for all cmb to finish */
    if (cmb_barrier (ctx->cmb, name, cmb_size (ctx->cmb)) < 0)
        err_exit ("cmb_barrier");

    /* Commit any new namespace */
    if (cmb_kvs_commit (ctx->cmb, name) < 0)
        err_exit ("state_change: cmb_kvs_commit");

    /* Rank 0 updates job state */
    if ((cmb_rank (ctx->cmb) == 0) && update_job_state (ctx, state) < 0)
        err_exit ("update_job_state");

    return (0);
}


json_object * json_task_info_object_create (struct prog_ctx *ctx,
    const char *cmd, pid_t pid)
{
    json_object *o = json_object_new_object ();
    json_object *ocmd = json_object_new_string (cmd);
    json_object *opid = json_object_new_int (pid);
    json_object *onodeid = json_object_new_int (ctx->nodeid);
    json_object_object_add (o, "command", ocmd);
    json_object_object_add (o, "pid", opid);
    json_object_object_add (o, "nodeid", onodeid);
    return (o);
}

int rexec_taskinfo_put (struct prog_ctx *ctx, int localid)
{
    char *key;
    json_object *o;
    int global_taskid = globalid (ctx, localid);

    o = json_task_info_object_create (ctx, ctx->argv [0], ctx->pids [localid]);
    if (asprintf (&key, "lwj.%lu.%d.procdesc", ctx->id, global_taskid) < 0) {
        err ("asprintf failure");
        return (-1);
    }
    msg ("cmb_kvs_put: %s = %s", key, json_object_to_json_string (o));
    if (cmb_kvs_put (ctx->cmb, key, o) < 0) {
        err ("kvs_put failure");
        return (-1);
    }

    json_object_put (o);
    return (0);
}

int send_startup_message (struct prog_ctx *ctx)
{
    int i;
    for (i = 0; i < ctx->nprocs; i++) {
        if (rexec_taskinfo_put (ctx, i) < 0)
            return (-1);
    }

    if (cmb_kvs_flush (ctx->cmb) < 0) {
        err ("cmb_kvs_flush");
        return (-1);
    }

    if (rexec_state_change (ctx, "running") < 0) {
        err ("rexec_state_change");
        return (-1);
    }

    return (0);
}

int send_exit_message (struct prog_ctx *ctx, int taskid, int status)
{
    char *key;
    int global_taskid = globalid (ctx, taskid);
    json_object *o = json_object_new_int (status);

    if (asprintf (&key, "lwj.%lu.%d.exit_status", ctx->id, global_taskid) < 0)
        return (-1);

    if (cmb_kvs_put (ctx->cmb, key, o) < 0)
        return (-1);

    if (cmb_kvs_flush (ctx->cmb) < 0)
        return (-1);

    json_object_put (o);

    return (0);
}

int exec_command (struct prog_ctx *ctx, int i)
{
    pid_t cpid = fork ();

    if (cpid < 0)
        err_exit ("fork: %s", strerror (errno));
    if (cpid == 0) {
        msg ("in child going to exec %s", ctx->argv [0]);
        child_io_devnull (ctx);

        /* give each task its own process group so we can use killpg(2) */
        setpgrp();
        if (execvp (ctx->argv [0], ctx->argv) < 0)
            err_exit ("execvp: %s", strerror (errno));
    }

    /*
     *  Parent: Close child fds
     */
    msg ("in parent: child pid[%d] = %d", i, cpid);
    ctx->pids [i] = cpid;

    return (0);
}

int exec_commands (struct prog_ctx *ctx)
{
    int i;
    for (i = 0; i < ctx->nprocs; i++)
        exec_command (ctx, i);

    return send_startup_message (ctx);
}

int pid_to_taskid (struct prog_ctx *ctx, pid_t pid)
{
    int i = 0;
    while (ctx->pids [i] != pid)
        i++;

    if (i >= ctx->nprocs)
        return (-1);

    return (i);
}

int reap_child (struct prog_ctx *ctx)
{
    int id;
    int status;
    pid_t wpid;

    wpid = waitpid ((pid_t) -1, &status, WNOHANG);
    if (wpid == (pid_t) 0)
        return (0);

    if (wpid < (pid_t) 0) {
        err ("waitpid ()");
        return (0);
    }

    id = pid_to_taskid (ctx, wpid);
    msg ("task%d: pid %d (%s) exited with status 0x%04x",
            id, wpid, ctx->argv [0], status);
    if (send_exit_message (ctx, id, status) < 0)
        msg ("Sending exit message failed!");
    return (1);
}

int prog_ctx_signal (struct prog_ctx *ctx, int sig)
{
    int i;
    for (i = 0; i < ctx->nprocs; i++)
        killpg (ctx->pids [i], sig);
    return (0);
}

int cleanup (struct prog_ctx *ctx)
{
    return prog_ctx_signal (ctx, SIGKILL);
}

int signal_cb (zloop_t *zl, zmq_pollitem_t *zp, struct prog_ctx *ctx)
{
    int n;
    struct signalfd_siginfo si;

    n = read (zp->fd, &si, sizeof (si));
    if (n < 0) {
        err ("read");
        return (0);
    }
    else if (n != sizeof (si)) {
        err ("partial read?");
        return (0);
    }

    if (si.ssi_signo == SIGTERM || si.ssi_signo == SIGINT) {
        cleanup (ctx);
        return (0); /* Continue, so we reap children */
    }

    /* SIGCHLD assumed */

    while (reap_child (ctx)) {
        if (++ctx->exited == ctx->nprocs) {
            rexec_state_change (ctx, "complete");
            return (-1); /* Wakeup zloop */
        }
    }
    return (0);
}

int cmb_cb (zloop_t *zl, zmq_pollitem_t *zp, struct prog_ctx *ctx)
{
    char *tag;
    json_object *o;

    zmsg_t *zmsg = zmsg_recv (zp->socket);
    if (!zmsg) {
        msg ("rexec_cb: no msg to recv!");
        return (0);
    }
    free (zmsg_popstr (zmsg)); /* Destroy dealer id */

    if (cmb_msg_decode (zmsg, &tag, &o) < 0) {
        err ("cmb_msg_decode");
        return (0);
    }

    /* Got an incoming message from cmbd */
    if (strcmp (tag, "rexec.kill") == 0) {
        int sig = json_object_get_int (o);
        if (sig == 0)
            sig = 9;
        msg ("Killing jobid %lu with signal %d", ctx->id, sig);
        prog_ctx_signal (ctx, sig);
    }
    zmsg_destroy (&zmsg);
    json_object_put (o);
    return (0);
}

int prog_ctx_zloop_init (struct prog_ctx *ctx)
{
    zmq_pollitem_t zp = { .fd = -1, .events = 0, .revents = 0, .socket = 0 };

    /*
     *  Listen for "events" coming from the signalfd
     */
    zp.events = ZMQ_POLLIN | ZMQ_POLLERR;
    zp.fd = ctx->signalfd;
    zloop_poller (ctx->zl, &zp, (zloop_fn *) signal_cb, (void *) ctx);

    /*
     *  Add a handler for events coming from CMB
     */
    zp.fd = -1;
    zp.socket = ctx->zs_rep;
    zloop_poller (ctx->zl, &zp, (zloop_fn *) cmb_cb, (void *) ctx);

    return (0);
}

static void daemonize ()
{
    switch (fork ()) {
        case  0 : break;        /* child */
        case -1 : err_exit ("fork");
        default : _exit(0);     /* exit parent */
    }

    if (setsid () < 0)
        err_exit ("setsid");

    switch (fork ()) {
        case  0 : break;        /* child */
        case -1 : err_exit ("fork");
        default : _exit(0);     /* exit parent */
    }
}

int optparse_get_int (optparse_t p, char *name)
{
    long l;
    char *end;
    const char *s;

    if (!optparse_getopt (p, name, &s))
        return (-1);

    l = strtol (s, &end, 10);
    if ((end == s) || (*end != '\0') || (l < 0) || (l > INT_MAX))
        err_exit ("--%s=%s invalid", name, s);
    return ((int) l);
}

int prog_ctx_get_id (struct prog_ctx *ctx, optparse_t p)
{
    const char *id;
    char *end;

    if (!optparse_getopt (p, "lwj-id", &id))
        err_exit ("Required argument --lwj-id missing");

    errno = 0;
    ctx->id = strtol (id, &end, 10);
    if (  (*end != '\0')
       || (ctx->id == 0 && errno == EINVAL)
       || (ctx->id == ULONG_MAX && errno == ERANGE))
           err_exit ("--lwj-id=%s invalid", id);

    return (0);
}

int main (int ac, char **av)
{
    int parent_fd = -1;
    struct prog_ctx *ctx;
    optparse_t p;
    struct optparse_option opts [] = {
        { .name =    "lwj-id",
          .key =     1000,
          .has_arg = 1,
          .arginfo = "ID",
          .usage =   "Operate on LWJ id [ID]",
        },
        { .name =    "parent-fd",
          .key =     1001,
          .has_arg = 1,
          .arginfo = "FD",
          .usage =   "Signal parent on file descriptor [FD]",
        },
        OPTPARSE_TABLE_END,
    };

    log_init ("rexecd");

    p = optparse_create (av[0]);
    if (optparse_add_option_table (p, opts) != OPTPARSE_SUCCESS)
        err_exit ("optparse_add_option_table");
    if (optparse_parse_args (p, ac, av) < 0)
        err_exit ("parse args");

    daemonize ();

    ctx = prog_ctx_create ();
    signalfd_setup (ctx);

    if (prog_ctx_get_id (ctx, p) < 0)
        err_exit ("Failed to get lwj id from cmdline");

    prog_ctx_init_from_cmb (ctx);
    prog_ctx_zmq_socket_setup (ctx);

    if ((cmb_rank (ctx->cmb) == 0) && update_job_state (ctx, "starting") < 0)
        err_exit ("update_job_state");

    if ((parent_fd = optparse_get_int (p, "parent-fd")) >= 0)
        prog_ctx_signal_parent (parent_fd);
    prog_ctx_zloop_init (ctx);
    exec_commands (ctx);

    while (zloop_start (ctx->zl) == 0)
        {msg ("EINTR?");}

    msg ("exiting...");

    prog_ctx_destroy (ctx);

    return (0);
}

/*
 *  vi: ts=4 sw=4 expandtab
 */
