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
#include <sys/syslog.h>

#include "util/optparse.h"
#include "util/util.h"
#include "util/zmsg.h"
#include "cmb.h"

struct prog_ctx {
    cmb_t cmb;
    kvsdir_t kvs;           /* Handle to this job's dir in kvs */
    int64_t id;             /* id of this execution */
    int nnodes;
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

static void log_fatal (struct prog_ctx *ctx, int code, char *format, ...)
{
    cmb_t c;
    va_list ap;
    va_start (ap, format);
    if ((ctx != NULL) && ((c = ctx->cmb) != NULL))
        cmb_vlog (c, LOG_EMERG, format, ap);
    else
        vfprintf (stderr, format, ap);
    va_end (ap);
    exit (code);
}

static int log_err (struct prog_ctx *ctx, const char *fmt, ...)
{
    cmb_t c = ctx->cmb;
    va_list ap;
    va_start (ap, fmt);
    cmb_vlog (c, LOG_ERR, fmt, ap);
    va_end (ap);
    return (-1);
}

static void log_msg (struct prog_ctx *ctx, const char *fmt, ...)
{
    cmb_t c = ctx->cmb;
    va_list ap;
    va_start (ap, fmt);
    cmb_vlog (c, LOG_INFO, fmt, ap);
    va_end (ap);
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
        log_err (ctx, "Failed to block signals in parent");

    ctx->signalfd = signalfd (-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (ctx->signalfd < 0)
        log_fatal (ctx, 1, "signalfd");
    return (0);
}

static char * ctime_iso8601_now (char *buf, size_t sz)
{
    struct tm tm;
    time_t now = time (NULL);

    memset (buf, 0, sz);

    if (!localtime_r (&now, &tm))
        return (NULL);
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
    memset (ctx, 0, sizeof (*ctx));
    zsys_handler_set (NULL); /* Disable czmq SIGINT/SIGTERM handlers */
    if (!ctx)
        log_fatal (ctx, 1, "malloc");
    ctx->zctx = zctx_new ();
    ctx->zl = zloop_new ();
    if (!ctx->zl)
        log_fatal (ctx, 1, "zloop_new");

    ctx->id = -1;
    ctx->nodeid = -1;

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

int json_array_to_argv (struct prog_ctx *ctx,
    json_object *o, char ***argvp, int *argcp)
{
    int i;
    if (json_object_get_type (o) != json_type_array) {
        log_err (ctx, "json_array_to_argv: not an array");
        errno = EINVAL;
        return (-1);
    }

    *argcp = json_object_array_length (o);
    if (*argcp <= 0) {
        log_err (ctx, "json_array_to_argv: array length = %d", *argcp);
        return (-1);
    }

    *argvp = xzmalloc ((*argcp + 1) * sizeof (char **));

    for (i = 0; i < *argcp; i++) {
        json_object *ox = json_object_array_get_idx (o, i);
        if (json_object_get_type (ox) != json_type_string) {
            log_err (ctx, "malformed cmdline");
            free (*argvp);
            return (-1);
        }
        (*argvp) [i] = strdup (json_object_get_string (ox));
        json_object_put (ox);
    }
    return (0);
}

int prog_ctx_load_lwj_info (struct prog_ctx *ctx, int64_t id)
{
    json_object *v;

    if (kvsdir_get (ctx->kvs, "cmdline", &v) < 0)
        log_fatal (ctx, 1, "kvs_get: cmdline");

    log_msg (ctx, "got cmdline object : '%s'", json_object_to_json_string (v));

    if (json_array_to_argv (ctx, v, &ctx->argv, &ctx->argc) < 0)
        log_fatal (ctx, 1, "Failed to get cmdline from kvs");
    json_object_put (v);

    if (kvsdir_get_int (ctx->kvs, "nprocs", &ctx->nprocs) < 0) /* Assume ENOENT */
        ctx->nprocs = 1;

    ctx->pids = xzmalloc (ctx->nprocs * sizeof (*ctx->pids));

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
        log_fatal (ctx, 1, "cmb_init");

    if (kvs_get_dir (ctx->cmb, KVS_GET_FILEVAL, &ctx->kvs,
                     "lwj.%lu", ctx->id) < 0) {
        log_fatal (ctx, 1, "kvs_get_dir (lwj.%lu): %s\n",
                   ctx->id, strerror (errno));
    }

    ctx->nodeid = cmb_rank (ctx->cmb);
    ctx->nnodes = cmb_size (ctx->cmb);
    log_msg (ctx, "initializing from CMB: rank=%d", ctx->nodeid);
    if (prog_ctx_load_lwj_info (ctx, ctx->id) < 0)
        log_fatal (ctx, 1, "Failed to load lwj info");

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
            log_fatal (ctx, 1, "dup2: %s", strerror (errno));

    closeall (3);
}

int update_job_state (struct prog_ctx *ctx, const char *state)
{
    char buf [64];
    char *key;
    json_object *to =
        json_object_new_string (ctime_iso8601_now (buf, sizeof (buf)));

    assert (cmb_rank (ctx->cmb) == 0);

    log_msg (ctx, "updating job state to %s", state);

    if (kvsdir_put_string (ctx->kvs, "state", state) < 0)
        return (-1);

    if (asprintf (&key, "%s-time", state) < 0)
        return (-1);
    if (kvsdir_put (ctx->kvs, key, to) < 0)
        return (-1);
    free (key);
    json_object_put (to);

    if (kvs_commit (ctx->cmb) < 0)
        return (-1);

    return (0);
}

int rexec_state_change (struct prog_ctx *ctx, const char *state)
{
    char *name;

    if (strcmp (state, "running") == 0)
        asprintf (&name, "lwj.%lu.startup", ctx->id);
    else
        asprintf (&name, "lwj.%lu.shutdown", ctx->id);

    /* Wait for all wrexecds to finish and commit */
    if (kvs_fence (ctx->cmb, name, cmb_size (ctx->cmb)) < 0)
        log_fatal (ctx, 1, "kvs_fence");

    /* Rank 0 updates job state */
    if ((cmb_rank (ctx->cmb) == 0) && update_job_state (ctx, state) < 0)
        log_fatal (ctx, 1, "update_job_state");

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
    json_object *o;
    char *key;
    int rc;
    int global_taskid = globalid (ctx, localid);

    o = json_task_info_object_create (ctx, ctx->argv [0], ctx->pids [localid]);

    asprintf (&key, "%d.procdesc", global_taskid);

    rc = kvsdir_put (ctx->kvs, key, o);
    free (key);
    json_object_put (o);
    //kvs_commit (ctx->cmb);

    if (rc < 0)
        return log_err (ctx, "kvs_put failure");
    return (0);
}

int send_startup_message (struct prog_ctx *ctx)
{
    int i;
    for (i = 0; i < ctx->nprocs; i++) {
        if (rexec_taskinfo_put (ctx, i) < 0)
            return (-1);
    }

    if (rexec_state_change (ctx, "running") < 0) {
        log_err (ctx, "rexec_state_change");
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

    if (kvs_put (ctx->cmb, key, o) < 0)
        return (-1);

    if (kvs_commit (ctx->cmb) < 0)
        return (-1);

    json_object_put (o);

    return (0);
}

int exec_command (struct prog_ctx *ctx, int i)
{
    pid_t cpid = fork ();

    if (cpid < 0)
        log_fatal (ctx, 1, "fork: %s", strerror (errno));
    if (cpid == 0) {
        //log_msg (ctx, "in child going to exec %s", ctx->argv [0]);

        setenvf ("MPIRUN_RANK",       1, "%d", globalid (ctx, i));
        setenvf ("CMB_LWJ_TASK_ID",       1, "%d", globalid (ctx, i));
        setenvf ("CMB_LWJ_LOCAL_TASK_ID", 1, "%d", i);

        /* give each task its own process group so we can use killpg(2) */
        setpgrp();
        if (execvp (ctx->argv [0], ctx->argv) < 0) {
            exit (255);
            //log_fatal (ctx, 1, "execvp: %s", strerror (errno));
        }
    }

    /*
     *  Parent: Close child fds
     */
    log_msg (ctx, "in parent: child pid[%d] = %d", i, cpid);
    ctx->pids [i] = cpid;

    return (0);
}

char *gtid_list_create (struct prog_ctx *ctx, char *buf, size_t len)
{
    char *str = NULL;
    int i, n = 0;
    int truncated = 0;

    memset (buf, 0, len);

    for (i = 0; i < ctx->nprocs; i++) {
        int count;

        if (!truncated)  {
            count = snprintf (buf + n, len - n, "%u,", globalid (ctx, i));

            if ((count >= (len - n)) || (count < 0))
                truncated = 1;
            else
                n += count;
        }
        else
            n += strlen (str) + 1;
    }

    if (truncated)
        buf [len - 1] = '\0';
    else {
        /*
         * Delete final separator
         */
        buf[strlen(buf) - 1] = '\0';
    }

    return (buf);
}


int exec_commands (struct prog_ctx *ctx)
{
    char buf [4096];
    int i;

    setenvf ("CMB_LWJ_ID",     1, "%d", ctx->id);
    setenvf ("CMB_LWJ_NNODES", 1, "%d", ctx->nnodes);
    setenvf ("CMB_NODE_ID",    1, "%d", ctx->nodeid);
    setenvf ("CMB_LWJ_NTASKS", 1, "%d", ctx->nprocs * ctx->nnodes);
    setenvf ("MPIRUN_NPROCS",  1, "%d", ctx->nprocs * ctx->nnodes);
    gtid_list_create (ctx, buf, sizeof (buf));
    setenvf ("CMB_LWJ_GTIDS",  1, "%s", buf);

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
        log_err (ctx, "waitpid ()");
        return (0);
    }

    id = pid_to_taskid (ctx, wpid);
    log_msg (ctx, "task%d: pid %d (%s) exited with status 0x%04x",
            id, wpid, ctx->argv [0], status);
    if (send_exit_message (ctx, id, status) < 0)
        log_msg (ctx, "Sending exit message failed!");
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
        log_err (ctx, "read");
        return (0);
    }
    else if (n != sizeof (si)) {
        log_err (ctx, "partial read?");
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
        log_msg (ctx, "rexec_cb: no msg to recv!");
        return (0);
    }
    free (zmsg_popstr (zmsg)); /* Destroy dealer id */

    if (cmb_msg_decode (zmsg, &tag, &o) < 0) {
        log_err (ctx, "cmb_msg_decode");
        return (0);
    }

    /* Got an incoming message from cmbd */
    if (strcmp (tag, "rexec.kill") == 0) {
        int sig = json_object_get_int (o);
        if (sig == 0)
            sig = 9;
        log_msg (ctx, "Killing jobid %lu with signal %d", ctx->id, sig);
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
        case -1 : exit (2);
        default : _exit(0);     /* exit parent */
    }

    if (setsid () < 0)
        exit (3);

    switch (fork ()) {
        case  0 : break;        /* child */
        case -1 : exit (4);
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
        log_fatal (NULL, 1, "--%s=%s invalid", name, s);
    return ((int) l);
}

int prog_ctx_get_id (struct prog_ctx *ctx, optparse_t p)
{
    const char *id;
    char *end;

    if (!optparse_getopt (p, "lwj-id", &id))
        log_fatal (ctx, 1, "Required argument --lwj-id missing");

    errno = 0;
    ctx->id = strtol (id, &end, 10);
    if (  (*end != '\0')
       || (ctx->id == 0 && errno == EINVAL)
       || (ctx->id == ULONG_MAX && errno == ERANGE))
           log_fatal (ctx, 1, "--lwj-id=%s invalid", id);

    return (0);
}

int main (int ac, char **av)
{
    int parent_fd = -1;
    struct prog_ctx *ctx = NULL;
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

    p = optparse_create (av[0]);
    if (optparse_add_option_table (p, opts) != OPTPARSE_SUCCESS)
        log_fatal (ctx, 1, "optparse_add_option_table");
    if (optparse_parse_args (p, ac, av) < 0)
        log_fatal (ctx, 1, "parse args");

    daemonize ();

    ctx = prog_ctx_create ();
    signalfd_setup (ctx);

    if (prog_ctx_get_id (ctx, p) < 0)
        log_fatal (ctx, 1, "Failed to get lwj id from cmdline");

    prog_ctx_init_from_cmb (ctx);
    cmb_log_set_facility (ctx->cmb, "wrexecd");
    prog_ctx_zmq_socket_setup (ctx);

    if ((cmb_rank (ctx->cmb) == 0) && update_job_state (ctx, "starting") < 0)
        log_fatal (ctx, 1, "update_job_state");

    if ((parent_fd = optparse_get_int (p, "parent-fd")) >= 0)
        prog_ctx_signal_parent (parent_fd);
    prog_ctx_zloop_init (ctx);
    exec_commands (ctx);

    while (zloop_start (ctx->zl) == 0)
        {log_msg (ctx, "EINTR?");}

    log_msg (ctx, "exiting...");

    prog_ctx_destroy (ctx);

    return (0);
}

/*
 *  vi: ts=4 sw=4 expandtab
 */
