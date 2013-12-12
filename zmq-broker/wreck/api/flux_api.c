/*
 * $Header: $
 *--------------------------------------------------------------------------------
 * Copyright (c) 2013, Lawrence Livermore National Security, LLC. Produced at
 * the Lawrence Livermore National Laboratory. Written by Dong H. Ahn <ahn1@llnl.gov>.
 *--------------------------------------------------------------------------------
 *
 *  Update Log:
 *        Dec 12 2013 DHA: DIST_BOOTSTRAP_FILE support
 *        Oct 25 2013 DHA: Adapt to new CMB/KVS APIs
 *        Oct 07 2013 DHA: File created
 */


#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdbool.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h> 
#include <json/json.h>
#include <zmq.h>
#include <czmq.h>
#include "cmb.h"
#include "kvs.h"
#include "log.h"
#include "util.h"
#include "zmsg.h"
#include "flux_api.h"

typedef enum {
    level0 = 0,
    level1,
    level2,
    level3
} verbose_level_e;

#define NEW_LWJ_MSG_REQ            "job.create"
#define NEW_LWJ_MSG_REPLY          "job.create"
#define NEW_LWJ_MSG_REPLY_FIELD    "jobid"
#define BOOTSTRAP_FILE             "bootstrap_file"

#define JOB_STATE_RESERVED         "reserved"
#define JOB_STATE_STARTING         "starting"
#define JOB_STATE_RUNNING          "running"
#define JOB_STATE_COMPLETE         "complete"

#define JOB_STATE_KEY              "state"
#define JOB_CMDLINE_KEY            "cmdline"
#define JOB_NPROCS_KEY             "nprocs"
#define JOB_PROCTAB_KEY            "procdesc"
#define JOB_APP_BOOTSTRAP          "app-bootstrap"
#define JOB_APP_BOOTSTRAP_ARGV     "app-bootstrap-argv"

#define REXEC_PLUGIN_RUN_EVENT_MSG "event.rexec.run."
#define REXEC_PLUGIN_KILL_EVENT_MSG "event.rexec.kill."
#define FLUXAPI_MAX_STRING         1024


/******************************************************
*
* Static Data
*
*****************************************************/
static FILE 
*myout = NULL;

static cmb_t 
cmbcxt = NULL;

static 
verbose_level_e vlevel = level0;

static char 
myhostname[FLUXAPI_MAX_STRING];


/******************************************************
*
* Static Functions
*
*****************************************************/

static int
append_timestamp ( const char *ei,const char *fstr, 
		   char *obuf, uint32_t len)
{
    int rc;
    char timelog[PATH_MAX];
    const char *format = "%b %d %T";
    time_t t;
    time(&t);
    strftime (timelog, 
	      PATH_MAX, 
	      format, 
	      localtime(&t));

    rc = snprintf(obuf, 
		  len, 
		  "<%s> <Flux API> (%s): %s\n", 
		  timelog, 
		  ei, 
		  fstr);
    return rc;
} 


static flux_lwj_status_e
resolve_raw_state (const char *state_str)
{
    flux_lwj_status_e rc = status_null;

    if (!state_str) {
        error_log (
            "State value is null!",
            0);
        goto ret_loc;
    }

    if ( !strcmp (state_str, JOB_STATE_RESERVED)) {
        rc = status_registered;
    }
    else if ( !strcmp (state_str, JOB_STATE_STARTING)) {
        rc = status_registered;
    }
    else if ( !strcmp (state_str, JOB_STATE_RUNNING)) {
        rc = status_running;
    }
    else if ( !strcmp (state_str, JOB_STATE_COMPLETE)) {
        rc = status_completed;
    }

ret_loc:
    return rc;
}


static size_t
query_globalProcTableSizeOr0 (const flux_lwj_id_t *lwj)
{
    int krc        = 0;
    int64_t nprocs = 0;
    size_t retval  = -1;
    char kvs_key[FLUXAPI_MAX_STRING]
                   = {'\0'};
    kvsdir_t dirobj;

    /*
     * Retrieve the lwj root directory
     */
    snprintf (kvs_key,
        FLUXAPI_MAX_STRING,
        "lwj.%ld", *lwj);

    /*
     * Getting the lwj.* directory
     */
    if ( (krc = kvs_get_dir ((void *)cmbcxt,
                             &dirobj,
                             kvs_key)) < 0) {
        error_log (
            "kvs_get_dir returned error", 0);
        goto retloc;
    }

    /*
     * TODO: 10/23/2013 "size" should be store 
     * in lwj directory ask Mark about this again
     */
    if ( ( krc = kvsdir_get_int64 (dirobj,
                                JOB_NPROCS_KEY,
                                &nprocs)) < 0) {
        //
        // JOB_NPROCS_KEY doesn't exist. This isn't
        // an error in this routine 
        //
        retval = 0;
    }
    else {
        retval = (nprocs > 0)
                 ?  nprocs * cmb_size (cmbcxt) 
                 : 0;
    }

retloc:
    return retval;
}


static uint8_t *
read_whole_file (const char * bootpath, size_t *s)
{
    struct stat sb;
    uint8_t *contents; 
    if (stat (bootpath, &sb) != 0) {
        error_log (
            "Failed to stat bootstrapper: %s",
            0, bootpath);
        goto error;
    }

    contents = (uint8_t *) malloc (sb.st_size);
    FILE *fptr = fopen (bootpath, "r");
    if (fptr == NULL) {
        error_log (
            "Failed to open the bootstrapper: %s",
            0, bootpath);
        goto error;
    }

    /* TODO: fread wrapper */
    size_t n = fread (contents,
                      sizeof (uint8_t),
                      sb.st_size,
                      fptr);

    if (n != sb.st_size) {
        error_log (
            "Failed to open the bootstrapper: %s",
            0, bootpath);
        goto error;
    }
    *s = sb.st_size;

    fclose (fptr);
    return contents;

error:
    return NULL;
}


static flux_rc_e
put_bootstrapper_block (
                kvsdir_t rootdir, 
                const char * bootpath,
                char * const bootargv[])
{
    json_object *b_arr = NULL;
    char * const *b_argv = NULL;
    size_t s = 0;

    if (getenv ("DIST_BOOTSTRAP_FILE") != NULL) {

        uint8_t *contents = NULL;
        json_object *b = NULL;

        if ( !(contents = read_whole_file (bootpath, &s)) ) {
            error_log (
                "Failed to read bootstrapper: %s",
                0, bootpath);
            goto error;
        }

        b = json_object_new_object ();
        util_json_object_add_base64 (b,
                                     BOOTSTRAP_FILE,
                                     contents,
                                     (int) s);

        if ( kvsdir_put(rootdir, JOB_APP_BOOTSTRAP, b) < 0) {
            error_log (
                "Failed to put bootstrapper %s into KVS",
                0, bootpath);
            goto error;
        }
        json_object_put (b);
    }

    b_arr = json_object_new_array ();
    b_argv = bootargv;
    while (*b_argv != NULL) {
        json_object *o = json_object_new_string (*b_argv);
        json_object_array_add (b_arr, o);
        b_argv++;
    }

    if ( kvsdir_put (rootdir, JOB_APP_BOOTSTRAP_ARGV, b_arr) < 0 ) {
        error_log ("Failed to put bootstrapper argv", 0);
        goto error;
    }

    json_object_put (b_arr);

    return FLUX_OK;

error:
    return FLUX_ERROR;
}


static flux_rc_e
put_job_metadata (
                kvsdir_t rootdir, 
                int sync,
                const flux_lwj_id_t *coloc_lwj,
                const char * lwjpath,
                char * const lwjargv[],
                const char * bootpath,
                char * const bootargv[],
                int coloc,
                int nnodes,
                int nprocs_per_node)
{
    int krc = 0;
    char ** argvptr = NULL;
    json_object *cmd_array = NULL;

    if ( (krc = kvsdir_put_int64 (rootdir,
                           JOB_NPROCS_KEY,
                           nprocs_per_node))) { 
        error_log (
            "Failed to put nprocs file", 0);
        goto error;
    }

    cmd_array = json_object_new_array ();
    argvptr = (char **) lwjargv;
    while (*argvptr != NULL) {
        json_object *o = json_object_new_string (*argvptr);
        json_object_array_add (cmd_array, o);
        argvptr++;
    }

    if ( (krc = kvsdir_put (rootdir,
                            JOB_CMDLINE_KEY,
                            cmd_array)) < 0 ) {
        error_log ("Failed to put cmdline nprocs file", 0);
        goto error;
    }

    if (bootpath) {
        if ( put_bootstrapper_block (rootdir, 
                             bootpath, 
                             bootargv) != FLUX_OK) {
            error_log ("Failed to put bootstrapper into KVS", 0);
            goto error;
        }
    }

    if ( (krc = kvs_commit ((void *) cmbcxt) < 0)) {
        error_log ("kvs_put failed", 0);
        goto error;
    }

    json_object_put (cmd_array);
    return FLUX_OK;

error:
    return FLUX_ERROR;
}


static flux_rc_e
start_job (const flux_lwj_id_t *lwj)
{
    char event_msg[FLUXAPI_MAX_STRING] = {'\0'};

    /*
     * Now KVS has all information, 
     * so tell the rexec plug-in to run
     */
    snprintf (event_msg, FLUXAPI_MAX_STRING,
        "%s%lu", REXEC_PLUGIN_RUN_EVENT_MSG, *lwj);

    if ( cmb_event_send (cmbcxt, event_msg) < 0 ) {
        error_log ("Sending a run event failed"
                   "in FLUX_launch_spawn", 0);
        return FLUX_ERROR;
    }

    return FLUX_OK;
}


static flux_rc_e
kill_job (const flux_lwj_id_t *lwj)
{
    char event_msg[FLUXAPI_MAX_STRING] = {'\0'};

    /*
     * Now KVS has all information, 
     * so tell the rexec plug-in to run
     */
    snprintf (event_msg, FLUXAPI_MAX_STRING,
        "%s%lu", REXEC_PLUGIN_KILL_EVENT_MSG, *lwj);

    if ( cmb_event_send (cmbcxt, event_msg) < 0 ) {
        error_log ("Sending a kill event failed"
                   "in FLUX_launch_spawn", 0);
        return FLUX_ERROR;
    }

    return FLUX_OK;
}


static char *
get_hname_from_hosts (json_object *hosts, 
                int64_t nid)
{
    char *rc = NULL;

    json_object *hblurb
        = json_object_array_get_idx (hosts, nid);
    if (hblurb) {
        json_object *entry 
            = json_object_object_get (hblurb, "name"); 
        rc = strdup (json_object_get_string (entry));
        /* must not manually decr ref counts */
    }
    else {
        error_log (
            "hostname unavailable", 0);
    }

    return rc;
}


static flux_rc_e
iter_and_fill_procdesc (kvsdir_t dirobj,
               MPIR_PROCDESC_EXT *ptab_buf,
               const size_t ptab_buf_size,
               size_t *ret_ptab_size)
{
    int rank            = 0;
    int incr            = 0;
    int64_t pid         = 0;
    int64_t nid         = 0;
    const char *name    = NULL;
    const char *cmd_str = NULL;
    json_object *rankobj= NULL;
    json_object *hosts  = NULL;
    kvsitr_t iter;

    /* get the hosts array */
    if (kvs_get ((void*) cmbcxt, 
                 "hosts", &hosts) < 0) {
            error_log (
                "error kvsdir_get", 0);
            goto fatal;
    } 

    /*
     * TODO: 10/23/2013 tell Mark/Jim symlink structure 
     * I need to speed up this query
     */
    iter = kvsitr_create (dirobj);
    while ( (name = kvsitr_next (iter))
            && (incr < ptab_buf_size)) {

        /*
         * If an entry is a subdirectory, it is currently
         * only of the procdesc type. The scheme will be
         * broken when other types of dirs will be popluated. 
         */
        kvsdir_t procdir;

        if (!kvsdir_isdir (dirobj, name)) 
            continue;

        pid = 0;
        nid = 0;
        rank = atoi (name);    
        cmd_str = NULL;

        if ( kvsdir_get_dir (dirobj, 
                             &procdir,
                             name) < 0) {
            error_log (
                "error kvsdir_get_dir", 0);
            goto fatal;
        } 
     
        if ( kvsdir_get (procdir, 
                         "procdesc", 
                         &rankobj) < 0) {
            /* this isn't procdesc directory */
            continue;
        } 

        if ( util_json_object_get_string (rankobj, 
                                          "command",
                                          &cmd_str) < 0) {
            error_log (
                "proctable ill-formed (command)", 0);
            goto fatal;
        } 

        if ( util_json_object_get_int64 (rankobj, 
                                         "nodeid",
                                         &nid) < 0) {
            error_log (
                "proctable ill-formed (nodeid)", 0);
            goto fatal;
        } 

        if ( util_json_object_get_int64 (rankobj, 
                                         "pid",
                                         &pid) < 0) {
            error_log (
                "proctable ill-formed (pid)", 0);
                goto fatal;
        } 
       
        char *h = NULL;
        if ( (h = get_hname_from_hosts (hosts, nid))) {  
            ptab_buf[rank].pd.host_name = h; 
        }
        else {
            ptab_buf[rank].pd.host_name = strdup("NA");
        }
        ptab_buf[rank].pd.executable_name 
            = strdup (cmd_str);
        ptab_buf[rank].pd.pid = pid;
        ptab_buf[rank].mpirank = rank;
        ptab_buf[rank].cnodeid = nid;

        incr++;
            
        json_object_put (rankobj);
        kvsdir_destroy (procdir);
    }  

    json_object_put (hosts);
    kvsitr_destroy (iter);
    *ret_ptab_size = incr;

    return FLUX_OK;

fatal:
    return FLUX_ERROR; 
}


/******************************************************
*
* Public Interfaces
*
*****************************************************/

FILE *
set_log_fd (FILE *newfd)
{
    FILE *tmp = myout;
    myout = newfd;
    return tmp;
}


unsigned int
set_verbose_level (unsigned int level)
{
    int tmp = vlevel;
    vlevel = level;
    return tmp;
}


void
error_log (const char *format, unsigned int error, ...)
{    
    const char *ei_str      = error? "INFO" : "ERROR";
    char x_format[PATH_MAX] = {'\0'};
    va_list vlist;
         
    if ( ((int) vlevel) >= error) {
        append_timestamp (ei_str, format, 
            x_format, PATH_MAX);

        va_start (vlist, error);
        vfprintf (myout, x_format, vlist);
        va_end (vlist);
    }
}


flux_rc_e 
FLUX_init ()
{
    flux_rc_e rc = FLUX_OK;

    if (myout == NULL) {
        myout = stdout;
    }	

    if (gethostname (myhostname, FLUXAPI_MAX_STRING) < 0) {
	error_log (
	    "Initializing hostname failed", 0);
	rc = FLUX_ERROR;
    }

    if ( (cmbcxt = cmb_init ()) == NULL) {
	error_log (
	    "Initializing CMB (cmb_init) failed", 0);
	rc = FLUX_ERROR;
    }

    return rc;
}


flux_rc_e 
FLUX_fini ()
{
    flux_rc_e rc = FLUX_OK;

    if (cmbcxt) {
	cmb_fini (cmbcxt);
    }
    else {
	rc = FLUX_ERROR; 
	error_log (
            "CMB never initialized?", 0);
    }

    return rc;
}


flux_rc_e 
FLUX_update_createLWJCxt (int s, flux_lwj_id_t *lwj)
{
    int rc              = FLUX_ERROR;
    int64_t jobid       = -1;
    char *tag           = NULL;
    json_object *jobreq = NULL;
    json_object *o      = NULL;
    char hn[FLUXAPI_MAX_STRING] = {'\0'};
    char stinfo[FLUXAPI_MAX_STRING] = {'\0'};
    int mypid = -1;

    /* Creating an empty lwj context in KVS 
     * through job plugin
     */
    jobreq = json_object_new_object ();

    if (s != 0) {
        if (gethostname (hn, FLUXAPI_MAX_STRING) < 0) {
            error_log ("gethostname returned an error", 0);
            goto cmb_error;
        }
        mypid = getpid ();
        snprintf (stinfo, FLUXAPI_MAX_STRING,
            "%s-%d", hn, mypid);

        util_json_object_add_string (jobreq, 
            "wreckrun-info", stinfo);
    }

    if ( (rc = cmb_send_message (cmbcxt, jobreq, 
			         NEW_LWJ_MSG_REQ)) < 0) {   
	error_log (
            "Sending a cmb msg failed"
            "in FLUX_update_createLWJCxt", 0);
	goto cmb_error;
    }
    json_object_put (jobreq);

    /*
     * nonblocking flag is false: o is a tuple
     */ 
    if ( (rc = cmb_recv_message (cmbcxt, &tag, 
                                 &o, false)) < 0 ) {
	error_log (
            "Failed to receive a cmb msg"
            "in FLUX_update_createLWJCxt", 0);
	goto cmb_error;
    }

    if ( (rc = strcmp (tag, NEW_LWJ_MSG_REPLY) != 0 )) {	
	error_log (
            "Tag mismatch in FLUX_update_createLWJCxt: %s", 
            0, tag);
        free (tag);
	goto cmb_error;
    }
    free (tag);

    if ( (rc = util_json_object_get_int64 (o, 
                    NEW_LWJ_MSG_REPLY_FIELD, &jobid) < 0)) {
	error_log (
            "Failed to get jobid from json = %s", 0,
            json_object_to_json_string (o));		
	goto cmb_error;
    }
    
    *lwj = jobid;
    json_object_put (o);

    return FLUX_OK;

cmb_error:
    return rc;
}


flux_rc_e 
FLUX_update_destoryLWJCxt (const flux_lwj_id_t *lwj)
{        
    return FLUX_OK;
}


flux_rc_e 
FLUX_query_pid2LWJId (
                 const flux_starter_info_t *starter,
		 flux_lwj_id_t *lwj)
{
    char kvs_key[FLUXAPI_MAX_STRING];
    int64_t intval;

    snprintf (kvs_key, FLUXAPI_MAX_STRING,
        "starterinfo.%s-%d",
        starter->hostname, starter->pid);    

    if ( kvs_get_int64 ((void *)cmbcxt,
                        kvs_key, &intval) < 0 ) {
        error_log (
            "kvs_get_int64 returned error", 0);
        goto error;
    }

    if ( intval < 0 ) {
        error_log (
            "invalid lwj id", 0);
        goto error;
    }

    *lwj = intval;
    return FLUX_OK; 

error:
    return FLUX_ERROR;
}


flux_rc_e 
FLUX_query_LWJId2JobInfo (
	         const flux_lwj_id_t *lwj, 
		 flux_lwj_info_t *lwj_info)
{
    int krc              = -1;
    int rc               = FLUX_OK;
    char *st_lwj         = NULL;
    flux_lwj_status_e st = status_null;
    char kvs_key[FLUXAPI_MAX_STRING] 
                         = {'\0'};
    kvsdir_t dirobj;

    snprintf (kvs_key,
        FLUXAPI_MAX_STRING,
        "lwj.%ld", *lwj);

    /*
     * Getting the lwj.* directory
     */
    if ( (krc = kvs_get_dir ((void *)cmbcxt, 
                             &dirobj,
                             kvs_key)) < 0) {
        error_log (
            "kvs_get_dir returned error", 0);
        rc = FLUX_ERROR;
        goto error;
    }

    /*
     * Getting the state file.
     */
    if ( (krc = kvsdir_get_string (dirobj,
                               JOB_STATE_KEY,
                               &st_lwj)) < 0 ) {
        error_log (
            "key not found? %s", 
            0, JOB_STATE_KEY);
        rc = FLUX_ERROR;
        goto error;

    }
    kvsdir_destroy (dirobj);

    st = resolve_raw_state (st_lwj);
    free (st_lwj);

    memcpy (&(lwj_info->lwj), lwj, sizeof(*lwj));
    lwj_info->lwjid = *lwj;
    lwj_info->status = st;
    lwj_info->starter.hostname = strdup (myhostname);
    lwj_info->starter.pid = *lwj;
    lwj_info->proc_table_size 
        = query_globalProcTableSizeOr0 (lwj);

    return rc;

error:
    return rc;
}


flux_rc_e 
FLUX_query_globalProcTableSize (
	         const flux_lwj_id_t *lwj,
		 size_t *count)
{
    flux_rc_e rc   = FLUX_OK;
    
    if ( (*count = query_globalProcTableSizeOr0 (
                                       lwj)) == 0) {

        error_log (
            "global process count unavailable!", 
            0);
        rc = FLUX_ERROR;
    }

    return rc;
}


flux_rc_e 
FLUX_query_globalProcTable (
	         const flux_lwj_id_t *lwj,
		 MPIR_PROCDESC_EXT *ptab_buf, 
		 const size_t ptab_buf_size,
                 size_t *ret_ptab_size)
{
    int krc      = 0;
    char kvs_key[FLUXAPI_MAX_STRING] = {'\0'};
    kvsdir_t dirobj;

    /*
     * Retrieve the lwj root directory
     */
    snprintf (kvs_key,
        FLUXAPI_MAX_STRING,
        "lwj.%ld", *lwj);
    if ( (krc = kvs_get_dir ((void *)cmbcxt,
                             &dirobj,
                             kvs_key)) < 0) {
        error_log (
            "kvs_get_dir returned error", 0);
        goto fatal;
    }

    if ( iter_and_fill_procdesc (dirobj, ptab_buf,
                                 ptab_buf_size,
                                 ret_ptab_size) != FLUX_OK) {
        error_log (
            "failed to fill procdesc", 0);
        goto fatal;
    }

    return FLUX_OK;

fatal:
    return FLUX_ERROR;
}


flux_rc_e 
FLUX_query_localProcTableSize (
	         const flux_lwj_id_t *lwj, 
		 const char *hostname, 
		 size_t *count)
{
    /* CMB routine to get procdesc field */
    error_log ("FLUX_query_localProcTableSize not yet implemented", 0); 
    return FLUX_NOT_IMPL;
}


flux_rc_e 
FLUX_query_localProcTable (
	         const flux_lwj_id_t *lwj,
		 const char *hostname,
		 MPIR_PROCDESC_EXT *ptab_buf, 
		 const size_t ptab_buf_size,
                 size_t *ret_ptab_size)
{
    /* CMB routine to get procdesc field */
    error_log ("FLUX_query_localProcTable not yet implemented", 0); 
    return FLUX_NOT_IMPL;
}


flux_rc_e 
FLUX_query_LWJStatus (
	         const flux_lwj_id_t *lwj, 
		 flux_lwj_status_e *status)
{
    flux_lwj_info_t lwjInfo;

    /*
     * Perf optimization may be needed here
     */
    if (FLUX_query_LWJId2JobInfo (lwj, &lwjInfo) != FLUX_OK) {
        *status = lwjInfo.status;
        error_log (
            "Failed to fetch lwj info", 0);
        return FLUX_ERROR; 
    }

    *status = lwjInfo.status;    
    return FLUX_OK;
}


flux_rc_e 
FLUX_monitor_registerStatusCb (
	         const flux_lwj_id_t *lwj, 
		 int (*cback) (flux_lwj_status_e *status))
{
    error_log ("FLUX_monitor_registerStatusCb not yet implemented", 0); 
    return FLUX_NOT_IMPL;
}


flux_rc_e
FLUX_launch_spawn (
		const flux_lwj_id_t *lwj, 
		int sync, 
		const flux_lwj_id_t *coloc_lwj,
                const char * lwjpath,
		char * const lwjargv[],
                const char * bootpath,
		char * const bootargv[],
                int coloc,
		int nnodes,
		int nprocs_per_node)
{ 
    int krc = 0;
    char *state_str = NULL;
    char kvs_key[FLUXAPI_MAX_STRING] = {'\0'};
    kvsdir_t rootdir;
    flux_lwj_status_e status;

    /*
     * Retrieve the target lwj root directory
     */
    snprintf (kvs_key, 
	FLUXAPI_MAX_STRING,
	"lwj.%ld", *lwj); 
    if ( (krc = kvs_get_dir ((void *) cmbcxt, 
                             &rootdir,
                             kvs_key)) < 0) {
	error_log ("kvs_get error", 0);
        goto error;
    }

    if ( (krc = kvsdir_get_string (rootdir,
                                   JOB_STATE_KEY,
                                   &state_str)) < 0) {
	error_log (
            "Failed to retrieve the job state", 0);
        goto error;
    }

    if ( (status = resolve_raw_state (state_str))
         != status_registered) {
	error_log (
            "job state (%d) isn't ready for launch", 
            0, status);
        goto error;
    }

    if ( put_job_metadata (rootdir, sync, coloc_lwj,
                           lwjpath, lwjargv,
                           bootpath, bootargv,
                           coloc, nnodes, 
                           nprocs_per_node) != FLUX_OK) {
	error_log ("failed to put job metadata", 0); 
        goto error;
    }

    if ( start_job (lwj) != FLUX_OK) {
	error_log ("failed to start the lwj", 0); 
        goto error;
    }

    return FLUX_OK;   

error:
    return FLUX_ERROR;	
}


flux_rc_e 
FLUX_control_killLWJ (
                 const flux_lwj_id_t *lwj)
{
    return kill_job (lwj);
}
