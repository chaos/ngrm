/*****************************************************************************/
/* This file is part of FTB (Fault Tolerance Backplance) - the core of CIFTS
 * (Co-ordinated Infrastructure for Fault Tolerant Systems)
 *
 * See http://www.mcs.anl.gov/research/cifts for more information.
 * 	
 */

/* FTB_Version: 0.6.2
 * FTB_API_Version: 0.5
 * FTB_Heredity:FOSS_ORIG
 * FTB_License:BSD
 */

/* This software is licensed under BSD. See the file FTB/misc/license.BSD for
 * complete details on your rights to copy, modify, and use this software.
 */
/*****************************************************************************/

#ifndef FTB_H
#define FTB_H

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>

/* *INDENT-OFF* */
#ifdef __cplusplus
extern "C" {
#endif
/* *INDENT-ON* */

#define FTB_SUCCESS                             0
#define FTB_ERR_GENERAL                         (-1)
#define FTB_ERR_EVENTSPACE_FORMAT               (-2)
#define FTB_ERR_SUBSCRIPTION_STYLE              (-3)
#define FTB_ERR_INVALID_VALUE                   (-4)
#define FTB_ERR_DUP_CALL                        (-5)
#define FTB_ERR_NULL_POINTER                    (-6)
#define FTB_ERR_NOT_SUPPORTED                   (-7)
#define FTB_ERR_INVALID_FIELD                   (-8)
#define FTB_ERR_INVALID_HANDLE                  (-9)
#define FTB_ERR_DUP_EVENT                       (-10)
#define FTB_ERR_INVALID_SCHEMA_FILE             (-11)
#define FTB_ERR_INVALID_EVENT_NAME              (-12)
#define FTB_ERR_INVALID_EVENT_TYPE              (-13)
#define FTB_ERR_SUBSCRIPTION_STR                (-14)
#define FTB_ERR_FILTER_ATTR                     (-15)
#define FTB_ERR_FILTER_VALUE                    (-16)
#define FTB_GOT_NO_EVENT                        (-17)
#define FTB_FAILURE                             (-18)
#define FTB_ERR_INVALID_PARAMETER               (-19)
#define FTB_ERR_NETWORK_GENERAL                 (-20)
#define FTB_ERR_NETWORK_NO_ROUTE                (-21)

/* If client will subscribe to any events */
#define FTB_SUBSCRIPTION_NONE               0x0

/* If client plans to poll - a polling queue is created */
#define FTB_SUBSCRIPTION_POLLING            0x1

/* If client plans to use callback handlers */
#define FTB_SUBSCRIPTION_NOTIFY             0x2

#define FTB_DEFAULT_POLLING_Q_LEN    64
#define FTB_MAX_CLIENTSCHEMA_VER     8
#define FTB_MAX_EVENTSPACE           64
#define FTB_MAX_CLIENT_NAME          16
#define FTB_MAX_CLIENT_JOBID         16
#define FTB_MAX_EVENT_NAME           32
#define FTB_MAX_SEVERITY             16
#define FTB_MAX_HOST_ADDR            64
#define FTB_MAX_PID_TIME             32
#define FTB_MAX_PAYLOAD_DATA         368

/*FTB_MAX_SUBSCRIPTION_STYLE included as part of an event message */
#define FTB_MAX_SUBSCRIPTION_STYLE	 32

/*
 * The FTB_EVENT_SIZE field size is just sufficient for the event +
 * event_handle (and event_type), if needed
 */
#define FTB_EVENT_SIZE               720

typedef char FTB_eventspace_t[FTB_MAX_EVENTSPACE];
typedef char FTB_client_name_t[FTB_MAX_CLIENT_NAME];
typedef char FTB_client_schema_ver_t[FTB_MAX_CLIENTSCHEMA_VER];
typedef char FTB_client_jobid_t[FTB_MAX_CLIENT_JOBID];
typedef char FTB_severity_t[FTB_MAX_SEVERITY];
typedef char FTB_event_name_t[FTB_MAX_EVENT_NAME];
typedef char FTB_hostip_t[FTB_MAX_HOST_ADDR];
typedef char FTB_subscription_style_t[FTB_MAX_SUBSCRIPTION_STYLE];
typedef char FTB_pid_starttime_t[FTB_MAX_PID_TIME];

typedef struct FTB_client {
    FTB_client_schema_ver_t client_schema_ver;
    FTB_eventspace_t event_space;
    FTB_client_name_t client_name;
    FTB_client_jobid_t client_jobid;
    FTB_subscription_style_t client_subscription_style;
    int client_polling_queue_len;
} FTB_client_t;

typedef struct FTB_event_info {
    FTB_event_name_t event_name;
    FTB_severity_t severity;
} FTB_event_info_t;

typedef struct FTB_event_properties {
    uint8_t event_type;
    char event_payload[FTB_MAX_PAYLOAD_DATA];
} FTB_event_properties_t;

typedef struct FTB_location_id {
    char hostname[FTB_MAX_HOST_ADDR];
    FTB_pid_starttime_t pid_starttime;
    pid_t pid;
} FTB_location_id_t;

typedef struct FTB_receive_event_info {
    FTB_eventspace_t event_space;
    FTB_event_name_t event_name;
    FTB_severity_t severity;
    FTB_client_jobid_t client_jobid;
    FTB_client_name_t client_name;
    uint8_t client_extension;
    uint16_t seqnum;
    FTB_location_id_t incoming_src;
    uint8_t event_type;
    char event_payload[FTB_MAX_PAYLOAD_DATA];
} FTB_receive_event_t;

typedef struct FTB_client_handle *FTB_client_handle_t;
typedef struct FTB_subscribe_handle *FTB_subscribe_handle_t;
typedef struct FTB_event_handle *FTB_event_handle_t;


int FTB_Connect(const FTB_client_t * client_info,
		FTB_client_handle_t * client_handle);

int FTB_Publish(FTB_client_handle_t client_handle,
		const char *event_name,
                const FTB_event_properties_t * event_properties,
		FTB_event_handle_t * event_handle);

int FTB_Subscribe(FTB_subscribe_handle_t * subscribe_handle,
		FTB_client_handle_t client_handle,
		const char *subscription_str,
		int (*callback) (FTB_receive_event_t *, void *),
                void *arg);

int FTB_Unsubscribe(FTB_subscribe_handle_t * subscribe_handle);

int FTB_Declare_publishable_events(FTB_client_handle_t client_handle,
		const char *schema_file,
		const FTB_event_info_t * event_info,
		int num_events);

int FTB_Poll_event(FTB_subscribe_handle_t shandle,
		FTB_receive_event_t * receive_event);

int FTB_Disconnect(FTB_client_handle_t client_handle);

int FTB_Get_event_handle(const FTB_receive_event_t receive_event,
		FTB_event_handle_t * event_handle);

int FTB_Compare_event_handles(const FTB_event_handle_t event_handle1,
                              const FTB_event_handle_t event_handle2);

/* *INDENT-OFF* */
#ifdef __cplusplus
} /*extern "C"*/
#endif
/* *INDENT-ON* */

#endif
