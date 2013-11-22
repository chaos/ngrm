/***********************************************************************************/
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
/***********************************************************************************/

#ifndef FTB_CLIENT_LIB_DEFS_H
#define FTB_CLIENT_LIB_DEFS_H

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include "ftb_def.h"

/* *INDENT-OFF* */
#ifdef __cplusplus
extern "C" {
#endif
/* *INDENT-ON* */

/*
 * In case of some error, that FTB client just stop functioning,
 * all subsequent FTB calls will take no effect and return
 * FTB_ERR_GENERAL immediately
 */
#define FTB_ERR_HANDLE_NONE                      0x0

/*
 * If some error happened, FTB will generate an event and next
 * time when FTB_Catch is called, that event will be caught by
 * client.
 */
#define FTB_ERR_HANDLE_NOTIFICATION              0x1

/*
 * If some error happened, FTB will try to recover, but this option
 * will cause more resource usage and an additional thread in client
 * process.
 */
#define FTB_ERR_HANDLE_RECOVER                   0x2

/*
 * region, comp_cat and comp is set to FTB_eventspace_t,
 * which is very inefficient.
 */
typedef struct FTB_client_id {
    FTB_eventspace_t region;
    FTB_eventspace_t comp_cat;
    FTB_eventspace_t comp;
    FTB_client_name_t client_name;
    uint8_t ext;
} FTB_client_id_t;

struct FTB_client_handle {
    uint8_t valid;
    FTB_client_id_t client_id;
};

typedef struct FTB_id {
    FTB_location_id_t location_id;
    FTB_client_id_t client_id;
} FTB_id_t;

/*
 * region, comp_cat and comp is set to FTB_eventspace_t,
 * which is very inefficient.
 */
typedef struct FTB_event {
    FTB_eventspace_t region;
    FTB_eventspace_t comp_cat;
    FTB_eventspace_t comp;
    FTB_event_name_t event_name;
    FTB_severity_t severity;
    FTB_client_jobid_t client_jobid;
    FTB_client_name_t client_name;
    FTB_hostip_t hostname;
    uint16_t seqnum;
    uint8_t event_type;
    char event_payload[FTB_MAX_PAYLOAD_DATA];
#ifdef FTB_TAG
    FTB_tag_len_t len;
    char dynamic_data[FTB_MAX_DYNAMIC_DATA_SIZE];
#endif
} FTB_event_t;

struct FTB_subscribe_handle {
    FTB_client_handle_t client_handle;
    FTB_event_t subscription_event;
    uint8_t subscription_type;
    uint8_t valid;
};

struct FTB_event_handle {
    FTB_event_name_t event_name;
    FTB_severity_t severity;
    FTB_client_id_t client_id;
    uint16_t seqnum;
    FTB_location_id_t location_id;
};

/* *INDENT-OFF* */
#ifdef __cplusplus
}				/*extern "C" */
#endif
/* *INDENT-ON* */
#endif /*FTB_CLIENT_LIB_DEFS_H */
