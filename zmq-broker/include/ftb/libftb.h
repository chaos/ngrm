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

#ifndef LIBFTB_H
#define LIBFTB_H

#include "ftb_def.h"
#include "ftb_client_lib_defs.h"
//#include "ftb_manager_lib.h"

/* *INDENT-OFF* */
#ifdef __cplusplus
extern "C" {
#endif
/* *INDENT-ON* */

int FTB_Connect(const FTB_client_t * client_info, FTB_client_handle_t * client_handle);

int FTB_Publish(FTB_client_handle_t client_handle, const char *event_name,
                const FTB_event_properties_t * event_properties, FTB_event_handle_t * event_handle);

int FTB_Subscribe(FTB_subscribe_handle_t * subscribe_handle, FTB_client_handle_t client_handle,
                  const char *subscription_str, int (*callback) (FTB_receive_event_t *, void *),
                  void *arg);

int FTB_Unsubscribe(FTB_subscribe_handle_t * subscribe_handle);

int FTB_Declare_publishable_events(FTB_client_handle_t client_handle, const char *schema_file,
                                   const FTB_event_info_t * event_info, int num_events);

int FTB_Poll_event(FTB_subscribe_handle_t shandle, FTB_receive_event_t * receive_event);

int FTB_Disconnect(FTB_client_handle_t client_handle);

int FTB_Get_event_handle(const FTB_receive_event_t receive_event, FTB_event_handle_t * event_handle);

int FTB_Compare_event_handles(const FTB_event_handle_t event_handle1,
                              const FTB_event_handle_t event_handle2);

#ifdef FTB_TAG

/*
 *    FTB_Add_dynamic_tag, FTB_Remove_dynamic_tag, & FTB_Read_dynamic_tag
 *    Provide a simple mechanism to stamp some dynamic info, such as job id, onto the event thrown from a same client.
 *
 *    FTB_Add_dynamic_tag tells the FTB client library to stamp a tag on any events it throws out later. It will affect events
 *    thrown by all components linked with that client library.
 *    On success it returns FTB_SUCCESS
 *    If there is not enough space for the tag, FTB_ERR_TAG_NO_SPACE is returned.
 *    If the same tag already exists and it belongs to the same component (same client_handle), the tag data will get updated.
 *    If the same tag is added by another component, FTB_ERR_TAG_CONFLICT is returned.
 *
 *    FTB_Remove_dynamic_tag removes the previously added tags.
 *    On success it returns FTB_SUCCESS
 *    If the tag is not found, FTB_ERR_TAG_NOT_FOUND is returned.
 *    If the same tag is added by another component, FTB_ERR_TAG_CONFLICT is returned.
 *
 *    FTB_Read_dynamic_tag gets the value of a specific tag from an event.
 *    On success it returns FTB_SUCCESS, and the data len will be updated to the actual size of tag data.
 *    If there is no such tag with the event, FTB_ERR_TAG_NOT_FOUND is returned.
 *    If the data_len passed in is smaller than the actual data, FTB_ERR_TAG_NO_SPACE is returned.
 */
int FTB_Add_tag(FTB_client_handle_t handle, FTB_tag_t tag, const char *tag_data, FTB_tag_len_t data_len);


int FTB_Remove_tag(FTB_client_handle_t handle, FTB_tag_t tag);

int FTB_Read_tag(const FTB_receive_event_t * event, FTB_tag_t tag, char *tag_data,
                 FTB_tag_len_t * data_len);
#endif

/* *INDENT-OFF* */
#ifdef __cplusplus
} /*extern "C"*/
#endif
/* *INDENT-ON* */

#endif
