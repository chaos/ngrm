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

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdarg.h>
#include <czmq.h>
#include <json.h>

#include "log.h"
#include "xzmalloc.h"
#include "shortjson.h"

#include "flux.h"

JSON flux_lspeer (flux_t h, int rank)
{
    JSON request = Jnew ();
    JSON response = NULL;

    response = flux_rank_rpc (h, rank, request, "cmb.lspeer");
    Jput (request);
    return response;
}

int flux_reparent (flux_t h, int rank, const char *uri)
{
    JSON request = Jnew ();
    JSON response = NULL;
    int rc = -1;

    if (!uri) {
        errno = EINVAL;
        goto done;
    }
    Jadd_str (request, "uri", uri);
    if ((response = flux_rank_rpc (h, rank, request, "cmb.reparent"))) {
        errno = EPROTO;
        goto done;
    }
    if (errno != 0)
        goto done;
    rc = 0;
done:
    Jput (request);
    Jput (response);
    return rc;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
