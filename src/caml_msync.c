/*****************************************************************************/
/*                                                                           */
/* Open Source License                                                       */
/* Copyright (c) 2020 DaiLambda, Inc. <contact@dailambda.jp>                 */
/*                                                                           */
/* Permission is hereby granted, free of charge, to any person obtaining a   */
/* copy of this software and associated documentation files (the "Software"),*/
/* to deal in the Software without restriction, including without limitation */
/* the rights to use, copy, modify, merge, publish, distribute, sublicense,  */
/* and/or sell copies of the Software, and to permit persons to whom the     */
/* Software is furnished to do so, subject to the following conditions:      */
/*                                                                           */
/* The above copyright notice and this permission notice shall be included   */
/* in all copies or substantial portions of the Software.                    */
/*                                                                           */
/* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*/
/* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  */
/* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   */
/* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*/
/* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   */
/* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       */
/* DEALINGS IN THE SOFTWARE.                                                 */
/*                                                                           */
/*****************************************************************************/


/* Needed (under Linux at least) to get pwrite's prototype in unistd.h.
   Must be defined before the first system .h is included. */
#define _XOPEN_SOURCE 600

#include <stddef.h>
#include "caml/bigarray.h"
#include "caml/fail.h"
#include "caml/io.h"
#include "caml/mlvalues.h"
#include "caml/signals.h"
#include "caml/sys.h"
#include "caml/unixsupport.h"

#include <errno.h>
#ifdef HAS_UNISTD
#include <unistd.h>
#endif
#ifdef HAS_MMAP
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

void caml_msync(void * addr, uintnat len)
{
  uintnat page = sysconf(_SC_PAGESIZE);
  uintnat delta = (uintnat) addr % page;
  if (len == 0) return;
  addr = (void *)((uintnat)addr - delta);
  len  = len + delta;
  msync(addr, len, MS_SYNC);
}

void caml_ba_msync(value v)
{
  struct caml_ba_array * b = Caml_ba_array_val(v);
  CAMLassert((b->flags & CAML_BA_MANAGED_MASK) == CAML_BA_MAPPED_FILE);
  caml_msync(b->data, caml_ba_byte_size(b));
}
