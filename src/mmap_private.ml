(** Additional support for mmap; we want to have: 1) growable files;
   2) ability to track the max used part of the file *)

(* open! Import *)

module Bigarray = Stdlib.Bigarray 

open Bigarray

type buffer = (char, int8_unsigned_elt, c_layout) Array1.t 

type t = { 
  fd                      : Unix.file_descr; 
  mutable buf             : buffer; 
  mutable min_not_written : int (* min not written beyond file's initial size *)
}

open Unix

(** What we need to use mmap; for large file sizes, we need to be on
   64 bit machine *)
module type S = sig 
  val int_size_is_geq_63: bool
  val mmap_increment_size: int 
end

(** Version with src_buf=string, dst_buf=bytes *)
module Make_1(S:S) = struct
  open S
  let _ = assert(int_size_is_geq_63 && Sys.int_size >= 63)

  type nonrec t = t

  let map fd sz = 
    let shared = true in
    let buf = 
      Unix.map_file fd Char c_layout shared [| sz |] 
      |> array1_of_genarray
    in
    buf

  let remap sz t = 
    Printf.printf "Resizing %d\n" sz;
    (* release old buffer *)
    t.buf <- Bigstringaf.create 0;
    (* force collection of old buffer; perhaps try to detect
       finalization *)
    Gc.full_major ();
    (* create new map *)
    let buf = map t.fd sz in
    t.buf <- buf 
    
  (* NOTE public functions from here *)
  
  (* FIXME shared is always true? if we are unmapping and remapping,
     we presumably want the new data to be changed *)
  let of_fd fd = 
    let sz = (Unix.fstat fd).st_size in
    let buf = map fd (sz+mmap_increment_size) in
    { fd; buf; min_not_written=sz }

  let unsafe_write t ~(src:string) ~src_off ~dst_off ~len =
    begin 
      match dst_off+len > Array1.dim t.buf with
      | true -> remap (dst_off+len+mmap_increment_size) t
      | false -> ()
    end;    
    Bigstringaf.blit_from_string src ~src_off t.buf ~dst_off ~len;
    t.min_not_written <- max t.min_not_written (dst_off+len);
    ()

  let unsafe_read t ~src_off ~len ~(buf:bytes) = 
    assert(Bytes.length buf >= len);
    begin 
      match src_off+len > Array1.dim t.buf with
      | true -> remap (src_off+len+mmap_increment_size) t
      | false -> ()
    end;    
    Bigstringaf.blit_to_bytes t.buf ~src_off buf ~dst_off:0 ~len;
    ()    

  (* FIXME need to use msync, not fsync *)
  let fsync t = Unix.fsync t.fd
      
  let fstat t = Unix.fstat t.fd

  (* FIXME need to use munmap *)
  let close t = 
    fsync t;
    (* reduce the size of the file to avoid large numbers of trailing
       0 bytes *)
    Unix.ftruncate t.fd t.min_not_written;
    Unix.close t.fd
end

module Make_2(S:S) : Mmap_intf.S_STRING = Make_1(S)

(** Version with src_buf=dst_buf=bigstring *)  
module Make_3(S:S) = struct

  module Buf = Bigstringaf 
  type buf = Buf.t

  open S
  let _ = assert(int_size_is_geq_63 && Sys.int_size >= 63)

  type nonrec t = t

  module M = Make_1(S)

  let map,remap,of_fd = M.(map,remap,of_fd)

  let unsafe_write t ~(src:buf) ~src_off ~dst_off ~len =
    begin 
      match dst_off+len > Array1.dim t.buf with
      | true -> remap (dst_off+len+mmap_increment_size) t
      | false -> ()
    end;    
    Bigstringaf.blit src ~src_off t.buf ~dst_off ~len;
    t.min_not_written <- max t.min_not_written (dst_off+len);
    ()

  let unsafe_read t ~src_off ~len ~(buf:buf) = 
    assert(Buf.length buf >= len);
    begin 
      match src_off+len > Array1.dim t.buf with
      | true -> remap (src_off+len+mmap_increment_size) t
      | false -> ()
    end;    
    Bigstringaf.blit t.buf ~src_off buf ~dst_off:0 ~len;
    ()    

  let fsync,fstat,close = M.(fsync,fstat,close)

end

module Make_4(S:S) : Mmap_intf.S_BIGSTRING = Make_3(S)
