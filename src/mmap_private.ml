(** Additional support for mmap; we want to have: 1) growable files;
   2) ability to track the max used part of the file *)

(* open Util *)
open Mmap_intf

module Bigarray = Stdlib.Bigarray 

open Bigarray

type buffer = bigstring

type t = { 
  fd          : Unix.file_descr; 
  mutable buf : buffer; 
}

open Unix

(** What we need to use mmap; for large file sizes, we need to be on a
   64 bit machine *)
module type S = sig 
  val int_size_is_geq_63: bool
  val mmap_increment_size: int 
end

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
    (* NOTE we don't increase the size at this point; that can happen
       later *)
    let buf = map fd sz in
    { fd; buf }

  let length t = Array1.dim t.buf 

  let rec sub t ~off ~len = 
    let buf_len = Array1.dim t.buf in
    match off+len <= buf_len with
    | true -> Array1.sub t.buf off len
    | false -> 
      remap (buf_len + mmap_increment_size) t;
      sub t ~off ~len

  let msync t = Msync.msync (genarray_of_array1 t.buf)
      
  let fstat t = Unix.fstat t.fd

  let close t = 
    msync t;
    t.buf <- Bigstringaf.create 0;
    Gc.full_major ();
    Unix.close t.fd
end

