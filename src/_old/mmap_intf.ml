type bigstring =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(** To write to/from the mmap, use the sub function to get the
   subarray of interest. NOTE that because the map may grow when
   asking for a sub which extends beyond the current length, a remap
   may occur. Thus, subs should not be reused or stored, since they
   may refer to an out of date slice for an old mapping. *)
module type S = sig
  type t
  val of_fd  : Unix.file_descr -> t

  (** Length of the mmap; should match the fstat size *)
  val length : t -> int

  (** Subarray from offset of the given length *)
  val sub    : t -> off:int -> len:int -> bigstring

  (** Perform an msync to flush data to disk *)
  val msync  : t -> unit
  val fstat  : t -> Unix.stats
  val close  : t -> unit
end


