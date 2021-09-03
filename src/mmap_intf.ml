
type bigstring =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(** Sig with abstract buf type *)
module type S = sig
  type t
  type src_buf
  type dst_buf
  val of_fd : Unix.file_descr -> t
  val unsafe_write :
    t ->
    src:src_buf ->
    src_off:int -> dst_off:int -> len:int -> unit
  val unsafe_read :
    t -> src_off:int -> len:int -> buf:dst_buf -> unit
  val sync : t -> unit
  val fstat : t -> Unix.stats
  val close : t -> unit

  (** NOTE the raw_mmap is unsafe, and you need to check that there is
     enough space to write *)
  val raw_mmap : t -> bigstring

  (** If using raw_mmap, we need to take care to update the max written afterwards *)
  val raw_mmap_update_offset : t -> int -> unit
end

module type S_STRING = S with type src_buf:=string and type dst_buf:=bytes

module type S_BIGSTRING = S with type src_buf:=bigstring and type dst_buf:=bigstring

