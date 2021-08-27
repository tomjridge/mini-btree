
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
  val fsync : t -> unit
  val fstat : t -> Unix.stats
  val close : t -> unit
end

module type S_STRING = S with type src_buf:=string and type dst_buf:=bytes

module type S_BIGSTRING = S with type src_buf:=bigstring and type dst_buf:=bigstring

