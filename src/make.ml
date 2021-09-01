(** Provide two impls: one with fd, one with mmap *)

(* open Btree_impl_intf *)

(* module Make = Make_with_mmap_private.Make *)
(* module Make_with_fd = Make_with_fd_private.Make *)

module Make_with_mmap = Make_with_mmap_private.Make

