module Util = Util

module M = Btree_impl_intf.M

module Btree_impl_intf = Btree_impl_intf

module Make_intf = Make_intf

(* module Make_with_fd = Make.Make_with_fd *)

module Make_with_mmap = Make.Make_with_mmap

module Examples = Examples


module Private = struct
  (** Most of the actual modules *)

  module Btree_impl = Btree_impl

  module Make = Make 

  (* With fd *)
  module Make_with_fd_private = Make_with_fd_private

  (* With mmap *)
  module Make_with_mmap_private = Make_with_mmap_private

  (* Various mmap modules which we do not expose here *)

  module Simple_cache = Simple_cache

end
