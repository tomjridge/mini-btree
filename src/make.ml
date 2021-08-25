open Btree_impl_intf
open Make_private

module Make = Make

module Example_int_int : Make_intf.T with type k=int and type v=int = struct

  module S = struct
    open Bin_prot.Std
    type k = int[@@deriving bin_io]
    type v = int[@@deriving bin_io]
    type r = int[@@deriving bin_io]
    let k_cmp = {k_cmp=Stdlib.Int.compare}
    let blk_sz = blk_sz_4096
    let constants = make_constants ~blk_sz ~k_sz:10 ~v_sz:10
  end

  module Made = Make(S)

  include Made

end
