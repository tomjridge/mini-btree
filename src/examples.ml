open Btree_impl_intf

module Example_int_int_mmap : Make_intf.T with type k=int and type v=int = struct

  module S = struct
    open Bin_prot.Std
    type k = int[@@deriving bin_io]
    type v = int[@@deriving bin_io]
    type r = int[@@deriving bin_io]
    let k_cmp = {k_cmp=Stdlib.Int.compare}
    let blk_sz = blk_sz_4096
    let constants = make_constants ~blk_sz ~k_sz:10 ~v_sz:10
  end

  module Made = Make_with_mmap_private.Make(S)

  include Made

end

module Example_ii = Example_int_int_mmap


module Example_string_string_mmap(S0:sig val max_k_size:int val max_v_size:int end) 
  : Make_intf.T with type k=string and type v=string 
= struct
  open S0

  module S = struct
    open Bin_prot.Std
    type k = string[@@deriving bin_io]
    type v = string[@@deriving bin_io]
    type r = int[@@deriving bin_io]
    let k_cmp = {k_cmp=Stdlib.String.compare}
    let blk_sz = blk_sz_4096

    (* following ensures that string encoding takes 3+string length at most *)
    let _ = assert(max_k_size < 65536 && max_v_size < 65536)
      
    (* need to fit at least two one r k r in a node and at least one
       kv in a leaf FIXME put this check in make_constants function *)
    let _ = assert(max_k_size + max_v_size + 6 <= 4096) 

    let constants = make_constants ~blk_sz ~k_sz:(3+max_k_size) ~v_sz:(3+max_v_size)
  end

  module Made = Make_with_mmap_private.Make(S)

  include Made

end


module Example_int_string_mmap(S0:sig val max_v_size:int end) 
  : Make_intf.T with type k=int and type v=string 
= struct
  open S0

  module S = struct
    open Bin_prot.Std
    type k = int[@@deriving bin_io]
    type v = string[@@deriving bin_io]
    type r = int[@@deriving bin_io]
    let k_cmp = {k_cmp=Stdlib.Int.compare}
    let blk_sz = blk_sz_4096

    let max_k_size = 9

    (* following ensures that string encoding takes 3+string length at most *)
    let _ = assert(max_v_size < 65536)
      
    (* need to fit at least two one r k r in a node and at least one
       kv in a leaf FIXME put this check in make_constants function *)
    let _ = assert(max_k_size + max_v_size + 6 <= 4096) 

    let constants = make_constants ~blk_sz ~k_sz:(max_k_size) ~v_sz:(3+max_v_size)
  end

  module Made = Make_with_mmap_private.Make(S)

  include Made

end
