(** An example instance using bin_prot for marshalling, and a backing file *)

open Btree_intf

type buf = bytes 
(** NOTE this is what lwt prefers, but bin_prot prefers bigarray :(
   Bin_prot.Common has some useful blitting functions... also in
   Bigstringaf *)

let ba_to_bytes ba = 
  Bigstringaf.length ba |> fun len -> 
  Bytes.create len |> fun buf -> 
  Bigstringaf.blit_to_bytes ba ~src_off:0 buf ~dst_off:0 ~len;
  buf

let bytes_to_ba buf = 
  Bytes.length buf |> fun len -> 
  Bigstringaf.create len |> fun ba -> 
  Bigstringaf.blit_from_bytes buf ~src_off:0 ba ~dst_off:0 ~len;
  ba


(** Provided by the underlying block device, via lwt pread/pwrite on a
   file for example *)
type 'r blk_dev_ops = {
  read  : 'r -> buf m;
  write : 'r -> buf -> unit m
}

module Marshalling_with_bin_prot(
    S:sig
      type k[@@deriving bin_io]
      type v[@@deriving bin_io]
      type r[@@deriving bin_io]

    end) 
= struct
  open Bin_prot.Std
  open S

  (* What gets stored on disk *)
  type node = Branch of k list * r list | Leaf of (k*v) list [@@deriving bin_io]

  [@@@warning "-26-27-32"]

  let blk_dev_to_store
      ~(blk_dev_ops:r blk_dev_ops) 
      ~(leaf_ops:_ leaf_ops) 
      ~(branch_ops:_ branch_ops) 
      ~(node_ops:_ node_ops) 
      ~blk_size
    =
    let open (struct

      (** Read the relevant blk, unmarshal using bin_prot, and convert
          to leaf or branch *)
      let read r =
        blk_dev_ops.read r >>= fun buf -> 
        let ba = bytes_to_ba buf in
        bin_read_node ba ~pos_ref:(ref 0) |> fun node -> 
        let node = 
          match node with
          | Branch(ks,rs) -> branch_ops.of_krs (ks,rs) |> node_ops.of_branch
          | Leaf(kvs) -> leaf_ops.of_kvs kvs |> node_ops.of_leaf
        in
        return node

      let buf_size = blk_size

      (** Marshal the node and write to disk *)
      let write r node = 
        node |> node_ops.cases 
          ~leaf:(fun lf -> 
              leaf_ops.to_kvs lf |> fun kvs -> 
              let ba = Bigstringaf.create buf_size in
              Leaf kvs |> bin_write_node ba ~pos:0 |> fun _n -> 
              blk_dev_ops.write r (ba|>ba_to_bytes))
          ~branch:(fun x -> 
              branch_ops.to_krs x |> fun (ks,rs) -> 
              let ba = Bigstringaf.create buf_size in
              Branch (ks,rs) |> bin_write_node ba ~pos:0 |> fun _n -> 
              blk_dev_ops.write r (ba|>ba_to_bytes))

    end)
    in
    ({read;write} : _ store_ops)

  let _ = blk_dev_to_store

end

(** Simple example, where we store the B-tree blocks in a file *)
module Btree_on_file = struct

end


(*
module K = struct
  type t = int[@@deriving bin_io]
end

module V = struct
  type t = int[@@deriving bin_io]
end

module R = struct
  type t = int[@@deriving bin_io]
end
*)
