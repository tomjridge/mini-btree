(** (Using mmap) An example instance using bin_prot for marshalling, and a backing
    file+mmap to store the nodes of the B-tree *)

(* FIXME it might still be worth caching to avoid repeated marshalling
   of the leafs and nodes that are subsequently modified and
   remarshalled *)
(* open Util *)
open Btree_impl_intf


open (struct module Buf = Bigstringaf end)

type buf = Bigstringaf.t
(** NOTE unlike using Lwt.pread,pwrite, we can use mmap's bigarray
    which fits in better with bin_prot. *)



(** Generic marshalling, arbitrary k,v,r; direct to mmap *)
module Marshalling_with_bin_prot = struct

  module type S = Make_intf.FOR_BIN_PROT

  module Make(S:S) = struct
    open Bin_prot.Std
    open S

    (* What gets stored on disk *)
    type node = Branch of k list * r list | Leaf of (k*v) list [@@deriving bin_io]

    [@@@warning "-26-27-32"]

    let make_read_write
        ~(leaf_ops:_ leaf_ops) 
        ~(branch_ops:_ branch_ops) 
        ~(node_ops:_ node_ops) 
        ~blk_size
      =
      let read (mmap:_ Mmap.t) r = 
        let buf = Mmap.sub mmap ~off:(r*blk_size) ~len:blk_size in
        bin_read_node buf ~pos_ref:(ref 0) |> fun node -> 
        let node = 
          match node with
          | Branch(ks,rs) -> branch_ops.of_krs (ks,rs) |> node_ops.of_branch
          | Leaf(kvs) -> leaf_ops.of_kvs kvs |> node_ops.of_leaf
        in
        node
      in
      let write (mmap:_ Mmap.t) r node = 
        let buf = Mmap.sub mmap ~off:(r*blk_size) ~len:blk_size in
        node |> node_ops.cases 
          ~leaf:(fun lf -> 
              leaf_ops.to_kvs lf |> fun kvs -> 
              Leaf kvs |> bin_write_node buf ~pos:0 |> fun _n ->               
              (* NOTE n is the next position, not the length *)
              ())
          ~branch:(fun x -> 
              branch_ops.to_krs x |> fun (ks,rs) -> 
              Branch (ks,rs) |> bin_write_node buf ~pos:0 |> fun _n -> 
              ())
      in
      (read,write)

  end
end


(** Simple example, where we store the B-tree blocks in a file+mmap;
    we don't cache explicitly, but allow the mmap to optimize *)
module Btree_on_mmap = struct

  module type S = Make_intf.S

  module Make1(S:S) = struct
    open S

    module M = Marshalling_with_bin_prot.Make(S)

    module B = Btree_impl.Make(S)
    open B

    let make = B.make

    open Bin_prot.Std

    type header = {
      mutable root: r;
      mutable min_free_blk : int;
    } [@@deriving bin_io]   

    module R = Base.Int


    (* state type *)
    type t = {
      fn             : string; (* filename *)
      fd             : Unix.file_descr;
      mmap           : Mmap.char_mmap;
      header         : header;
      store_ops      : (r,node) store_ops;
      btree_ops      : (k,v,r) btree_ops;
      mutable closed : bool;
    }

    (* Header block, always written to blk 0 *)
    module H = struct
      let write mmap h = 
        let buf = Mmap.sub mmap ~off:0 ~len:blk_sz in
        ignore(bin_write_header buf ~pos:0 h);
        ()

      let read mmap = 
        let buf = Mmap.sub mmap ~off:0 ~len:blk_sz in
        bin_read_header buf ~pos_ref:(ref 0)
    end

    module From_fd(S:sig
        val fd: Unix.file_descr
      end) = struct
      open S
      let mmap = Mmap.(of_fd fd char_kind)

      let uncached_store_ops : _ store_ops =         
        let read,write = 
          M.make_read_write ~leaf_ops:leaf ~branch_ops:branch ~node_ops:node
            ~blk_size:blk_sz 
        in
        let write = write mmap in
        let read = read mmap in
        let flush () = Mmap.msync mmap in
        { read;write;flush }

      (** Add a store cache, which improves performance dramatically
         for single inserts because it avoids repeated
         marshalling/demarshalling *)
      let store_ops = Make_util.add_cache ~uncached_store_ops ~max_sz:500_000
      (* FIXME this cache is ridiculously large *)
    end

    module From_store(S:sig
        val store_ops: (r,node) store_ops
        val header : header
      end) = struct

      open S

      let btree_ops : _ btree_ops = 
        B.make ~store:store_ops
          ~alloc:(fun () -> 
              let x = header.min_free_blk in
              header.min_free_blk <- x+1;
              x)    
    end


    (* To create: initialize file; write header block to blk 0, write
       empty leaf to blk 1 *)
    let create ~fn = 
      Unix.(openfile fn [O_CREAT;O_RDWR; O_TRUNC] 0o640) |> fun fd -> 
      let open From_fd(struct let fd = fd end) in
      let header = {
        root=1;
        min_free_blk=2 
      }
      in
      H.write mmap header |> fun () -> 
      let open From_store(struct 
          let store_ops=store_ops
          let header=header 
        end) 
      in
      store_ops.write 1 (node.of_leaf (leaf.of_kvs [])) |> fun () -> 
      { fn;fd;mmap;header;store_ops;btree_ops;closed=false }

    (* To open, read header *)
    let open_ ~fn = 
      Unix.(openfile fn [O_RDWR] 0o640) |> fun fd -> 
      let open From_fd(struct let fd = fd end) in
      H.read mmap |> fun header -> 
      let open From_store(struct 
          let store_ops=store_ops
          let header=header 
        end) 
      in
      { fn;fd;mmap;header;store_ops;btree_ops;closed=false }

    let find t k = 
      assert(not t.closed);
      t.btree_ops.find ~r:t.header.root ~k

    let insert t k v = 
      assert(not t.closed);
      t.btree_ops.insert ~k ~v ~r:t.header.root |> fun (_free,new_root_o) -> 
      match new_root_o with
      | Some {new_root} -> (t.header.root <- new_root; ())
      | None -> ()
    (* NOTE we don't use free at this point, but in reality we should
       probably the blocks to the freelist *)

    let insert_many t kvs = 
      assert(not t.closed);
      t.btree_ops.insert_many ~kvs ~r:t.header.root |> function
      | Rebuilt(_free,new_root_o,kvs) -> (
          (* NOTE again we don't use free at this point *)
          match new_root_o with
          | Some {new_root} -> (t.header.root <- new_root; kvs)
          | None -> kvs)
      | Remaining kvs -> kvs
      | Unchanged_no_kvs -> []

    let delete t k = 
      assert(not t.closed);
      t.btree_ops.delete ~k ~r:t.header.root

    (* To close, write header *)
    let close t = 
      t.store_ops.flush () |> fun () -> 
      H.write t.mmap t.header |> fun () -> 
      Mmap.close t.mmap; (* closes the underlying fd *)
      t.closed <- true; ()
  end

  (** Package up, omit internal defns *)
  module Make_2(S:S) : sig
    type k=S.k
    type v=S.v
    type t
    val create      : fn:string -> t
    val open_       : fn:string -> t
    val find        : t -> k -> v option
    val insert      : t -> k -> v -> unit
    val insert_many : t -> (k * v) list -> (k * v) list
    val delete      : t -> k -> unit
    val close       : t -> unit

  end
  = struct
    type k=S.k
    type v=S.v
    (* type r=S.r *)
    open Make1(S)
    type nonrec t = t
    let create,open_,find,insert,insert_many,delete,close =
      create,open_,find,insert,insert_many,delete,close
  end

end

module Make : Make_intf.MAKE = Btree_on_mmap.Make_2
