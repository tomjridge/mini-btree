(** An example instance using bin_prot for marshalling, and a backing
   file+mmap to store the nodes of the B-tree *)

open Btree_impl_intf

(* module Intf = Make_intf *)
(* open Intf *)

open (struct module Buf = Bigstringaf end)

type buf = Bigstringaf.t
(** NOTE unlike using Lwt.pread,pwrite, we can use mmap's bigarray
   which fits in better with bin_prot. *)

(** Provided by the underlying block device, via lwt pread/pwrite on a
   file for example *)
type 'r blk_dev_ops = {
  read  : 'r -> buf m;
  write : 'r -> buf -> unit m
}


(** Generic marshalling, arbitrary k,v,r *)
module Marshalling_with_bin_prot = struct

  module type S = Make_intf.FOR_BIN_PROT
  
  module Make(S:S) = struct
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
          bin_read_node buf ~pos_ref:(ref 0) |> fun node -> 
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
                blk_dev_ops.write r ba)
            ~branch:(fun x -> 
                branch_ops.to_krs x |> fun (ks,rs) -> 
                let ba = Bigstringaf.create buf_size in
                Branch (ks,rs) |> bin_write_node ba ~pos:0 |> fun _n -> 
                blk_dev_ops.write r ba)

      end)
      in
      ({read;write} : _ store_ops)

    let _ = blk_dev_to_store

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

    module Mmap_ = Mmap.With_bigstring

    (* state type *)
    type t = {
      fn             : string; (* filename *)
      fd             : Unix.file_descr;
      mmap           : Mmap_.t;
      header         : header;
      store_ops      : (r,node) store_ops;
      btree_ops      : (k,v,r) btree_ops;
      mutable closed : bool;
    }

    (* FIXME why are we using lwt? *)

    let write_blk mmap n buf = 
      assert(Buf.length buf = blk_sz);
      let file_offset = blk_sz * n in
      Mmap_.unsafe_write mmap ~src:buf ~src_off:0 ~dst_off:file_offset ~len:blk_sz;
      return ()

    let read_blk mmap n =
      let file_offset = blk_sz * n in
      let buf = Buf.create blk_sz in
      Mmap_.unsafe_read mmap ~src_off:file_offset ~len:blk_sz ~buf;
      return buf

    (* Header block, always written to blk 0 *)
    module H = struct
      let write mmap h = 
        let ba = Bigstringaf.create blk_sz in (* ASSUMES header fits in blk *)
        ignore(bin_write_header ba ~pos:0 h);
        write_blk mmap 0 ba 

      let read mmap = 
        read_blk mmap 0 >>= fun ba -> 
        bin_read_header ba ~pos_ref:(ref 0) |> return
    end

    module From_fd(S:sig
        val fd: Unix.file_descr
      end) = struct
      open S
      let mmap = Mmap_.of_fd fd
      let blk_dev_ops = {read=read_blk mmap;write=write_blk mmap}

      (* NOTE uncached *)
      let store_ops = 
        M.blk_dev_to_store ~blk_dev_ops ~leaf_ops:leaf ~branch_ops:branch ~node_ops:node
          ~blk_size:blk_sz 
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
              return x)    
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
      H.write mmap header >>= fun () -> 
      let open From_store(struct 
          let store_ops=store_ops
          let header=header 
        end) 
      in
      store_ops.write 1 (node.of_leaf (leaf.of_kvs [])) >>= fun () -> 
      return { fn;fd;mmap;header;store_ops;btree_ops;closed=false }

    (* To open, read header *)
    let open_ ~fn = 
      Unix.(openfile fn [O_RDWR] 0o640) |> fun fd -> 
      let open From_fd(struct let fd = fd end) in
      H.read mmap >>= fun header -> 
      let open From_store(struct 
          let store_ops=store_ops
          let header=header 
        end) 
      in
      return { fn;fd;mmap;header;store_ops;btree_ops;closed=false }

    let find t k = 
      assert(not t.closed);
      t.btree_ops.find ~r:t.header.root ~k

    let insert t k v = 
      assert(not t.closed);
      t.btree_ops.insert ~k ~v ~r:t.header.root >>= fun (_free,new_root_o) -> 
      match new_root_o with
      | Some {new_root} -> (t.header.root <- new_root; return ())
      | None -> return ()
    (* NOTE we don't use free at this point, but in reality we should
       probably return the blocks to the freelist *)

    let insert_many t kvs = 
      assert(not t.closed);
      t.btree_ops.insert_many ~kvs ~r:t.header.root >>= function
      | Rebuilt(_free,new_root_o,kvs) -> (
          (* NOTE again we don't use free at this point *)
          match new_root_o with
          | Some {new_root} -> (t.header.root <- new_root; return kvs)
          | None -> return kvs)
      | Remaining kvs -> return kvs
      | Unchanged_no_kvs -> return []

    let delete t k = 
      assert(not t.closed);
      t.btree_ops.delete ~k ~r:t.header.root

    (* To close, write header *)
    let close t = 
      H.write t.mmap t.header >>= fun () -> 
      Mmap_.close t.mmap; (* closes the underlying fd *)
      t.closed <- true; return ()
  end

  (** Package up, omit internal defns *)
  module Make_2(S:S) : sig
    type k=S.k
    type v=S.v
    type t
    val create      : fn:string -> t m
    val open_       : fn:string -> t m
    val find        : t -> k -> v option m
    val insert      : t -> k -> v -> unit m
    val insert_many : t -> (k * v) list -> (k * v) list m
    val delete      : t -> k -> unit m
    val close       : t -> unit m

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
