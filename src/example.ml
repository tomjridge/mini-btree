(** An example instance using bin_prot for marshalling, and a backing
   file to store the nodes of the B-tree *)

open Util
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

module Marshalling_int_int = Marshalling_with_bin_prot(struct
    open Bin_prot.Std
    type k = int[@@deriving bin_io]
    type v = int[@@deriving bin_io]
    type r = int[@@deriving bin_io]
  end)

(** Simple example, where we store the B-tree blocks in a file *)
module Private_btree_on_file = struct

  module M = Marshalling_int_int

  open struct
    module B = Btree_impl
    module E = Btree_impl.Example_int_int
  end

  let blk_sz = E.blk_sz
                 
  let make = E.make

  open Bin_prot.Std

  type header = {
    mutable root: E.r;
    mutable min_free_blk : int;
  } [@@deriving bin_io]   

  type cache_entry = {
    node          : E.node;
    mutable dirty : bool
  }

  type cache = (E.r,cache_entry)Base.Hashtbl.t

  let empty_cache () = Base.Hashtbl.create (module Base.Int)

  (* state type *)
  type t = {
    fn             : string; (* filename *)
    fd             : Lwt_unix.file_descr;
    header         : header;
    uncached_store : (E.r,E.node) store_ops;
    store_ops      : (E.r,E.node) store_ops;
    btree_ops      : (E.k,E.v,E.r) btree_ops;
    mutable closed : bool;
    cache          : cache
  }

  let write_blk_bytes fd n buf = 
    assert(Bytes.length buf = blk_sz);
    let file_offset = blk_sz * n in
    Lwt_unix.pwrite fd buf ~file_offset 0 (* buf off *) blk_sz (* len *) >>= fun x -> 
    assert(blk_sz=x); (* FIXME maybe it isn't... so write again *)
    return ()

  let write_blk_ba fd n ba =
    write_blk_bytes fd n (ba_to_bytes ba)

  let read_blk_bytes fd n =
    let file_offset = blk_sz * n in
    let buf = Bytes.create blk_sz in
    Lwt_unix.pread fd buf ~file_offset 0 (* buf off *) blk_sz (* len *) >>= fun x -> 
    assert(blk_sz=x);
    return buf

  let read_blk_ba fd n = 
    read_blk_bytes fd n >>= fun buf -> 
    return (bytes_to_ba buf)
    
    
  (* Header block, always written to blk 0 *)
  module H = struct
    let write fd h = 
      let ba = Bigstringaf.create blk_sz in (* ASSUMES header fits in blk *)
      ignore(bin_write_header ba ~pos:0 h);
      write_blk_ba fd 0 ba 

    let read fd = 
      read_blk_ba fd 0 >>= fun ba -> 
      bin_read_header ba ~pos_ref:(ref 0) |> return
  end
  
  module From_fd(S:sig
      val fd: Lwt_unix.file_descr
    end) = struct
    open S
    open E
    let blk_dev_ops = {read=read_blk_bytes fd;write=write_blk_bytes fd}
    let uncached_store = 
      M.blk_dev_to_store ~blk_dev_ops ~leaf_ops:leaf ~branch_ops:branch ~node_ops:node
        ~blk_size:blk_sz 
  end

  (** This is too simple: at the moment it caches every read and
     write, and only goes to disk on a flush. OK for a demo but should
     be replaced with an LRU or similar. *)
  module Cached_store = struct

    module Hashtbl = Base.Hashtbl
    
    let cache_store cache (store_ops:(E.r,E.node) store_ops) : _ store_ops = {
      read=(fun r -> 
          Hashtbl.find cache r |> function
          | None -> 
            store_ops.read r >>= fun node -> 
            Hashtbl.set cache ~key:r ~data:{node;dirty=false};
            return node
          | Some e -> 
            return e.node);
      write=(fun r node -> 
          (* NOTE if r this is already in the cache, then node should
             be part of the corresponding entry *)
          Base.Hashtbl.update cache r ~f:(function
              | None -> { node; dirty=true }
              | Some e -> 
                (* assert(e.node==node); *)
                { node=e.node; dirty=true });                
          return ());    
    }

    let flush_cache cache (uncached_store : _ store_ops) = 
      let dirties = ref [] in
      Hashtbl.iteri cache ~f:(fun ~key ~data -> 
          match data.dirty with 
          | true -> 
            data.dirty <- false;
            dirties:=(key,data.node)::!dirties
          | false -> ());
      (* now do the writes *)
      !dirties |> iter_k (fun ~k ds -> 
          match ds with
          | [] -> return ()
          | (r,n)::rest -> 
            uncached_store.write r n >>= fun () -> 
            k rest)
  end    

  module From_store(S:sig
      val uncached_store: (E.r,E.node) store_ops
      val cache : cache
      val header : header
    end) = struct

    open S

    let store_ops = Cached_store.cache_store cache uncached_store

    let btree_ops : _ btree_ops = 
      make ~store:store_ops
        ~alloc:(fun () -> 
            let x = header.min_free_blk in
            header.min_free_blk <- x+1;
            return x)    
  end
                      

  (* To create: initialize file; write header block to blk 0, write
     empty leaf to blk 1 *)
  let create ~fn = 
    let open E in
    Lwt_unix.(openfile fn [O_CREAT;O_RDWR; O_TRUNC] 0o640) >>= fun fd -> 
    let header = {
      root=1;
      min_free_blk=2 
    }
    in
    H.write fd header >>= fun () -> 
    let open From_fd(struct let fd = fd end) in
    let cache = empty_cache () in
    let open From_store(struct 
        let uncached_store=uncached_store
        let header=header 
        let cache=cache end) 
    in
    store_ops.write 1 (node.of_leaf (leaf.of_kvs [])) >>= fun () -> 
    return { fn;fd;header;uncached_store;store_ops;btree_ops;closed=false;cache }

  (* To open, read header *)
  let open_ ~fn = 
    Lwt_unix.(openfile fn [O_RDWR] 0o640) >>= fun fd -> 
    H.read fd >>= fun header -> 
    let open From_fd(struct let fd = fd end) in    
    let cache = empty_cache () in
    let open From_store(struct 
        let uncached_store=uncached_store
        let header=header 
        let cache=cache end) 
    in
    return { fn;fd;header;uncached_store;store_ops;btree_ops;closed=false;cache }

  let find t k = 
    assert(not t.closed);
    t.btree_ops.find ~r:t.header.root ~k

  let insert t k v = 
    assert(not t.closed);
    t.btree_ops.insert ~k ~v ~r:t.header.root >>= function
      | `New_root (_to_free,r) -> (t.header.root <- r; return ())
      | `Ok _to_free -> return ()

  let insert_many t kvs = 
    assert(not t.closed);
    t.btree_ops.insert_many ~kvs ~r:t.header.root >>= function
    | `Rebuilt(`New_root(_,r),`Remaining kvs) -> (t.header.root <- r; return kvs)
    | `Rebuilt(`Ok _,`Remaining kvs) -> return kvs
    | `Remaining kvs -> return kvs
    | `Unchanged -> return []

  let delete t k = 
    assert(not t.closed);
    t.btree_ops.delete ~k ~r:t.header.root

  (* To close, write header *)
  let close t = 
    Cached_store.flush_cache t.cache t.uncached_store >>= fun () ->
    H.write t.fd t.header >>= fun () -> 
    Lwt_unix.close t.fd >>= fun () -> 
    t.closed <- true; return ()

end

module Btree_on_file : sig
  type t
  val create      : fn:string -> t m
  val open_       : fn:string -> t m
  val find        : t -> int -> int option m
  val insert      : t -> int -> int -> unit m
  val insert_many : t -> (int * int) list -> (int * int) list m
  val delete      : t -> int -> unit m
  val close       : t -> unit m
end = struct 
  include Private_btree_on_file
end
