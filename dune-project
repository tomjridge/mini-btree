(lang dune 2.8)
(name mini-btree)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/mini-btree))
(homepage             https://github.com/tomjridge/mini-btree)
(documentation        https://tomjridge.github.io/mini-btree/ocamldoc/index.html)
(generate_opam_files  true)


(package 
 (name mini-btree)
 (synopsis "A minimal implementation of a B-tree")
 (depends ocamlfind dune ppx_jane lwt lwt.unix bigstringaf)
)


