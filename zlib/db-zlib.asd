;;;; -*- Mode: lisp -*-
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defsystem db-zlib
  :description "Simple zlib binding"
  :depends-on (:cffi)
  :serial t
  :components ((:file "packages")
	       (:file "zlib")))
