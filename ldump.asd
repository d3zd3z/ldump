(defsystem #:ldump
  :description "Ldump stystem"
  :depends-on (:cffi :iterate :babel :alexandria
		     :db-zlib
		     :cl-base64)
  :serial t
  :components ((:file "packages")
	       (:file "macros")
	       (:file "pdump")
	       (:file "hashlib")
	       (:file "pack")
	       (:file "chunk")))
