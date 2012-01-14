(defsystem #:ldump
  :description "Ldump stystem"
  :depends-on (:cffi :iterate :babel :alexandria
		     :db-zlib
		     :cl-base64)
  :serial t
  :components ((:file "packages")
	       (:file "macros")
	       (:file "pdump")
	       (:file "pack")
	       (:file "hashlib")
	       (:file "chunk")
	       (:file "file-index")))
