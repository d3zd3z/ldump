(defsystem #:ldump
  :description "Ldump stystem"
  :depends-on (:cffi :iterate :babel :alexandria :xmls :local-time
		     :command-line-arguments

		     :db-zlib
		     :cl-base64 :cl-fad)
  :serial t
  :components ((:file "packages")
	       (:file "macros")
	       (:file "pdump")
	       (:file "pack")
	       (:file "hashlib")
	       (:file "chunk")
	       (:file "file-index")
	       (:file "file-pool")
	       (:file "nodes")
	       (:file "main")))
