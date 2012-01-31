#-(or ccl sbcl)
(error "Currently, only ccl or sbcl are supported")

(defsystem #:ldump
  :description "Ldump stystem"
  :depends-on (:cffi :iterate :babel :alexandria :xmls :local-time
		     :command-line-arguments
		     :split-sequence
		     :uuid

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
	       (:file "pool")
	       (:file "file-pool")
	       (:file "nodes")
	       #+sbcl (:file "posix-sbcl")
	       #+ccl (:file "posix-ccl")
	       (:file "main")))
