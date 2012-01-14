(defsystem #:ldump-test
  :description "Test suite for ldump"
  :depends-on (:ldump :iterate :lift)
  :serial t
  :components ((:file "test")
	       (:file "test-chunk")))
