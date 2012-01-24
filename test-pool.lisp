;;; Testing pools.

(in-package #:ldump.test)

(deftestsuite test-pool (tmpdir-mixin)
  ())
(pushnew 'test-pool *test-suites*)

(addtest must-directory
  (ensure-error
    (create-pool (merge-pathnames (make-pathname :directory '(:relative "tmp"))
				  tmpdir))))

(addtest must-be-empty
  (ensure-error
    (with-open-file (stream (merge-pathnames (make-pathname :name "tmp")
					     tmpdir))
      (write-string "hello" stream))
    (create-pool tmpdir)))

(addtest validate-props
  (create-pool tmpdir :limit (* 12 1024 1024) :newfile t)
  (with-pool (pool tmpdir)
    (ensure-same (slot-value pool 'ldump.file-pool::limit)
		 (* 12 1024 1024))
    (ensure (slot-value pool 'ldump.file-pool::newfile))
    (ensure (slot-boundp pool 'ldump.file-pool::uuid))))
