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

;;; Simple test, make sure we can write to the pool.
(addtest simple-writes
  (create-pool tmpdir)
  (let ((hashes (with-pool (pool tmpdir)
		  (iter (for size in (make-test-sizes))
			(for chunk = (make-test-chunk size 1))
			(collect (chunk-hash chunk))
			(write-pool-chunk chunk)))))
    (with-pool (pool tmpdir)
      (iter (for size in (make-test-sizes))
	    (for chunk = (make-test-chunk size 1))
	    (for read-chunk = (pool-get-chunk (chunk-hash chunk)))
	    (ensure (not (mismatch (chunk-data read-chunk)
				   (chunk-data chunk))))))))
