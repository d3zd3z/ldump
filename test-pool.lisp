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

;;; For more complex cases, write chunks of a consistent size, with a
;;; changing seed.  During the test, keep track of what's been written
;;; here (we store the hash of the chunk to avoid having to recompute
;;; it).
(defvar *stored-chunks*)

(defparameter *test-chunk-size* (* 32 1024))

(defun add-chunks (lower upper)
  "Add chunks with an index from LOWER to UPPER (inclusive)."
  (iter (for index from lower to upper)
	(for chunk = (make-test-chunk *test-chunk-size* index))
	(for hash = (chunk-hash chunk))
	(write-pool-chunk chunk)
	(push hash *stored-chunks*)))

(defun check-chunks ()
  "Check that the chunks that were previously added are present."
  (iter (for hash in *stored-chunks*)
	(for chunk = (pool-get-chunk hash))
	(ensure chunk)
	(ensure (not (mismatch (chunk-hash chunk)
			       hash)))))

(addtest multiple-writes
  (let (*stored-chunks*)
    (create-pool tmpdir :limit (* 1024 1024))
    (with-pool (pool tmpdir)
      (add-chunks 1 200)
      (check-chunks)
      (add-chunks 201 400)
      (check-chunks))
    (with-pool (pool tmpdir)
      (check-chunks)
      (add-chunks 401 600)
      (check-chunks))
    (with-pool (pool tmpdir)
      (check-chunks))))
