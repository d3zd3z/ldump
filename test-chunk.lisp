;;; Test the chunk code.

(in-package #:ldump.test)

(deftestsuite test-chunk (tmpdir-mixin hash-compare-mixin)
  ())
(pushnew 'test-chunk *test-suites*)

(addtest simple
  ;; Verity that hash and such are computed properly.
  (let* ((c1 (make-test-chunk 10 1 :zyzy))
	 (c1-data (chunk-data c1))
	 (hash1 (hashlib:sha1-objects "ZYZY" c1-data))
	 (c2 (make-test-chunk 500 2))
	 (c2-data (chunk-data c2))
	 (hash2 (hashlib:sha1-objects "BLOB" c2-data)))
    (ensure-same hash1 (chunk-hash c1))
    (ensure-same hash2 (chunk-hash c2))))

(deftestsuite test-chunkfile (test-chunk tmpdir-mixin)
  (tmpfile)
  (:setup
   (setf tmpfile (format nil "~A/chunk-1" tmpdir))))

(defun make-test-sizes ()
  (let ((sizes (make-hash-table)))
    (iter (for power from 0 to 18)
	  (iter (for bump from -1 to 1)
		(setf (gethash (+ (expt 2 power) bump) sizes) t)))
    (iter (for (key value) in-hashtable sizes)
	  (declare (ignorable value))
	  (collect key into result)
	  (finally (return (sort result #'<))))))

(addtest chunk-file-test
  (let* ((cfile (open-chunk-file tmpfile))
	 (psizes (iter (for i from 1)
		       (for size in (make-test-sizes))
		       (for chunk = (make-test-chunk size i))
		       (collect (list (write-chunk cfile chunk)
				      (chunk-hash chunk))))))
    ;; Just read them back in, with verification.
    (iter (for (offset hash) in psizes)
	  (for chunk = (read-chunk cfile offset :verify-hash t))
	  #+(or) (ensure-same hash (chunk-hash chunk)))))
