;;; Test the index code.

(in-package #:ldump.test)

(deftestsuite test-file-index (tmpdir-mixin hash-compare-mixin)
  ())
(pushnew 'test-file-index *test-suites*)

(defun invent-kind (num)
  (let* ((types '#(:blob :|DIR | :dir0 :dir1 :ind0 :ind1 :ind2 :back))
	 (index (mod num (length types))))
    (elt types index)))

(defun hash-of-int (num)
  (hashlib:sha1-objects (princ-to-string num)))

(defun add-sample (index num)
  "Add a made up entry for the given number."
  (index-add index
	     (hash-of-int num)
	     (invent-kind num)
	     num))

(defun mangle-hash (hash delta)
  "Modify a hash by adding DELTA to it, to the last byte."
  (let ((new-hash (copy-seq hash)))
    (iter (for pos from (1- (length hash)) downto 0)
	  (for element = (aref new-hash pos))
	  (for new-element = (+ element delta))
	  (for new-element-byte = (logand new-element 255))
	  (setf (aref new-hash pos) new-element-byte)
	  (until (= new-element new-element-byte)))
    new-hash))

;;; Check that number is present.
(defun check-sample (index num)
  "Make sure that num is in the index, and that very close hashes are
not."
  (let ((hash (hash-of-int num)))
    (flet ((try (delta expected)
	     (let ((result (index-lookup index (mangle-hash hash delta))))
	       (unless (equalp result expected)
		 (error "Unexpected index result: ~S, expecting ~S"
			result expected)))))
      (try 0 (make-index-node :type (invent-kind num) :offset num))
      (try 1 nil)
      (try -1 nil)
      t)))

(defun naive-test (path)
  (let ((index (make-instance 'ram-index))
	(index-name (merge-pathnames #p"blort.idx" path)))
    (iter (for i from 1 to 50000)
	  (add-sample index i))
    (iter (for i from 1 to 50000)
	  (check-sample index i))
    (safe-write-index index-name index #x12345)
    (let ((findex (read-index index-name #x12345)))
      (iter (for i from 1 to 50000)
	    (check-sample findex i)))))

(addtest naive
  (naive-test tmpdir))
