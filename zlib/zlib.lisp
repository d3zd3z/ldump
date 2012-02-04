;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Zlib CFFI binding.

(in-package #:db-zlib)

(define-foreign-library zlib
    (:unix (:or "libz.so.1.2.3" "libz.so.1" "libz.so"))
    (t (:default "libz")))
(use-foreign-library zlib)

(defcfun ("compressBound" %compress-bound) :ulong (src :ulong))
(defun compress-bound (length)
  (%compress-bound length))

(defcfun ("compress2" %compress2) :int
  (dest :pointer)
  (dest-len :pointer)
  (source :pointer)
  (source-len :long)
  (level :int))
(defcfun ("uncompress" %uncompress) :int
  (dest :pointer)
  (dest-len :pointer)
  (source :pointer)
  (source-len :long))

(defcfun ("zError" %zerror) :string (val :int))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defconstant Z_OK 0)
  (defconstant Z_BUF_ERROR -5))

(defun bound-check (vector start end)
  "Perform a basic bounds check that START/END describe a valid region
of VECTOR."
  (assert (>= start 0))
  (assert (>= end start))
  (assert (<= end (length vector))))

(defun compress (dest src &key
		 (dest-start 0) dest-end
		 (src-start 0) src-end
		 (level -1)
		 (overflow-error-p t)
		 (overflow-error-value nil))
  "Compress a single block of data as a unit.  Both DEST and SRC
should be allocated with cffi::make-shareable-byte-vector.  There must
be sufficient data within DEST-START to DEST-END to hold the
compressed results of the data in the source buffer.  A suitable
length can be determined with COMPRESS-BOUND.  Returns the first
position in the DEST that was not written to.

If OVERFLOW-ERROR-P is true, then an error will be signalled if there
is not room in the destination buffer.  Otherwise, the value of
OVERFLOW-ERROR-VALUE will be returned instead of the compression
offset."
  (let* ((dest-end (or dest-end (length dest)))
	 (src-end (or src-end (length src))))
    (bound-check src src-start src-end)
    (bound-check dest dest-start dest-end)

    (with-foreign-object (dest-len* :ulong)
      (with-pointer-to-vector-data (src* src)
	(with-pointer-to-vector-data (dest* dest)
	  (setf (mem-aref dest-len* :ulong) (- dest-end dest-start))
	  (let ((result (%compress2 (inc-pointer dest* dest-start)
				    dest-len*
				    (inc-pointer src* src-start)
				    (- src-end src-start)
				    level)))
	    (case result
	      (#.Z_OK
		 (mem-ref dest-len* :ulong))
	      (#.Z_BUF_ERROR
		 (if overflow-error-p
		     (error "Compression error: ~A (~A)" result
			    (%zerror result))
		     overflow-error-value)))))))))

(defun uncompress (dest src &key
		   (dest-start 0) dest-end
		   (src-start 0) src-end)
  "Uncompress a single block of data.  DEST must have enough room to
hold the entire decompression.  Returns the first position in DEST not
written to."
  (let* ((dest-end (or dest-end (length dest)))
	 (src-end (or src-end (length src))))
    (bound-check src src-start src-end)
    (bound-check dest dest-start dest-end)

    (with-foreign-object (dest-len* :ulong)
      (with-pointer-to-vector-data (src* src)
	(with-pointer-to-vector-data (dest* dest)
	  (setf (mem-aref dest-len* :ulong) (- dest-end dest-start))
	  (let ((result (%uncompress (inc-pointer dest* dest-start)
				     dest-len*
				     (inc-pointer src* src-start)
				     (- src-end src-start))))
	    (unless (zerop result)
	      (error "Decompression error: ~A (~A)" result
		     (%zerror result)))
	    (mem-ref dest-len* :ulong)))))))
