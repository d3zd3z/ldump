;;; Packed data.

(defpackage #:ldump.pack
  (:use #:cl #:iterate)
  (:export #:pack-le-integer #:unpack-le-integer
	   #:byte-vector #:make-byte-vector
	   #:read-new-sequence))
(in-package #:ldump.pack)

(defun pack-le-integer (dest offset num bytes)
  "Pack NUM in little endian format into the sequence DEST starting at
OFFSET using BYTES.  The number is silently truncated."
  (do ((offset offset (1+ offset))
       (bytes bytes (1- bytes))
       (num num (ash num -8)))
      ((zerop bytes))
    (setf (aref dest offset)
	  (ldb (byte 8 0) num))))

(defun unpack-le-integer (buffer offset bytes)
  "Unpack a BYTES-byte little endian number starting at OFFSET into
the BUFFER."
  (iter (for pos from (+ offset (1- bytes)) downto offset)
	(reducing (elt buffer pos)
		  by (lambda (num byte)
		       (logior (ash num 8) byte))
		  initial-value 0)))

(deftype byte-vector (&optional (length '*))
  `(vector (unsigned-byte 8) ,length))

(defun make-byte-vector (length &key (initial-element 0))
  (make-sequence 'byte-vector length :initial-element initial-element))

(defun read-new-sequence (stream count &key (on-failure-raise 'error))
  "Read COUNT bytes from STREAM into a new sequence.  Signals an error
if the file was too short to read from."
  (let* ((buffer (make-byte-vector count))
	 (read-count (read-sequence buffer stream)))
    (unless (= count read-count)
      (error (make-condition on-failure-raise)))
    buffer))
