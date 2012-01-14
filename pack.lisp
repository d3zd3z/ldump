;;; Packed data.

(defpackage #:ldump.pack
  (:use #:cl #:iterate)
  (:export #:pack-le-integer #:unpack-le-integer))
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

