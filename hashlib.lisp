;;; Hashing library, in the spirit of Python's hashlib.

(defpackage #:hashlib
  (:use #:cl #:cffi #:iterate)
  (:export #:sha1-objects
	   #:hexify #:unhexify))
(in-package #:hashlib)

(define-foreign-library libcrypto
  (:unix (:or "libcrypto.so.0.9.8" "libcrypto.so"))
  (t (:default "libssl")))
(use-foreign-library libcrypto)

;;; C bindings to the sha1 library.

(defcfun ("SHA1_Init" %sha1-init) :void (ctx :pointer))
(defcfun ("SHA1_Update" %sha1-update) :void
  (ctx :pointer) (data :pointer) (len :long))
(defcfun ("SHA1_Final" %sha1-final) :void
  (md :pointer) (ctx :pointer))

;;; TODO: Could grovel for this, but it makes the build system so much
;;; more complex.
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defconstant +sha-ctx-length+ 96)
  (defconstant +sha1-hash-length+ 20))

(defun make-sha1-context ()
  "Create a context object for hashing.  Add data with SHA1-ITEM.
Must free the resulting foreign pointer."
  (let ((ctx (foreign-alloc :char :count +sha-ctx-length+)))
    (%sha1-init ctx)
    ctx))

(defun sha1-vector (ctx obj &optional (start 0) (end (length obj)))
  (assert (>= start 0))
  (assert (<= start (length obj)))
  (assert (>= end start))
  (assert (<= end (length obj)))
  (with-pointer-to-vector-data (obj* obj)
    (%sha1-update ctx (inc-pointer obj* start)
		  (- end start))))

(defun sha1-item (ctx obj)
  "Update the SHA1 hash, appropriately for several different types."
  (etypecase obj
    (string (sha1-string ctx obj))
    (keyword (sha1-string ctx (symbol-name obj)))
    ((vector (unsigned-byte 8))
     (sha1-vector ctx obj))))

(defun sha1-string (ctx string)
  "Update the SHA1 with the string data.  Convenience wrapper,
converst the string appropriately."
  (sha1-item ctx (babel:string-to-octets string)))

(defun sha1-final (ctx)
  "Finalize the SHA1 computation.  Returns the final hash."
  (let ((hash (make-shareable-byte-vector +sha1-hash-length+)))
    (with-pointer-to-vector-data (hash* hash)
      (%sha1-final hash* ctx))
    hash))

(defmacro with-sha1-context ((ctx) &body body)
  "Compute BODY, binding CTX to the context value to pass into
SHA1-ITEM.  The resulting form evalues to the final hash."
  `(let ((,ctx (make-sha1-context)))
     (unwind-protect
	  (progn ,@body (sha1-final ,ctx))
       (foreign-free ,ctx))))

(defun sha1-objects (&rest objs)
  "Compute the SHA1 sum of the concatenation of the specified
objects"
  (with-sha1-context (ctx)
    (mapc #'(lambda (obj)
	      (sha1-item ctx obj))
	  objs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Class interface.

(defclass hash ()
  ((block-size :type fixnum
	       :allocation :class
	       :reader hash-block-size)
   (digest-size :type fixnum
		:reader hash-digest-size)))

(defgeneric %update-hash (hash data)
  (:documentation
   "Update the hash with the data payload."))

(defgeneric get-digest (hash)
  (:documentation
   "Retrieve the digest from the given hash."))

(defgeneric clone-hash (hash)
  (:documentation
   "Return a new hash state based on the state of the specified hash.
The new hash can be updated independently."))

(defun thing-to-ubv8 (thing)
  (etypecase thing
    (keyword (thing-to-ubv8 (symbol-name thing)))
    (string (babel:string-to-octets thing))
    ((vector (unsigned-byte 8))
     thing)))

(defun update-hash (hash data)
  "Update the hash with the given data.  Handles a few basic types."
  ;; TODO: Handle displaced arrays.
  (%update-hash hash (thing-to-ubv8 data)))

(defun make-sha1-hash (&rest initializers)
  "Make a new SHA1 hash object, possibly initializing it with the
given initializers."
  (let ((hash (make-instance 'sha1-hash)))
    (dolist (item initializers hash)
      (update-hash hash item))))

;;; TODO: Not a particularly plesant interface.

(defun hexify (hash)
  "Convert a hash to hex notation"
  (let ((hex (make-array (* 2 (length hash))
			 :element-type 'character
			 :fill-pointer 0)))
    (dotimes (i (length hash) hex)
      (format hex "~(~2,'0x~)" (elt hash i)))))

(defun unhexify (hex-hash)
  "Convert a hex hash back into a byte-array form."
  (check-type hex-hash (vector character 40))
  (let ((hash (make-array 20 :element-type '(unsigned-byte 8))))
    (iter (for byte-pos from 0)
	  (declare (type fixnum byte-pos))
	  (for pos from 0 to 38 by 2)
	  (declare (type fixnum pos))
	  (setf (aref hash byte-pos)
		(parse-integer hex-hash :start pos :end (+ pos 2)
			       :radix 16)))
    hash))
