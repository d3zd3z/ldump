;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Use ASDF to extract a list of all of the fasls that are needed for
;;; the particular program.  Collect them into a single directory to
;;; quickly load the application.

;;; TODO: Clisp readable pprint always puts the package name in the
;;; symbols.  The symbols will have to be declared in cl-user in order
;;; for this to work.

(in-package #:cl-user)
'load-here
'path

(defpackage #:delivery
  (:use #:cl #:iterate #:cl-fad)
  (:import-from #:cl-user #:load-here #:path)
  (:export #:deploy))

(in-package #:delivery)

(defun get-fasls (system-name)
  (let ((system (asdf:find-system system-name)))
    (asdf:compile-system system)
    (iter (for (op . arg) in (asdf::traverse
			      (make-instance 'asdf:load-op :force :all) system))
	  (when (typep op 'asdf:load-op)
	    (nconcing (asdf::input-files op arg))))))

(defun make-local-name (fasl index)
  "Generate a local name for this particular fasl."
  (let ((name (pathname-name fasl))
	(type (pathname-type fasl))
	(dir (truename #p"objs/")))
    (merge-pathnames (make-pathname :name (format nil "~4,'0d-~a" index name)
				    :type type)
		     dir)))

(defun simplify-name (path)
  (let ((name (pathname-name path))
	(type (pathname-type path)))
    (make-pathname :name name :type type)))

(defvar *copy-fasls* t)

;; Some packages depend on asdf, but asdf won't ever include itself,
;; since it obviously thinks it is already running.  Not sure of a
;; proper way of getting this, but we will need it.
;; TODO: If we don't compile the exact one that the current lisp is
;; using, it usually will break the current running ASDF.
(defun build-asdf ()
  (compile-file #p"/usr/share/common-lisp/source/cl-asdf/asdf.lisp"
		:output-file (merge-pathnames #p"asdf" (truename #p"objs/"))
		:verbose nil
		:print nil))

;;; BUG: when *copy-fasls* is nil, asdf is loaded by absolute path,
;;; which prevents the build dir from being moved.

(defun deploy (system-name)
  (let* ((fasls (iter (for fasl in (get-fasls system-name))
		      (for index from 1)
		      (if *copy-fasls*
			  (progn
			    (for local = (make-local-name fasl index))
			    (copy-file fasl local :overwrite t)
			    (collect local))
			  (collect fasl))))
	 (fasls (cons (build-asdf) fasls))
	 (load-source #p"objs/load.lisp"))
    (with-open-file (stream load-source
			    :direction :output
			    :if-does-not-exist :create
			    :if-exists :supersede)
      (let* ((loaders (iter (for fasl in fasls)
			    (if *copy-fasls*
				(collect `(load-here ,(simplify-name fasl)))
				(collect `(load ,fasl)))))
	     (package '((in-package #:cl-user)))
	     (main '(funcall (find-symbol "MAIN" (find-package "LDUMP.MAIN"))))
	     (code (if *copy-fasls*
		       `(progn ,@package
			       (flet ((load-here (path)
					(load (merge-pathnames path *load-truename*))))
				 ,@loaders ,main))
		       `(progn ,@package ,@loaders ,main))))
	(let ((*package* #.*package*))
	  (write code :stream stream :pretty t :readably t))))
    (let ((*package* (find-package "CL-USER")))
      (compile-file load-source :verbose nil :print nil))))

;; Notes
;; ccl64 -n -l load -e '(main)' -e '(quit)' -- "$@"
;; sbcl --noinform --no-sysinit --no-userinit --disable-debugger --quit --load load --eval '(main)'
;; sbcl --script load.lisp "$@"

;; clisp -q -ansi -norc -x '(load "load" :verbose nil)' -x '(main)'
;;  but only sort of.  It still prints out a 'T' from the load, and
;;  the result of (main)'s evaluation.  It may be better to figure out
;;  how to get the invocation of main into the load script itself,
;;  then all will be easier.
