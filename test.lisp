;;; Root of test framework

(defpackage #:ldump.test
  (:use #:cl #:iterate #:lift
	#:ldump
	#:ldump.chunk)
  (:import-from #:cl-base64 #:integer-to-base64-string))
(in-package #:ldump.test)

(defvar *test-suites* ()
  "List of all of the suites to test.")

(defun run-all-tests (&key return-errors)
  (dolist (test *test-suites*)
    (let ((result (run-tests :suite test)))
      (when return-errors
	(return-from run-all-tests result))
      (when (failures result)
	(error "Failures in test: ~S" test))
      (when (errors result)
	(error "Errors in test: ~S" test)))))

(defun make-temporary-directory ()
  "Generate a temporary directory, returning it's pathname"
  (iter (for path = (format nil "/tmp/test-pool-~A/"
			    (integer-to-base64-string
			     (random (ash 1 64))
			     :uri t)))
	(for count from 1)
	(when (> count 35)
	  (error "Unable to create temporary directory: ~S" path))
	(multiple-value-bind (pathspec created)
	    (ensure-directories-exist path)
	  (declare (ignore pathspec))
	  (when created (leave path)))))

(defun remove-temporary-directory (path)
  "Remove the named temporary directory"
  ;; Take advantage of asdf:run-shell-command, which will be available
  ;; because we use asdf.
  (unless (zerop (asdf:run-shell-command "rm -rf ~S" path))
    (error "Unable to remove temporary directory: ~S" path)))

(defmacro with-temporary-directory ((path) &body body)
  "Create a temporary directory, and evaluate BODY with PATH bound to
the name of this directory.  Cleans up the directory when BODY
leaves."
  (unless (symbolp path)
    (error "Must use a simple symbol for PATH argument"))
  `(let ((,path (make-temporary-directory)))
     (unwind-protect
	  (progn ,@body)
       (remove-temporary-directory path))))

(deftestsuite hash-compare-mixin ()
  ()
  :equality-test #'(lambda (a b)
		     (not (mismatch a b))))

(deftestsuite tmpdir-mixin ()
  (tmpdir)
  (:setup
   (setf tmpdir (make-temporary-directory)))
  (:teardown
   (remove-temporary-directory tmpdir)))
