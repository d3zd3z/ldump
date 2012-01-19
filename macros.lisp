(in-package #:ldump)

(defmacro define-memoized-accessor (name (instance class) slot
				    &body forms)
  "Define a method called NAME which is an INSTANCE of CLASS.  This
definition will merely return the value of SLOT if it is bound,
otherwise it will evaluate FORM, storing the result into SLOT and
then returning it."
  (let ((temp (gensym "TEMP-")))
    `(defmethod ,name ((,instance ,class))
       (if (slot-boundp ,instance ',slot)
	   (slot-value ,instance ',slot)
	   (let ((,temp (progn ,@forms)))
	     (setf (slot-value ,instance ',slot) ,temp)
	     ,temp)))))

(defmacro define-apply-macro (name func &rest args)
  "Define a function NAME that applies FUNC to the rest of it's ARGS."
  `(defun ,name (args)
     (apply #',func ,@args args)))
