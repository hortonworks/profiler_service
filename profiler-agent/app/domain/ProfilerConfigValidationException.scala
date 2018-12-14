package domain

class ProfilerConfigValidationException(val code:String, val message: String) extends Exception(s"$code : $message")

class InstanceNotFoundException(val code:String, val message: String) extends Exception(s"$code : $message")
