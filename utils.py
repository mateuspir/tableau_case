
import sys
import threading


def system_exception(exception, file_name):
    """
        Performs a pretty print of the System Exception,
        specifying the line, file and thread number where \
        the exception was raised.
        
        Parameters:

            exception (Exception): Exception that was raised
            file_name (str): name of file who called the method
        
        Returns:

            None
    """

    # get the attributes
    line = sys.exc_info()[-1].tb_lineno
    thread = threading.current_thread().name
    message = "Error on line {} of {} inside thread: {}\n"
    

    return print(
    	message.format(
    		line,
    		file_name,
    		thread
    	),
    	type(exception).__name__,
    	exception
    )
