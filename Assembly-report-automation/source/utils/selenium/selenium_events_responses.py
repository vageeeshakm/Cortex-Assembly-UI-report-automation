class SeleniumUtilsConstants:
    """SeleniumUtilsConstants class """

    ELEMENT = 'element'
    EXCEPTION = 'exception'


class SeleniumEventsResponses:
    """Response functions for Selenium events and actions."""

    def success_function(data={}):
        """Wrapper function to return success message
            INPUT: {data}
                    data - ELEMENT If any
            OUTPUT:
                    Bollean value - True,
                    {dict}:
                        EXCEPTION - None,
                        ELEMENT - If present
        """
        success = True
        resp_data = {
            SeleniumUtilsConstants.EXCEPTION: None,
        }
        if data:
            resp_data.update(data)
        return success, resp_data

    def error_function(exception_object, data={}):
        """ Wrapper function to return error message
            INPUT: {data}
                    data - ELEMENT If any
            OUTPUT:
                    Bollean value - False,
                    {dict}:
                        EXCEPTION

        """
        success = False
        exception_type = type(exception_object)
        resp_data = {
            SeleniumUtilsConstants.EXCEPTION: exception_type(str(exception_object))
        }
        if data:
            resp_data.update(data)
        return success, resp_data
