import json
import logging


def write_to_file(full_file_path, contents_to_write, writing_mode='wb'):
    '''Takes byte stream json object and writes to a file with path specified as a parameter
        Params:
            **kwargs:   obj,
                        file_path
        Returns:
            None
    '''
    try:
        with open(full_file_path, mode=writing_mode) as file_obj:
            file_obj.write(contents_to_write)
    except Exception as file_writing_error:
        logging.error("Error while writing to file: {error}".format(error=file_writing_error))
        raise file_writing_error


def read_json_file(**kwargs):
    '''Reads a json file and returns the dicts
        Params:
            kwargs: file_path
        Returns:
            list of dicts
    '''

    try:
        file_path = kwargs['file_path']
        with open(file_path, mode='r+') as file_obj:
            obj = json.load(file_obj)
        return obj
    except Exception as file_read_error:
        logging.error("Error while reading file: {error}".format(error=file_read_error))
        raise file_read_error
