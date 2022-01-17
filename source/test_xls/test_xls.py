import openpyxl
import time
import sys
import os


def print_current_time():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print(f"Current time is --> {current_time}")


print("Reading file")

print_current_time()

file_direcory = sys.argv[1]
print(f"Directory is {file_direcory}")
file_name = 'original_xls.xlsx'

file_exists_res = os.path.exists(file_direcory + file_name)

if not file_exists_res:
    print("File not exist in specified path and exiting")
else:
    print("Loading xls file")

    print_current_time()
    # read excel sheet
    excel_file = openpyxl.load_workbook(file_direcory + file_name)

    print("XLS file loaded ..")
    print_current_time()

    print("Saving the xls report")
    print_current_time()
    excel_file.save(file_direcory + 'modified_xls.xlsx')
    print("File saved and exiting ...")
    print_current_time()


# python3 xls_test_script.py <path_to_direcot>


# pip3 install openpyxl

# python3 -m venv test_env

# source test_env/bin/activate
