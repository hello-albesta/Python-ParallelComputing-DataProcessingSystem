import threading as thread
from time import time

import pandas as pd
import numpy as np

from threads import Threading
from custom_defined import const_dict as const


class DataProcesses:
    """
    Handle file data row duplicates.

    :param resource_data: The resource data to handle duplicates for.
    :type resource_data: pandas.DataFrame

    :return: The cleaned resource data without duplicates.
    :rtype: pandas.DataFrame

    ==================================================================================================================

    The explanation of the code snippet:

    This code snippet defines a static method called handle_file_data_row_duplicate.
    It takes in a parameter called resource_data.

    The method first checks for duplicate rows in resource_data and stores the count of duplicates in a variable
    called data_row_duplicate_flag.

    Then, it removes the duplicate rows from resource_data using the drop_duplicates() method and
    resets the index.

    After that, it prints a message indicating the number of rows that were deleted.

    Finally, it returns the modified resource_data without the duplicate rows.
    """
    @staticmethod
    def handle_file_data_row_duplicate(resource_data):
        data_row_duplicate_flag = len(resource_data[resource_data.duplicated()])

        resource_data = resource_data.drop_duplicates().reset_index(drop=True)

        print(f"HAS BEEN DELETED: {data_row_duplicate_flag} ROWS")

        return resource_data

    """
    Remove rows from resource_data with invalid file names, and return the modified resource_data.

    Args:
        resource_data (DataFrame): The input DataFrame containing resource data.

    Returns:
        DataFrame: The modified resource_data DataFrame with invalid file names removed.

    ==================================================================================================================

    The explanation of the code snippet:

    This code snippet is a static method called handle_file_name_invalid_file_name
    that takes in a parameter resource_data.

    It first calculates the number of rows in resource_data where the COLUMN_PATH contains a specific string:
    SEARCHED_INVALID_FILE_NAME.

    Then, it filters out the rows in resource_data where COLUMN_PATH contains SEARCHED_INVALID_FILE_NAME,
    and assigns the filtered data back to resource_data.

    After that, it prints the number of rows that have been deleted due to the filter.

    Finally, it returns the filtered resource_data.
    """
    @staticmethod
    def handle_file_name_invalid_file_name(resource_data):
        invalid_file_name_data_flag = len(resource_data[
                                              resource_data[
                                                  const.COLUMN_PATH]
                                          .str.lower().str.contains(const.SEARCHED_INVALID_FILE_NAME)
                                          ])

        resource_data = resource_data[
            ~resource_data[const.COLUMN_PATH].str.lower().str.contains(const.SEARCHED_INVALID_FILE_NAME)
        ].reset_index(drop=True)

        print(f"HAS BEEN DELETED: {invalid_file_name_data_flag} ROWS")

        return resource_data

    """
    A static method that handles invalid file names in the resource data.

    Parameters:
        resource_data (pandas.DataFrame): The resource data containing file information.

    Returns:
        pandas.DataFrame: The filtered resource data containing only files with invalid names.

    ==================================================================================================================

    The explanation of the code snippet:

    This code snippet defines a static method called handle_file_name_invalid_format.
    It takes in a parameter called resource_data.

    The code first calculates the number of rows in resource_data that do not contain the string:
    const.SEARCHED_INVALID_FORMAT_FILE in the const.COLUMN_PATH column.
    This count is stored in the variable invalid_file_format_data_flag.

    Then, the code filters resource_data to only keep the rows that contain the string:
    const.SEARCHED_INVALID_FORMAT_FILE in the const.COLUMN_PATH column.

    After that, it prints a message indicating the number of rows that were deleted.

    Finally, it returns the filtered resource_data as the result of the method.
    """
    @staticmethod
    def handle_file_name_invalid_format(resource_data):
        invalid_file_format_data_flag = len(resource_data[
                                                ~resource_data[
                                                    const.COLUMN_PATH].str.contains(const.SEARCHED_INVALID_FORMAT_FILE)
                                            ])

        resource_data = resource_data[resource_data[const.COLUMN_PATH].str.contains(const.SEARCHED_INVALID_FORMAT_FILE)]

        print(f"HAS BEEN DELETED: {invalid_file_format_data_flag} ROWS")

        return resource_data

    """
    Convert the file name in the resource data to title case.

    Parameters:
        resource_data (pandas.DataFrame): The resource data containing the file names.

    Returns:
        pandas.DataFrame: The updated resource data with the file names converted to title case.

    ==================================================================================================================

    The explanation of the code snippet:

    This code defines a static method called handle_file_name_title_case
    that takes in a parameter called resource_data.

    Inside the method, it converts the value of the COLUMN_TITLE key in the resource_data dictionary
    to lowercase using the str.lower() method.

    Then, it returns the modified resource_data dictionary.
    """
    @staticmethod
    def handle_file_name_title_case(resource_data):
        resource_data[const.COLUMN_TITLE] = resource_data[const.COLUMN_TITLE].str.lower()

        return resource_data

    """
    Generate the function comment for the given function body in a markdown code block with the correct language syntax.

    :param resource_data: The data to be processed.
    :type resource_data: pandas.DataFrame
    :return: The processed data with corrected text spacing.
    :rtype: pandas.DataFrame

    ==================================================================================================================

    The explanation of the code snippet:

    This code snippet defines a static method called handle_file_content_text_spacing.
    It takes a parameter resource_data, which seems to be a DataFrame object.

    The method iterates over the columns of the DataFrame and checks for any occurrences of
    leading or multiple spaces in the data within each column.

    If such occurrences are found,
    the method removes the extra spaces and updates the DataFrame with the modified data.

    Finally, it returns the modified DataFrame.
    """
    @staticmethod
    def handle_file_content_text_spacing(resource_data):
        check_per_cols = {}

        column_data = [column for column in resource_data.columns]

        for column_title in column_data:
            leading_or_multi_spaces_total_flag = 0

            for data_per_column in resource_data[column_title]:
                if (data_per_column.count(" ") > 0 and
                        len(data_per_column.split()) - data_per_column.count(" ") != 1):
                    leading_or_multi_spaces_total_flag += 1

            check_per_cols[column_title] = leading_or_multi_spaces_total_flag

        for key in check_per_cols.keys():
            sample_spaces_data = []

            if check_per_cols[key] > 0:
                for data in resource_data[key]:
                    sample_spaces_data.append(" ".join(data.split()))

                sample_spaces_data_to_series = pd.Series(data=sample_spaces_data,
                                                         name=key)
                resource_data[key] = sample_spaces_data_to_series

        return resource_data

    """
    Handle null values in file content.

    Args:
        resource_data (pandas.DataFrame): The input resource data.

    Returns:
        pandas.DataFrame: The resource data after handling null values.

    ==================================================================================================================

    The explanation of the code snippet:

    This code defines a static method called handle_file_content_null_value that takes in
    resource_data as a parameter.
    
    It checks for null values in the resource_data and drops rows with null values for each column
    that has at least one null value.

    Finally, it returns the modified resource_data.
    """
    @staticmethod
    def handle_file_content_null_value(resource_data):
        check_per_cols = {key: value for key, value in resource_data.isnull().sum().items()}

        for column_title in check_per_cols.keys():
            if check_per_cols[column_title] > 0:
                resource_data.dropna(axis=0)

        return resource_data


"""
Creates a new thread to start the threading process.

Args:
    resource_data (Type): The sample resource data to be passed to the threading object.
    multi_thread_lock (Type): The thread lock object to be passed to the threading object.

Returns:
    None

==================================================================================================================

The explanation of the code snippet:

This code defines a function called starting_threading_process that takes two parameters:
resource_data and multi_thread_lock.

Inside the function, it creates an instance of a Threading class with the resource_data and
multi_thread_lock as arguments.

Then, it calls the start_threading method on the threading_obj instance.
"""


def starting_threading_process(resource_data, multi_thread_lock):
    threading_obj = Threading(sample_resource_data=resource_data, thread_lock=multi_thread_lock)
    threading_obj.start_threading()


"""
Initializes a single-threading process using the given `single_thread_res_resource_data`.

Args:
    single_thread_res_resource_data (any): The data to be processed in the single-threading process.

Returns:
    None

Prints:
    - "SINGLE-THREAD PROCESS IS STARTING NOW!"
    - The time taken to process the data in seconds.
    - The success message for the single-threading process.

==================================================================================================================

The explanation of the code snippet:

This code snippet defines a function single_threading_init that starts a single-threaded process.
It creates a thread t_single and starts it by calling t_single.start().
Then it waits for the thread to finish by calling t_single.join().

After the thread finishes, it calculates the total time taken for the processing and
prints it along with a success message.
"""


def single_threading_init(single_thread_res_resource_data):
    timer_process_checker_start = time()

    print("SINGLE-THREAD PROCESS IS STARTING NOW!")

    thread_locked = False

    t_single = thread.Thread(target=starting_threading_process, args=(single_thread_res_resource_data,
                                                                      thread_locked))

    t_single.start()
    t_single.join()

    timer_process_checker_end = time()
    total_time_process = round((timer_process_checker_end - timer_process_checker_start), 2)

    print(f"TIME TAKEN TO PROCESSING IT: {total_time_process} SECONDS")
    print(const.SINGLE_THREAD_SUCCESSFULLY_PROCEED)


"""
Initializes a multi-threading process.

Args:
    multi_thread_res_resource_data (list): A list containing the resource data for multi-threading.

Returns:
    None

==================================================================================================================

The explanation of the code snippet:

This code snippet is a function called multi_threading_init that initializes and
starts multiple threads to perform a threading process. It takes in a multi_thread_res_resource_data parameter,
which is a list of resource data to be split and processed by the threads.

The code splits the resource data into two parts, creates a thread lock object, and then creates three threads using
the Thread class from the thread module.

Each thread is targeted at the starting_threading_process function and is passed the split resource data and
the thread lock object as arguments.

The code then starts the threads, waits for them to finish using the join method,
and calculates the total time taken for the processing. Finally, it prints the total time and a success message.
"""


def multi_threading_init(multi_thread_res_resource_data):
    first_split_resource_data = multi_thread_res_resource_data[0]
    second_split_resource_data = multi_thread_res_resource_data[1].reset_index(drop=True)

    timer_process_checker_start = time()

    print("MULTI-THREAD PROCESS IS STARTING NOW!")

    thread_locked = thread.Lock()

    t_multi_1 = thread.Thread(target=starting_threading_process, args=(first_split_resource_data,
                                                                       thread_locked))

    t_multi_2 = thread.Thread(target=starting_threading_process, args=(second_split_resource_data,
                                                                       thread_locked))

    t_multi_3 = thread.Thread(target=starting_threading_process, args=(second_split_resource_data,
                                                                       thread_locked))

    t_multi_1.start()
    t_multi_2.start()
    t_multi_3.start()

    t_multi_1.join()
    t_multi_2.join()
    t_multi_3.join()

    timer_process_checker_end = time()
    total_time_process = round((timer_process_checker_end - timer_process_checker_start), 2)

    print(f"TIME TAKEN TO PROCESSING IT: {total_time_process} SECONDS")
    print(const.MULTI_THREAD_SUCCESSFULLY_PROCEED)


"""
Performs data preparation tasks on a given resource data file.

Args:
    multi_thread_is_enabled (bool): A flag indicating whether multi-threading is enabled.

Returns:
    None

==================================================================================================================

The explanation of the code snippet:

This code snippet is defining a function called data_preparation that takes a parameter multi_thread_is_enabled.
Inside the function, it reads an Excel file specified by resource_data_file_path and resource_data_file.

If the file is not empty, it performs a series of data processing operations on the file using functions
from a module called DataProcesses.

If multi_thread_is_enabled is False, it calls a function single_threading_init with the processed data.
Otherwise, it splits the processed data into multiple parts
and calls a function multi_threading_init with each part.

Afterwards, it deletes some temporary files and directories.
If the file is empty, it prints a warning message.
"""


def data_preparation(multi_thread_is_enabled):
    resource_data_file = const.EXCEL_FILE_RESOURCE_DATA
    resource_data_file_path = const.EXCEL_FILE_RESOURCE_DATA_PATH

    res_resource_data = pd.read_excel(resource_data_file_path + resource_data_file)

    if len(res_resource_data) > 0:
        res_resource_data = DataProcesses.handle_file_content_null_value(resource_data=res_resource_data)
        res_resource_data = DataProcesses.handle_file_content_text_spacing(resource_data=res_resource_data)
        res_resource_data = DataProcesses.handle_file_name_title_case(resource_data=res_resource_data)
        res_resource_data = DataProcesses.handle_file_name_invalid_format(resource_data=res_resource_data)
        res_resource_data = DataProcesses.handle_file_name_invalid_file_name(resource_data=res_resource_data)
        res_resource_data = DataProcesses.handle_file_data_row_duplicate(resource_data=res_resource_data)

        if multi_thread_is_enabled is False:
            single_threading_init(single_thread_res_resource_data=res_resource_data)

        else:
            split_res_resource_data = np.array_split(res_resource_data, const.NUM_OF_THREADS)

            multi_threading_init(multi_thread_res_resource_data=split_res_resource_data)

        Threading.delete_temp_directory_task_files_img()
        Threading.delete_temp_directory_task_files()
    else:
        print(f"{const.WARNING_TEXT_1}{const.EMPTY_RESOURCE_DATA_WARNS} ON "
              f"{resource_data_file}{const.WARNING_TEXT_2}")


if __name__ == '__main__':
    multi_thread_enabled_flag = True

    data_preparation(multi_thread_enabled_flag)
