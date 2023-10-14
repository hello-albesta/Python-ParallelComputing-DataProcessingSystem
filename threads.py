import threading as thread
import os.path
import shutil
from pathlib import Path

import torch
import PyPDF2
import pdf2image
import easyocr
import requests
from urllib.parse import urlparse
import re
from tabulate import tabulate

from custom_defined import const_dict as const


class Threading:
    def __init__(self,
                 sample_resource_data,
                 thread_lock):
        self.sample_resource_data = sample_resource_data
        self.thread_lock = thread_lock

    """
    Delete the temporary directory and all its files.

    This static method deletes the temporary directory and all its files if it exists.
    The path to the temporary directory is specified in the `EXPORT_WORKING_FILE_DATA_PATH`
    constant defined in the `const` module.

    Parameters:
        None

    Returns:
        None

    ==================================================================================================================
    
    The explanation of the code snippet:

    This code defines a static method called delete_temp_directory_task_files that deletes a temporary directory
    if it exists.

    The method checks if a directory specified by the constant: EXPORT_WORKING_FILE_DATA_PATH,
    exists using the os.path.isdir function.

    If the directory exists, it is removed recursively using the shutil.rmtree function.
    """
    @staticmethod
    def delete_temp_directory_task_files():
        if os.path.isdir(const.EXPORT_WORKING_FILE_DATA_PATH):
            shutil.rmtree(const.EXPORT_WORKING_FILE_DATA_PATH)

    """
    Deletes the temporary directory and its files for storing image data.

    This function is a static method that is used to delete the temporary directory and its files
    for storing image data. It first checks if the temporary directory exists using the `os.path.isdir()`
    function. If the directory exists, it is deleted using the `shutil.rmtree()` function.

    Parameters:
        None

    Returns:
        None

    ==================================================================================================================

    The explanation of the code snippet:

    This code defines a static method called delete_temp_directory_task_files_img which deletes a temporary directory
    if it exists.

    The method checks if the directory const.EXPORT_IMAGE_DATA_PATH exists using os.path.isdir, and if it does,
    it removes the directory and all its contents using shutil.rmtree.
    """
    @staticmethod
    def delete_temp_directory_task_files_img():
        if os.path.isdir(const.EXPORT_IMAGE_DATA_PATH):
            shutil.rmtree(const.EXPORT_IMAGE_DATA_PATH)

    """
    This static method takes in a resource file URL and a keyword as parameters and searches for the keyword
    in the resource file.

    It returns the resource file URL if the keyword is found, otherwise it returns False.
    
    :param resource_file_url: The URL of the resource file.
    :type resource_file_url: str
    :param keyword: The keyword to search for in the resource file.
    :type keyword: str
    :return: The resource file URL if the keyword is found, otherwise False.
    :rtype: str or bool

    ==================================================================================================================

    The explanation of the code snippet:

    This code snippet defines a static method called get_keyword_info_resource_text_pdf_file.
    It takes in two parameters: resource_file_url and keyword.

    The method performs the following steps:

        It initializes a variable called found with a value of 0.

        It parses the resource_file_url using the urlparse function and extracts the base file path.

        It generates a full file name for the PDF file based on the base file path.

        It constructs a sample_path by concatenating a constant path (EXPORT_WORKING_FILE_DATA_PATH)
        with the generated PDF file name.

        It prints the value of sample_path.

        It checks if the current thread name is equal to a constant value (FIRST_MULTI_THREAD_NAME) and
        prints a message accordingly.

        It creates a PdfFileReader object from the sample_path using the PyPDF2 library.

        It retrieves the total number of pages in the PDF file.

        It clears the CUDA cache.

        It creates an OCR reader object using the easyocr library, specifying a language list and enabling GPU.

        It iterates over the range of page indices in the PDF file.

        For each page, it extracts the text content and removes extra spaces.

        It searches for the keyword in the extracted text using a case-insensitive regular expression.

        If the keyword is found, it sets found to 1 and breaks out of the loop.

        If the keyword is not found, it extracts the file name without the extension from the base file path.

        It converts the current page of the PDF file to an image using the pdf2image library.

        It constructs a file name for the image based on the PDF file name and page index.

        It constructs a path for saving the image using a constant path (EXPORT_IMAGE_DATA_PATH) and
        the image file name.

        If the image file does not exist at the specified path, it saves the image at that path.

        It performs OCR on the saved image using the OCR reader object and retrieves the OCR data.

        It searches for the keyword in each OCR data using a case-insensitive regular expression.

        If the keyword is found in any OCR data, it sets found to 1 and breaks out of the loop.

        If found is equal to 1 at this point, it returns the resource_file_url.

        If found is still 0, it returns False.
    """
    @staticmethod
    def get_keyword_info_resource_text_pdf_file(resource_file_url, keyword):
        found = 0

        resource_parsed_url = urlparse(resource_file_url)
        resource_base_file_path = resource_parsed_url.path
        export_resource_pdf_file_full_name = Path(resource_base_file_path).name

        sample_path = const.EXPORT_WORKING_FILE_DATA_PATH + export_resource_pdf_file_full_name

        print(f"sample_path: {sample_path}")

        if thread.current_thread().name == const.FIRST_MULTI_THREAD_NAME:
            print(f"this is thread 1: {sample_path}")

        else:
            print(f"this is thread 2: {sample_path}")

        resource_objects_pdf_file = PyPDF2.PdfFileReader(sample_path,
                                                         strict=False)
        resource_file_pages_total = resource_objects_pdf_file.getNumPages()

        torch.cuda.empty_cache()

        ocr_reader_file = easyocr.Reader(lang_list=[const.IDN_LANG_CODE_ISO_619],
                                         gpu=True)

        # print(f"resource_file_pages_total : {resource_file_pages_total}")

        for data_idx in range(0, resource_file_pages_total):
            file_page = resource_objects_pdf_file.getPage(data_idx)
            text_data = file_page.extractText()

            sample_data = " ".join(text_data.split())

            if re.search(pattern=f"(?i){keyword}",
                         string=sample_data):
                print("TEXT OCR PART")

                found = 1

                break

            else:
                export_resource_pdf_file_name = Path(resource_base_file_path).stem

                sample_images_data = pdf2image.convert_from_path(sample_path)
                sample_images_file_name = (f"{export_resource_pdf_file_name.title()}_Page_{data_idx + 1}."
                                           f"{const.DEF_IMAGE_DATA_FORMAT}")
                sample_image_path = (const.EXPORT_IMAGE_DATA_PATH + sample_images_file_name)

                if not os.path.exists(sample_image_path):
                    sample_images_data[data_idx].save(fp=sample_image_path,
                                                      format=const.DEF_IMAGE_DATA_FORMAT)

                res_ocr_data = ocr_reader_file.readtext(image=sample_image_path,
                                                        paragraph=True,
                                                        detail=0)

                for ocr_data in res_ocr_data:
                    if re.search(pattern=f"(?i){keyword}",
                                 string=ocr_data):
                        print("IMAGE OCR PART")

                        found = 1

                        break

                if found == 1:
                    break

        if found == 1:
            return resource_file_url

        else:
            return False

    """
    The `search_keyword_processing` function performs keyword processing on a given dataset.

    Returns:
        res_searched_keyword_dict (dict): A dictionary containing the results of the keyword processing. 
        It has the following keys:
            - const.KEYWORD_TABLE_COL_NAME[0]: A list of indices indicating the order of the found keywords.
            - const.KEYWORD_TABLE_COL_NAME[1]: A list of searched keywords.
            - const.KEYWORD_TABLE_COL_NAME[2]: A list of found keywords for each searched keyword.

    Raises:
        None

    ==================================================================================================================
    
    The explanation of the code snippet:

    This code snippet defines a method called search_keyword_processing in a class.

    The method performs the following steps:

    Checks if a directory specified by EXPORT_IMAGE_DATA_PATH exists.
    If not, it creates a new directory specified by TEMP_IMAGE_DATA_DIRECTORY in the current working directory.

    Initializes an empty list called file_found_keywords_list.

    Iterates over a list of searched keywords and performs the following steps for each keyword:
        Iterates over a list of resource data paths and for each path,
        calls a function: get_keyword_info_resource_text_pdf_file with the resource file URL and
        the searched keyword as arguments.

    If the function call returns a valid response,
    it retrieves the title of the resource file and appends it to temp_found_keyword_list.

    If the number of elements in temp_found_keyword_list is greater than 1,
    it joins the elements with a comma and a newline character and appends the result to file_found_keywords_list.

    If the number of elements in temp_found_keyword_list is exactly 1,
    it appends the single element to file_found_keywords_list.

    If temp_found_keyword_list is empty, it appends a default value to file_found_keywords_list.

    Prints some debugging information based on the current thread's name.

    If the length of file_found_keywords_list is greater than 0,
    it creates a dictionary called res_searched_keyword_dict with three keys:
    KEYWORD_TABLE_COL_NAME[0], KEYWORD_TABLE_COL_NAME[1], and KEYWORD_TABLE_COL_NAME[2].
    The values are populated with indexes, the searched keywords,
    and the contents of file_found_keywords_list, respectively.

    If file_found_keywords_list is empty, it creates a dictionary called res_searched_keyword_dict with three keys:
    KEYWORD_TABLE_COL_NAME[0], KEYWORD_TABLE_COL_NAME[1], and KEYWORD_TABLE_COL_NAME[2].
    The values are set to default values.

    Prints some debugging information based on the current thread's name.

    Returns the res_searched_keyword_dict.
    """
    def search_keyword_processing(self):
        if not os.path.isdir(const.EXPORT_IMAGE_DATA_PATH):
            temp_img_data_dir = const.TEMP_IMAGE_DATA_DIRECTORY
            temp_img_data_dir_path = os.path.join(os.getcwd() + '/', temp_img_data_dir)

            os.mkdir(temp_img_data_dir_path)

        file_found_keywords_list = []

        for searched_keyword_idx in range(len(const.SEARCHED_KEYWORDS)):
            temp_found_keyword_list = []

            for i in range(len(self.sample_resource_data[const.COLUMN_PATH])):
                response = Threading.get_keyword_info_resource_text_pdf_file(
                    resource_file_url=self.sample_resource_data[const.COLUMN_PATH][i],
                    keyword=const.SEARCHED_KEYWORDS[searched_keyword_idx]
                )

                if response is not False:
                    keyword_result = self.sample_resource_data.loc[
                        self.sample_resource_data[const.COLUMN_PATH] == response,
                        const.COLUMN_TITLE].iloc[0].title()

                    temp_found_keyword_list.append(keyword_result)

                if i > 3:
                    break

            if len(temp_found_keyword_list) > 1:
                file_found_keywords_list.append(',\n'.join(temp_found_keyword_list))

            elif len(temp_found_keyword_list) == 1:
                file_found_keywords_list.append(temp_found_keyword_list[0])

            else:
                empty_data = const.EMPTY_SEARCHED_DATA_VALUE.title()
                file_found_keywords_list.append(empty_data)

            if thread.current_thread().name == const.FIRST_MULTI_THREAD_NAME:
                print(f"check df thread 1: {self.sample_resource_data}")
                print(f"check panjang df thread 1: {len(self.sample_resource_data)}")
                print(f"check data thread 1: {file_found_keywords_list}")

            elif thread.current_thread().name == const.SECOND_MULTI_THREAD_NAME:
                print(f"check df thread 2: {self.sample_resource_data}")
                print(f"check panjang df thread 2: {len(self.sample_resource_data)}")
                print(f"check data thread 2: {file_found_keywords_list}")

            else:
                print(f"check df thread 3: {self.sample_resource_data}")
                print(f"check panjang df thread 3: {len(self.sample_resource_data)}")
                print(f"check data thread 3: {file_found_keywords_list}")

        if len(file_found_keywords_list) > 0:
            keyword_found_list_idx = [
                file_keyword_idx + 1 for file_keyword_idx in range(len(file_found_keywords_list))
            ]

            res_searched_keyword_dict = {
                const.KEYWORD_TABLE_COL_NAME[0]: keyword_found_list_idx,
                const.KEYWORD_TABLE_COL_NAME[1]: const.SEARCHED_KEYWORDS,
                const.KEYWORD_TABLE_COL_NAME[2]: file_found_keywords_list
            }

        else:
            res_searched_keyword_dict = {
                const.KEYWORD_TABLE_COL_NAME[0]: [1],
                const.KEYWORD_TABLE_COL_NAME[1]: [const.NO_DATA_VALUE],
                const.KEYWORD_TABLE_COL_NAME[2]: [const.NO_DATA_VALUE]
            }

        if thread.current_thread().name == const.FIRST_MULTI_THREAD_NAME:
            print(f"check df thread 1: {self.sample_resource_data}")
            print(f"check panjang df thread 1: {len(self.sample_resource_data)}")
            print(f"res_searched_keyword_dict 1: {res_searched_keyword_dict}")

        elif thread.current_thread().name == const.SECOND_MULTI_THREAD_NAME:
            print(f"check df thread 2: {self.sample_resource_data}")
            print(f"check panjang df thread 2: {len(self.sample_resource_data)}")
            print(f"res_searched_keyword_dict 2: {res_searched_keyword_dict}")

        else:
            print(f"check df thread 3: {self.sample_resource_data}")
            print(f"check panjang df thread 3: {len(self.sample_resource_data)}")
            print(f"res_searched_keyword_dict 3: {res_searched_keyword_dict}")

        return res_searched_keyword_dict

    """
    Downloads a PDF file from an online resource given its URL.

    :param resource_file_url: The URL of the resource file to download.
    :type resource_file_url: str

    :return: True if the file was successfully downloaded, or the title of the resource file if an error occurred.
    :rtype: Union[bool, str]

    ==================================================================================================================
    
    The explanation of the code snippet:

    This code snippet defines a static method called get_online_resource_pdf_file that takes
    a resource_file_url as input.

    It tries to download a PDF file from the given URL and save it to a local file.
    If the download is successful, it returns True.

    If there is an HTTP error or a connection timeout during the download,
    it prints an error message and returns the title of the resource file.
    """
    @staticmethod
    def get_online_resource_pdf_file(resource_file_url):
        resource_file_name = os.path.basename(resource_file_url)

        try:
            res_request = requests.get(resource_file_url, stream=True)

            resource_file_path = os.path.join(os.getcwd() + const.IMPORT_RESOURCE_FILE_DATA_PATH, resource_file_name)

            with open(resource_file_path, 'wb') as pdf_object:
                pdf_object.write(res_request.content)

                print(f"{const.FILE_SUCCESSFULLY_SAVED} {resource_file_name}")

                return True

        except requests.exceptions.HTTPError as err_http_error_except:
            print(const.REQ_HTTP_ERROR_ENCOUNTERED)
            print(const.WARNING_TEXT_1 + err_http_error_except.args[0] + const.WARNING_TEXT_2)

            return resource_file_name.title()

        except requests.exceptions.ConnectTimeout:
            print(const.REQ_CONNECTION_TIMEOUT_ENCOUNTERED)

            return resource_file_name.title()

    """
    Downloads online resource PDF files and prints the result.

    :param self: The instance of the class.

    :return: None

    This code snippet defines a method called download_online_resource_pdf_file.

    The method checks if a directory exists and creates it if it doesn't.
    It then loops over a list of resource URLs and downloads each resource if it doesn't already exist locally.

    If a resource fails to download, its URL is added to a list called file_not_found_list.

    Finally, the method prints the contents of the file_not_found_list in a formatted table using
    the tabulate function from the tabulate library.

    Let me know if you need more information!
    """
    def download_online_resource_pdf_file(self):
        file_not_found_list = []

        if not os.path.isdir(const.EXPORT_WORKING_FILE_DATA_PATH):
            temp_resource_file_dir = const.TEMP_RESOURCE_FILE_DIRECTORY
            temp_resource_file_dir_path = os.path.join(os.getcwd() + '/', temp_resource_file_dir)

            os.mkdir(temp_resource_file_dir_path)

        for data_file_idx in range(len(self.sample_resource_data[const.COLUMN_PATH])):
            resource_parsed_url = urlparse(self.sample_resource_data[const.COLUMN_PATH][data_file_idx])
            resource_base_file_path = resource_parsed_url.path
            export_resource_pdf_file_full_name = Path(resource_base_file_path).name

            local_export_file_path = const.EXPORT_WORKING_FILE_DATA_PATH + export_resource_pdf_file_full_name

            if not os.path.exists(local_export_file_path):
                res_download_success = Threading.get_online_resource_pdf_file(
                    resource_file_url=self.sample_resource_data[const.COLUMN_PATH][data_file_idx]
                )

                if res_download_success is not True:
                    file_not_found_list.append(res_download_success)

        if len(file_not_found_list) > 0:
            file_not_found_list_idx = [
                file_not_found_idx + 1 for file_not_found_idx in range(len(file_not_found_list))
            ]

            file_not_found_list_stat = [
                const.FILE_NOT_FOUND.title() for _ in range(len(file_not_found_list))
            ]

            file_not_found_dict = {
                const.FILE_NOT_FOUND_TABLE_COL_NAME[0]: file_not_found_list_idx,
                const.FILE_NOT_FOUND_TABLE_COL_NAME[1]: file_not_found_list,
                const.FILE_NOT_FOUND_TABLE_COL_NAME[2]: file_not_found_list_stat
            }

        else:
            file_not_found_dict = {
                const.FILE_NOT_FOUND_TABLE_COL_NAME[0]: [1],
                const.FILE_NOT_FOUND_TABLE_COL_NAME[1]: [const.NO_DATA_VALUE],
                const.FILE_NOT_FOUND_TABLE_COL_NAME[2]: [const.NO_DATA_VALUE]
            }

        print("DISPLAY THE RESULT")
        print("==============================================================")
        print(tabulate(tabular_data=file_not_found_dict,
                       headers='keys',
                       tablefmt='fancy_grid',
                       showindex=False,
                       stralign="center",
                       numalign="center"))
        print("==============================================================")

    """
    Execute the threading process.

    This function starts a new thread to download an online resource PDF file. It then performs a keyword search processing and displays the result in a nicely formatted table.

    Parameters:
        None

    Returns:
        None

    ==================================================================================================================
    
    The explanation of the code snippet:

    This code snippet defines a method called start_threading that does the following:

    Calls the download_online_resource_pdf_file method.

    Calls the search_keyword_processing method and assigns the result to the variable res_searched_keyword_dict.

    Prints a header and a table using the tabulate function, displaying the contents of res_searched_keyword_dict.
    """
    def start_threading(self):
        self.download_online_resource_pdf_file()

        res_searched_keyword_dict = self.search_keyword_processing()

        print("DISPLAY THE RESULT")
        print("==============================================================")
        print(tabulate(tabular_data=res_searched_keyword_dict,
                       headers='keys',
                       tablefmt='fancy_grid',
                       showindex=False,
                       stralign="center",
                       numalign="center"))
        print("==============================================================")
