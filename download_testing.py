import torch
import sys

from tabulate import tabulate

import os
from pprint import pprint
from urllib.parse import urlparse
from pathlib import Path

import requests
import PyPDF2
import re

import textract

from custom_defined import const_dict as const


def download_pdf_file(url: str) -> bool:
    response = requests.get(url, stream=True)
    pdf_file_name = os.path.basename(url)

    if response.status_code == 200:
        filepath = os.path.join(os.getcwd() + const.DOWNLOAD_RESOURCE_FILE_DATA_PATH, pdf_file_name)

        with open(filepath, 'wb') as pdf_object:
            pdf_object.write(response.content)
            print(f'{pdf_file_name} was successfully saved!')

            return True

    else:
        print(f'Uh oh! Could not download {pdf_file_name},')
        print(f'HTTP response status code: {response.status_code}')

        return False


if __name__ == '__main__':
    URL = 'https://jdih.batam.go.id/dokumen-hukum/storage/upload/Peraturan_Walikota_Batam_No_1_Tahun_2022.pdf'
    download_pdf_file(URL)

    parsed_url = urlparse(URL)
    base = parsed_url.path

    filename = Path(base).name
    filename_without_extension = Path(filename).stem

    test_name = ''
    objects = PyPDF2.PdfFileReader(const.EXPORT_WORKING_FILE_DATA_PATH + "Peraturan_Walikota_Batam_No_1_Tahun_2022.pdf",
                                   strict=False)
    num_pages = objects.getNumPages()

    for i in range(0, num_pages):
        page = objects.getPage(i)
        text = page.extractText()

        sample = " ".join(text.split())

        if re.search(pattern=const.SEARCHED_KEYWORDS,
                     string=sample):
            print("found")

    for key, value in res_sample_output_data.to_dict().items():
        print(value[1127][0])
        if (value ==
                "http://jdih.batam.go.id/dokumen-hukum/storage/upload/Peraturan_Walikota_Batam_No_23_Tahun_2022.pdf"):
            print(key)

    res_sample_output_data2 = res_sample_output_data[res_sample_output_data[const.COLUMN_PATH]
    .str.contains(
        "http://jdih.batam.go.id/dokumen-hukum/storage/upload/Peraturan_Walikota_Batam_No_23_Tahun_2022.pdf")]

    test = res_sample_output_data2.to_dict()[const.COLUMN_TITLE][0]
    print(test)

    resource_objects_pdf_file = PyPDF2.PdfFileReader(f"{const.EXPORT_WORKING_FILE_DATA_PATH}"
                                                     f"Peraturan_Walikota_Batam_No_23_Tahun_2022.pdf", strict=False)


def get_dummy():
    return ["test143", "t12233"]


test = ['test 123', 'test 1111']
keyword = [1, 2]
testt = {}

for i in range(len(keyword)):
    for a in range(len(test)):
        functs = get_dummy()

        if len(functs) > 0:
            testt[i] = functs

print(testt)

text = textract.process(const.EXPORT_WORKING_FILE_DATA_PATH + "2022_Tahun_2022.pdf")
print(text)

import io
from PIL import Image
import pytesseract
from wand.image import Image as wi


file = Image.open(const.EXPORT_WORKING_FILE_DATA_PATH + "Peraturan_Walikota_Batam_No_55_Tahun_2022.pdf")
pdfFile = wi(filename = "const.EXPORT_WORKING_FILE_DATA_PATH + "Peraturan_Walikota_Batam_No_55_Tahun_2022.pdf"", resolution = 300)
image = pdfFile.convert('jpeg')

imageBlobs = []

for img in image.sequence:
	imgPage = wi(image = img)
	imageBlobs.append(imgPage.make_blob('jpeg'))

extract = []

for imgBlob in imageBlobs:
	image = Image.open(io.BytesIO(imgBlob))
	text = pytesseract.image_to_string(image, lang = 'eng')
	extract.append(text)

print(extract)


import pdf2image
import pytesseract
from pytesseract import Output

pdf_path = "Peraturan_Walikota_Batam_No_51_Tahun_2023.pdf"

images = pdf2image.convert_from_path(pdf_path)
pil_im = images[0] # assuming that we're interested in the first page only
pytesseract.pytesseract.tesseract_cmd = 'C:/APPLICATIONS/TESSERACT/tesseract.exe'

text = []

ocr_dict = pytesseract.image_to_data(pil_im, lang='eng', output_type=Output.DICT)
# ocr_dict now holds all the OCR info including text and location on the image
text.append((ocr_dict['text']))
print(ocr_dict['text'])


print(len(text))

for i in range(len(text)):
    for x in text[i]:
        if x == "pembayaran":
            print(i+1, x)
print(text)

import glob

IMAGE_PATH =
# pytesseract.pytesseract.tesseract_cmd = 'C:/APPLICATIONS/TESSERACT/tesseract.exe'
# pdfs = glob.glob(r"Peraturan_Walikota_Batam_No_51_Tahun_2023.pdf")
# print(pdfs)
# for pdf_path in pdfs:
#     pages = pdf2image.convert_from_path(pdf_path, 500)
#
#     for pageNum,imgBlob in enumerate(pages):
#         text = pytesseract.image_to_string(imgBlob,lang='eng')
#
# print(text)
import torchvision

print(torch.cuda.is_available())

import easyocr
reader = easyocr.Reader(['id'], gpu=True)


pdf_path = "Peraturan_Walikota_Batam_No_51_Tahun_2023.pdf"
images = pdf2image.convert_from_path(pdf_path)
pil_im = images[0].save("pdf_path.jpg", "JPEG")

print(pil_im)

result = reader.readtext(image="pdf_path.jpg",
                         paragraph=True,
                         detail=0)

print(result)

test = {}

test["a"] = ['a', 'b']
test[2] = [1, 2]

pprint(test)

listss = ['bahwa: Peraturan_Walikota_Batam_No_23_Tahun_2022', 'bahwa: Peraturan_Walikota_Batam_No_53_Tahun_2023', 'undang: Peraturan_Walikota_Batam_No_23_Tahun_2022', 'undang: Peraturan_Walikota_Batam_No_53_Tahun_2023']
pprint(listss)

print(', '.join(listss))

res_searched_keyword_dict = {}
resource_file_url_list = []

for a in ['a', 'b']:
    dummy = []

    for b in ["1", "2"]:
        dummy.append(b)

    resource_file_url_list.append(', '.join(dummy))

print(resource_file_url_list)

# resource_file_url_list = [', '.join(["c", "d"]), ', '.join(["a", "b"])]

res_searched_keyword_dict["kata"] = ["a", "b"]
res_searched_keyword_dict["peraturan"] = resource_file_url_list

print(res_searched_keyword_dict)

print(tabulate(tabular_data=res_searched_keyword_dict,
               headers='keys',
               tablefmt='fancy_grid',
               showindex=False,
               stralign="center",
               numalign="right"))


# pytesseract.pytesseract.tesseract_cmd = 'C:/APPLICATIONS/TESSERACT/tesseract.exe'
# ocr_pdf_path = const.EXPORT_WORKING_FILE_DATA_PATH + export_resource_pdf_file_name
# pdf_to_images = pdf2image.convert_from_path(pdf_path=ocr_pdf_path,
#                                             fmt='jpeg')

# for i in pdf_to_images:
#     ocr_dict = pytesseract.image_to_data(image=i,
#                                          lang='eng',
#                                          output_type=Output.DICT)
#
#     sample_data = " ".join(ocr_dict['text'])
#     print(sample_data)
#
#     if re.search(pattern=keyword,
#                  string=sample_data):
#         found = 1

print(const.THREAD_SUCCESSFULLY_PROCEED)

print(5%2)