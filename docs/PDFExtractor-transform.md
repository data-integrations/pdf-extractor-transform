# PDF Extractor Action


Description
-----------
This transform leverages the [Apache PDF Box](https://pdfbox.apache.org/) library to extract text and metadata from a PDF file. It is usually used in conjunction with the Whole File Reader plugin since it requires the entire contents of the PDF to be loaded into a single message and passed into the transform. Due to this, there may be memory issues when loading extremely large PDF files. 

Use Case
--------
A developer is given a large directory full of PDF files, possibly a set of papers or legal documents, and needs to quickly extract the title, author, and text from these documents. The developer can read the files using the Whole File Reader plugin, then leverage this transform to extract the required information, before writing the text to a table for easy querying.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Source Field Name** | **Y** | None | This is the name of the field on the input record containing the pdf file. It must be of type ``bytes`` and it must contain the entire contents of the PDF file. |
| **Continue Processing If There Are Errors?** | **Y** | false | Indicates if the pipeline should continue if processing a single PDF fails. |

Usage Notes
-----------

This plugin requires the entire contents of the PDF File to be loaded into memory for processing. This could cause issues when reading large PDF files or files with lots of images.

This plugin does not support encrypted PDFs with passwords. A runtime exception will be thrown, but can be ignored if desired.