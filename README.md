# large_xml_parser
A large xml file parser

## Assumptions:
1) The XML file is always very big in size.
2) We have aws credential set in local or EC2 machine where this code is deployed, then only csv upload to AWS s3 code will work.
3) XML file structure as in the elements dont change.

## Notes to run in local
* Dependencies can be installed from requirements.txt
* Source XML file in ./xml_files/ needs to be unzipped or new one added and corresponding name is updated in large_xml_parser.py file
* Final csv file present in ./csv/
* CSV upload to s3 function is commented off, uncommenting it will raise error but will not stop the primary functionality of parsing xml and generating csv

## Requirements Achieved
* Download the xml from this link
* From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
* Extract the xml from the zip.
* Convert the contents of the xml into a CSV with the following header:
    * FinInstrmGnlAttrbts.Id
    * FinInstrmGnlAttrbts.FullNm
    * FinInstrmGnlAttrbts.ClssfctnTp
    * FinInstrmGnlAttrbts.CmmdtyDerivInd
    * FinInstrmGnlAttrbts.NtnlCcy
    * Issr
* Store the csv from step 4) in an AWS S3 bucket
* Python 3
* Pydoc
* Logging
* Unit Tests
* PEP8 Standards - Code verified using pydocstyle package for pep8 standards( few I couldn't resolve)
## Requirements Not Achieved
* The above function should be run as an AWS Lambda (Optional)

## Other Notes
Could have attempted to write code for deployment to lambda function using AWS cli, but was not sure about what changes or setups are required to be done  from a devops standpoint, also did't want to mess up the code with unknown code and write test cases for it. Also its my first time writing python unit test cases and this is the coverage I could manage:
<img width="799" alt="Screenshot 2021-06-14 at 11 06 57 PM" src="https://user-images.githubusercontent.com/30325702/121934815-62eca900-cd65-11eb-99c3-68d9e1c9c018.png">
