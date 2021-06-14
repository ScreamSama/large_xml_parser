import xml.etree.ElementTree as ET
import csv
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime
from timeit import default_timer as timer


# Creating Logger file
logger_file = "./logs/"+datetime.now().date().strftime("%Y_%m_%d")+".log"
logging.basicConfig(filename=logger_file, filemode='w',
                    format='%(name)s - %(levelname)s : %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger('LargeXmlParser')


XML_FILE = './xml_files/DLTINS_20210117_01of01.xml'
# XML_FILE = './test_files/test_1.xml'
CSV_EXPORT_FILENAME = './csv/xml_csv'
AWS_STORAGE_BUCKET_NAME = "st-data-lake"
FIELDS = [
    'FinInstrmGnlAttrbts.Id',
    'FinInstrmGnlAttrbts.FullNm',
    'FinInstrmGnlAttrbts.ClssfctnTp',
    'FinInstrmGnlAttrbts.CmmdtyDerivInd',
    'FinInstrmGnlAttrbts.NtnlCcy',
    'Issr'
    ]


class LargeXmlParser:

    """
        LargeXmlParser parses xml file in parse_xml() and sends a
        list of dictionaries with parsed values
        to write_to_csv() for creating the final csv file.
        And final upload_file sends the csv file to a s3 bucket.
    """

    def __init__(self, csv_filename, xml_file):
        """ Data Members initialisation """
        logger.info("Intialising Data Members")
        self.csv_filename = csv_filename
        self.xml_file = xml_file

    def parse_xml(self):
        """
            Function to parse large xml iteratively
            looking at start and end events in the xml.

            :return: None
        """
        # Generate csv header
        with open(self.csv_filename, 'a') as csvfile:
            # creating a csv dict writer object
            writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
            # writing headers (field names)
            writer.writeheader()
            csvfile.close()

        logger.info('Starting XML Parsing...')
        try:
            # Get an iterable.
            context = ET.iterparse(self.xml_file, events=("start", "end"))
            elements_parsed = 0
            row_count = 0

            for index, (event, elem) in enumerate(context):

                con_data = []
                row_dict = {}
                if index == 0:
                    root = elem

                if event == "end" and "TermntdRcrd" in elem.tag:
                    index = elem.tag.index('TermntdRcrd')
                    attrib = elem.tag[:index]
                    tag = elem.tag[index:]
                    issr_elem = elem.find(attrib+"Issr")
                    row_dict["Issr"] = issr_elem.text
                    for it in elem.iter(attrib+'FinInstrmGnlAttrbts'):
                        fin_attr = it.find(attrib+"Id")
                        row_dict["FinInstrmGnlAttrbts.Id"] = fin_attr.text.encode('utf-8')
                        logger.info('Record Id: {}'.format(fin_attr.text.encode('utf-8')))
                        fin_attr = it.find(attrib+"FullNm")
                        row_dict["FinInstrmGnlAttrbts.FullNm"] = fin_attr.text.encode('utf-8')
                        fin_attr = it.find(attrib+"ClssfctnTp")
                        row_dict["FinInstrmGnlAttrbts.ClssfctnTp"] = fin_attr.text.encode('utf-8')
                        fin_attr = it.find(attrib+"CmmdtyDerivInd")
                        row_dict["FinInstrmGnlAttrbts.CmmdtyDerivInd"] = fin_attr.text.encode('utf-8')
                        fin_attr = it.find(attrib+"NtnlCcy")
                        row_dict["FinInstrmGnlAttrbts.NtnlCcy"] = fin_attr.text.encode('utf-8')
                        con_data.append(row_dict)
                        sample_dict = row_dict
                    root.clear()
                if len(row_dict) == 6:
                    logger.info("Writing row into CSV..")
                    wrote_dict = self.write_to_csv(con_data)
                    if wrote_dict is True:
                        row_count = row_count + 1
                    else:
                        raise ValueError('Data dict is empty!')
                    con_data = []
                elements_parsed = elements_parsed + 1
        except Exception as e:
            logger.error('Error occured during parsing XML due to : {}'.format(e))
            raise e
        return elements_parsed, row_count, sample_dict

    def write_to_csv(self, parsed_values):
        """
            Function to parsed xml values into csv.

            :param parsed_values: list of dictionaries to write to csv file
            :return: None
        """

        logger.info('Started writing into CSV file...')
        # writing to csv file
        try:
            if len(parsed_values) == 0:
                return False
            with open(self.csv_filename, 'a') as csvfile:

                # creating a csv dict writer object
                writer = csv.DictWriter(csvfile, fieldnames=FIELDS)

                # writing data rows
                writer.writerows(parsed_values)

                logger.info("Successfully inserted row into csv..")
                csvfile.close()
        except Exception as e:
            logger.error('Could not write to csv file : {}'.format(e))
            raise e
        return True

    def upload_file_to_s3(self, csv_file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        s3 = boto3.resource('s3')
        if object_name is None:
            object_name = csv_file_name

        try:
            s3.meta.client.upload_file(csv_file_name, AWS_STORAGE_BUCKET_NAME, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


if __name__ == '__main__':

    # Tracking start time
    start = timer()
    print("Start processing XML---->")
    logger.info('###########################################################################################')
    logger.info('           Creating CSV Export file name')
    logger.info('###########################################################################################')
    csv_filename = CSV_EXPORT_FILENAME+"_"+datetime.now().date().strftime("%Y_%m_%d")+".csv"
    logger.info('CSV Filename: "{}"'.format(csv_filename))
    logger.info('___________________________________________________________________________________________')

    logger.info('###########################################################################################')
    logger.info('           Parsing Large XML File')
    logger.info('###########################################################################################')
    parser = LargeXmlParser(csv_filename, XML_FILE)
    elements_parsed, row_count, row_dic_sample = parser.parse_xml()
    print('Elements Parsed: {}'.format(elements_parsed))
    print('Rows created in CSV : {}'.format(row_count))
    print('Sample Row Dict: {}'.format(row_dic_sample))
    logger.info('___________________________________________________________________________________________')

    end = timer()
    logger.info('################################## Process Summary #########################################################')
    logger.info('           Total Number of XML blocks parsed: {}'.format(elements_parsed))
    logger.info('           Total Number of Rows created in CSV : {}'.format(row_count))
    logger.info('           Sample parsed dict: {}'.format(row_dic_sample))
    logger.info('           Finished!! Time Taken: {}sec'.format(end - start))
    logger.info('############################################################################################################')
    print("Created CSV---->")
    print('Finished!! Time Taken: {}sec'.format(end - start))

    logger.info('###########################################################################################')
    logger.info('           Sending CSV to AWS s3')
    logger.info('###########################################################################################')
    logger.info('           This functionaly is currently commented off')
    # parser.upload_file_to_s3(str(csv_filename), AWS_STORAGE_BUCKET_NAME)
    logger.info('___________________________________________________________________________________________')
