import unittest
from large_xml_parser import LargeXmlParser
import os
import botocore
from botocore.exceptions import ClientError
from moto import mock_s3
import boto3

XML_FILE = './test_files/test_1.xml'
XML_FILE2 = './test_files/test_2.xml'
TEST_CSV_FILE_NAME = './test_files/test.csv'
WRONG_ELEMENT_XML_FILE = './test_files/wrong.xml'
AWS_STORAGE_BUCKET_NAME = "st-data-lake"

class ParserTest(unittest.TestCase):

    """
        Test Class for large_xml_parser.py
    """

    def setUp(self):

        """
            Setup objects and dummy data for test cases
        """
        # LargeXmlParser object with different parameters
        self.xml_parser = LargeXmlParser('./test_files/test_csv',XML_FILE)
        self.xml_parser2 = LargeXmlParser('./test_files/test_csv',XML_FILE2)
        self.wrong_xml_parser = LargeXmlParser('./test_files/test_csv',WRONG_ELEMENT_XML_FILE)

        # Dummy Dictionary Values
        self.test_dict = {'FinInstrmGnlAttrbts.Id': 'DE000A1R07V3', 'FinInstrmGnlAttrbts.ClssfctnTp': 'DBFTFB', 'FinInstrmGnlAttrbts.CmmdtyDerivInd': 'false', 'FinInstrmGnlAttrbts.FullNm': 'Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021)', 'FinInstrmGnlAttrbts.NtnlCcy': 'EUR', 'Issr': '549300GDPG70E3MBBU98'}
        self.wrong_dict = {'WrongField.Id': 'DE000A1R07V3', 'FinInstrmGnlAttrbts.ClssfctnTp': 'DBFTFB', 'FinInstrmGnlAttrbts.CmmdtyDerivInd': 'false', 'FinInstrmGnlAttrbts.FullNm': 'Kreditanst.f.Wiederaufbau     Anl.v.2014 (2021)', 'FinInstrmGnlAttrbts.NtnlCcy': 'EUR', 'Issr': '549300GDPG70E3MBBU98'}

    def tearDown(self):

        """
            Tear down test files created
        """

        pass

    def test_parse_xml_returned_values(self):

        """
            Test for return values from parse_xml()
        """

        elements_parsed, row_count, sample_dic = self.xml_parser.parse_xml()
        self.assertEqual(elements_parsed,100)
        self.assertEqual(row_count,1)
        self.assertIs(type(sample_dic),type(self.test_dict))
        self.assertEqual(len(sample_dic),len(self.test_dict))

    def test_parse_xml_returned_dict_fields(self):

        """
            Test for row dictionary keys
        """
        # Collecting returned values from parse_xml()
        elements_parsed, row_count, sample_dic = self.xml_parser.parse_xml()

        for key in sample_dic.keys():
            self.assertIn(key,['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 
            'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd',
            'FinInstrmGnlAttrbts.NtnlCcy', 'Issr'])

    def test_parse_xml_raise_excpetion(self):

        """
            Test for raising parse_xml() Exception
        """

        with self.assertRaises(Exception) as context:
            elements_parsed, row_count, sample_dic = self.wrong_xml_parser.parse_xml()

    def test_write_to_csv_true(self):

        """
            Test for return value from write_to_csv() true case
        """

        row_data = [self.test_dict]
        result_true = self.xml_parser.write_to_csv(row_data)
        self.assertEqual(result_true,True)

    def test_write_to_csv_false(self):

        """
            Test for return value from write_to_csv() false case
        """

        row_data = []
        result_false = self.xml_parser.write_to_csv(row_data)
        self.assertEqual(result_false,False)

    def test_write_to_csv_exception(self):

        """
            Test for write_to_csv() raise Exception
        """

        row_data = 'Wrong Data'
        with self.assertRaises(Exception) as context:
            result_false = self.xml_parser.write_to_csv(row_data)
    
    @mock_s3
    def test_upload_file_to_s3(self):

        """
            Test for return value from upload_file_to_s3() True case
        """

        conn = boto3.resource('s3', region_name='us-east-1')
        # We need to create the bucket since this is all in Moto's 'virtual'    AWS account.
        conn.create_bucket(Bucket=AWS_STORAGE_BUCKET_NAME)
        # Run your function. It should upload file to the above s3 bucket.
        result = self.xml_parser.upload_file_to_s3(TEST_CSV_FILE_NAME,AWS_STORAGE_BUCKET_NAME)
        message = "Test value is not true."
        self.assertTrue(result,message)

    @mock_s3
    def test_upload_file_to_s3_exception(self):

        """
            Test for return value from upload_file_to_s3() raise Exception
        """

        with self.assertRaises(Exception) as context:
            result = self.xml_parser.upload_file_to_s3(TEST_CSV_FILE_NAME,"test_bucket")


if __name__ == '__main__':
    unittest.main()