import unittest
from lake-formation-pemissions-sync.realtime.glue-lf-replicate-event.lambda_function import get_s3_table_target_bucket_name

class TestLambdaFunction(unittest.TestCase):

    def test_get_s3_table_target_bucket_name(self):
        # Test cases for different table_location inputs
        test_cases = [
            ("s3://my-bucket/path/to/data", "my-bucket"),
            ("s3://my-bucket-name/path/to/data/", "my-bucket-name"),
            ("s3://my.bucket.name.with.dots/path/to/data", "my.bucket.name.with.dots"),
        ]

        for table_location, expected_bucket_name in test_cases:
            result = get_s3_table_target_bucket_name(table_location)
            self.assertEqual(result, expected_bucket_name)

if __name__ == '__main__':
    unittest.main()
